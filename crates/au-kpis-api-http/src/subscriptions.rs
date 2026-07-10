//! `/v1/subscriptions` and webhook delivery helpers.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::{Duration, Instant, SystemTime},
};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use au_kpis_auth::VerifiedApiKey;
use au_kpis_domain::ids::{ArtifactId, DataflowId};
use au_kpis_source_register::{SourceStatus, load_source_register};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::{StreamExt, stream};
use hmac::{Hmac, Mac, digest::InvalidLength};
use rand::{Rng, RngCore, rngs::OsRng};
use reqwest::Client;
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{ApiError, AppState, auth::RequiredApiKey};

type HmacSha256 = Hmac<Sha256>;

const EVENT_DATA_UPDATED: &str = "data.updated";
const DEFAULT_DELIVERY_MAX_ATTEMPTS: i32 = 12;
const DEFAULT_DELIVERY_BATCH_SIZE: u32 = 32;
const DEFAULT_DELIVERY_BASE_BACKOFF: Duration = Duration::from_secs(30);
const DEFAULT_DELIVERY_MAX_BACKOFF: Duration = Duration::from_secs(60 * 60);
const DEFAULT_DELIVERY_POLL_INTERVAL: Duration = Duration::from_secs(5);
const WEBHOOK_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DELIVERY_WINDOW: ChronoDuration = ChronoDuration::hours(24);
const MAX_SUBSCRIPTIONS_PER_KEY: i64 = 5;
const MAX_DATAFLOWS_PER_SUBSCRIPTION: usize = 20;
const WORKER_DELIVERY_CONCURRENCY: usize = 32;
const DESTINATION_DELIVERY_CONCURRENCY: usize = 2;
const DEVELOPMENT_ENCRYPTION_KEY: [u8; 32] = [0x41; 32];

/// Request body for `POST /v1/subscriptions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateSubscriptionRequest {
    /// Absolute HTTP(S) URL to receive webhook deliveries.
    pub url: String,
    /// Optional dataflow filter. Empty means every dataflow update.
    #[serde(default)]
    pub dataflow_ids: Vec<DataflowId>,
}

/// Created webhook subscription details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SubscriptionDetails {
    /// Stable subscription id.
    pub id: Uuid,
    /// Delivery target URL.
    pub url: String,
    /// Dataflow filter attached to the subscription.
    pub dataflow_ids: Vec<DataflowId>,
    /// Current subscription status.
    pub status: String,
    /// UTC creation timestamp.
    pub created_at: DateTime<Utc>,
    /// UTC last-update timestamp.
    pub updated_at: DateTime<Utc>,
    /// UTC endpoint verification time.
    pub verified_at: Option<DateTime<Utc>>,
}

/// Response body for `POST /v1/subscriptions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateSubscriptionResponse {
    /// Created subscription.
    pub subscription: SubscriptionDetails,
    /// HMAC signing secret shown exactly once.
    pub signing_secret: String,
}

/// Collection response for subscriptions owned by one API key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ListSubscriptionsResponse {
    /// Subscriptions owned by the authenticated key.
    pub subscriptions: Vec<SubscriptionDetails>,
}

/// One-time secret returned by a rotation command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RotateSubscriptionSecretResponse {
    /// Updated subscription.
    pub subscription: SubscriptionDetails,
    /// New signing secret shown exactly once.
    pub signing_secret: String,
}

/// Data-update event used to fan out webhook delivery rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookDeliveryEvent {
    /// Stable event id used by receivers for deduplication.
    pub id: Uuid,
    /// Published ingestion generation.
    pub generation_id: Uuid,
    /// Dataflow that received new observations.
    pub dataflow_id: DataflowId,
    /// Optional source artifact that produced the update.
    pub artifact_id: Option<ArtifactId>,
    /// Number of observations loaded for the update.
    pub observations_loaded: u64,
    /// Timestamp associated with the update.
    pub occurred_at: DateTime<Utc>,
}

/// Runtime options for a webhook delivery sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryOptions {
    /// Maximum attempts before marking a delivery failed.
    pub max_attempts: i32,
    /// Delay after the first failed attempt.
    pub base_backoff: Duration,
    /// Maximum retry delay.
    pub max_backoff: Duration,
    /// Maximum due deliveries claimed in one sweep.
    pub batch_size: u32,
}

impl Default for DeliveryOptions {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_DELIVERY_MAX_ATTEMPTS,
            base_backoff: DEFAULT_DELIVERY_BASE_BACKOFF,
            max_backoff: DEFAULT_DELIVERY_MAX_BACKOFF,
            batch_size: DEFAULT_DELIVERY_BATCH_SIZE,
        }
    }
}

/// Aggregate result of one webhook delivery sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeliveryRunOutcome {
    /// Deliveries attempted.
    pub attempted: u64,
    /// Deliveries successfully accepted by subscribers.
    pub delivered: u64,
    /// Deliveries permanently failed after exhausting attempts.
    pub failed: u64,
}

/// Errors returned by webhook subscription and delivery operations.
#[derive(Debug, Error)]
pub enum SubscriptionError {
    /// Invalid caller input or persisted state.
    #[error("subscription validation: {0}")]
    Validation(String),
    /// Database access failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// HTTP delivery failed before a response was received.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// JSON serialization failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// HMAC signing failed.
    #[error(transparent)]
    Hmac(#[from] InvalidLength),
    /// Secret encryption/decryption failed.
    #[error("subscription encryption failed")]
    Crypto,
    /// Owned subscription was not found.
    #[error("subscription not found")]
    NotFound,
    /// A reclaimed delivery lease rejected a stale worker result.
    #[error("webhook delivery lease was lost")]
    LeaseLost,
}

/// `POST /v1/subscriptions`.
#[utoipa::path(
    post,
    operation_id = "createSubscription",
    path = "/v1/subscriptions",
    request_body(content = CreateSubscriptionRequest, content_type = "application/json"),
    responses(
        (status = 202, description = "Subscription pending endpoint verification.", body = CreateSubscriptionResponse, content_type = "application/json"),
        (status = 400, description = "Invalid subscription request.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 401, description = "Missing or invalid API key.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 403, description = "API key lacks subscriptions:write.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 503, description = "Redis or another required dependency is unavailable.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 500, description = "Internal error.", body = crate::ProblemDetails, content_type = "application/problem+json")
    ),
    security(("ApiKeyAuth" = [])),
    tag = "subscriptions"
)]
pub async fn create_subscription(
    State(state): State<AppState>,
    RequiredApiKey { key: api_key }: RequiredApiKey,
    Json(request): Json<CreateSubscriptionRequest>,
) -> Result<Response, ApiError> {
    require_scope(&api_key, "subscriptions:write")?;
    let response = create_subscription_record(&state.db, api_key.id, request)
        .await
        .map_err(subscription_error_to_api_error)?;
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

/// `GET /v1/subscriptions`.
#[utoipa::path(
    get,
    operation_id = "listSubscriptions",
    path = "/v1/subscriptions",
    responses(
        (status = 200, body = ListSubscriptionsResponse),
        (status = 401, body = crate::ProblemDetails),
        (status = 403, body = crate::ProblemDetails),
        (status = 503, body = crate::ProblemDetails)
    ),
    security(("ApiKeyAuth" = [])),
    tag = "subscriptions"
)]
pub async fn list_subscriptions(
    State(state): State<AppState>,
    RequiredApiKey { key }: RequiredApiKey,
) -> Result<Json<ListSubscriptionsResponse>, ApiError> {
    require_scope(&key, "subscriptions:read")?;
    let subscriptions = load_owned_subscriptions(&state.db, key.id)
        .await
        .map_err(subscription_error_to_api_error)?;
    Ok(Json(ListSubscriptionsResponse { subscriptions }))
}

/// `GET /v1/subscriptions/{id}`.
#[utoipa::path(
    get,
    operation_id = "getSubscription",
    path = "/v1/subscriptions/{id}",
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, body = SubscriptionDetails),
        (status = 401, body = crate::ProblemDetails),
        (status = 403, body = crate::ProblemDetails),
        (status = 404, body = crate::ProblemDetails),
        (status = 503, body = crate::ProblemDetails)
    ),
    security(("ApiKeyAuth" = [])),
    tag = "subscriptions"
)]
pub async fn get_subscription(
    State(state): State<AppState>,
    RequiredApiKey { key }: RequiredApiKey,
    Path(id): Path<Uuid>,
) -> Result<Json<SubscriptionDetails>, ApiError> {
    require_scope(&key, "subscriptions:read")?;
    load_owned_subscription(&state.db, key.id, id)
        .await
        .map(Json)
        .map_err(subscription_error_to_api_error)
}

/// `POST /v1/subscriptions/{id}/verify`.
#[utoipa::path(
    post,
    operation_id = "verifySubscription",
    path = "/v1/subscriptions/{id}/verify",
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, body = SubscriptionDetails),
        (status = 400, body = crate::ProblemDetails),
        (status = 401, body = crate::ProblemDetails),
        (status = 403, body = crate::ProblemDetails),
        (status = 404, body = crate::ProblemDetails),
        (status = 503, body = crate::ProblemDetails)
    ),
    security(("ApiKeyAuth" = [])),
    tag = "subscriptions"
)]
pub async fn verify_subscription(
    State(state): State<AppState>,
    RequiredApiKey { key }: RequiredApiKey,
    Path(id): Path<Uuid>,
) -> Result<Json<SubscriptionDetails>, ApiError> {
    require_scope(&key, "subscriptions:write")?;
    verify_owned_subscription(&state.db, key.id, id)
        .await
        .map(Json)
        .map_err(subscription_error_to_api_error)
}

/// `POST /v1/subscriptions/{id}/rotate-secret`.
#[utoipa::path(
    post,
    operation_id = "rotateSubscriptionSecret",
    path = "/v1/subscriptions/{id}/rotate-secret",
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, body = RotateSubscriptionSecretResponse),
        (status = 401, body = crate::ProblemDetails),
        (status = 403, body = crate::ProblemDetails),
        (status = 404, body = crate::ProblemDetails),
        (status = 503, body = crate::ProblemDetails)
    ),
    security(("ApiKeyAuth" = [])),
    tag = "subscriptions"
)]
pub async fn rotate_subscription_secret(
    State(state): State<AppState>,
    RequiredApiKey { key }: RequiredApiKey,
    Path(id): Path<Uuid>,
) -> Result<Json<RotateSubscriptionSecretResponse>, ApiError> {
    require_scope(&key, "subscriptions:write")?;
    rotate_owned_subscription_secret(&state.db, key.id, id)
        .await
        .map(Json)
        .map_err(subscription_error_to_api_error)
}

/// `DELETE /v1/subscriptions/{id}`.
#[utoipa::path(
    delete,
    operation_id = "revokeSubscription",
    path = "/v1/subscriptions/{id}",
    params(("id" = Uuid, Path)),
    responses(
        (status = 204, description = "Subscription revoked without deleting audit history."),
        (status = 401, body = crate::ProblemDetails),
        (status = 403, body = crate::ProblemDetails),
        (status = 404, body = crate::ProblemDetails),
        (status = 503, body = crate::ProblemDetails)
    ),
    security(("ApiKeyAuth" = [])),
    tag = "subscriptions"
)]
pub async fn revoke_subscription(
    State(state): State<AppState>,
    RequiredApiKey { key }: RequiredApiKey,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_scope(&key, "subscriptions:write")?;
    revoke_owned_subscription(&state.db, key.id, id)
        .await
        .map_err(subscription_error_to_api_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn require_scope(key: &VerifiedApiKey, scope: &'static str) -> Result<(), ApiError> {
    if key.scopes.iter().any(|value| value == scope) {
        Ok(())
    } else {
        Err(ApiError::Forbidden(format!(
            "API key requires `{scope}` scope"
        )))
    }
}

/// Insert webhook delivery rows for every active matching subscription.
#[tracing::instrument(skip(pool, event), fields(dataflow_id = %event.dataflow_id))]
pub async fn enqueue_data_update_event(
    pool: &PgPool,
    event: &WebhookDeliveryEvent,
) -> Result<u64, SubscriptionError> {
    let payload = delivery_payload(event);
    let artifact_bytes = event
        .artifact_id
        .map(|artifact_id| artifact_id.digest().as_bytes().to_vec());
    let rows = sqlx::query(
        "INSERT INTO webhook_deliveries (
             subscription_id, event_id, generation_id, event_type, dataflow_id,
             artifact_id, payload, status, attempts, max_attempts,
             next_attempt_at, expires_at
         )
         SELECT id, $2, $3, $4, $1, $5, $6, 'pending', 0, $7, $8, $9
         FROM webhook_subscriptions
         WHERE status = 'active'
           AND (cardinality(dataflow_ids) = 0 OR $1 = ANY(dataflow_ids))",
    )
    .bind(event.dataflow_id.as_str())
    .bind(event.id)
    .bind(event.generation_id)
    .bind(EVENT_DATA_UPDATED)
    .bind(artifact_bytes)
    .bind(payload)
    .bind(DEFAULT_DELIVERY_MAX_ATTEMPTS)
    .bind(event.occurred_at)
    .bind(event.occurred_at + DELIVERY_WINDOW)
    .execute(pool)
    .await?;

    tracing::info!(
        dataflow_id = %event.dataflow_id,
        deliveries = rows.rows_affected(),
        "webhook deliveries enqueued"
    );

    Ok(rows.rows_affected())
}

/// Deliver due webhook rows and persist success/failure attempts.
#[tracing::instrument(skip(pool, client))]
pub async fn deliver_due_webhooks(
    pool: &PgPool,
    client: &Client,
    now: DateTime<Utc>,
    options: DeliveryOptions,
) -> Result<DeliveryRunOutcome, SubscriptionError> {
    validate_delivery_options(options)?;
    let worker_id = Uuid::new_v4();
    let deliveries = claim_due_deliveries(pool, worker_id, now, options.batch_size).await?;
    let mut destination_limits = BTreeMap::new();
    for delivery in &deliveries {
        destination_limits
            .entry(delivery.target_url.clone())
            .or_insert_with(|| {
                std::sync::Arc::new(tokio::sync::Semaphore::new(
                    DESTINATION_DELIVERY_CONCURRENCY,
                ))
            });
    }

    let tasks = deliveries.into_iter().map(|delivery| {
        let destination_limit = destination_limits
            .get(&delivery.target_url)
            .expect("delivery destination semaphore was initialized")
            .clone();
        async move {
            let _destination_permit =
                acquire_destination_with_lease(pool, &delivery, destination_limit).await?;
            deliver_claimed_webhook(pool, client, delivery, now, options).await
        }
    });
    let mut outcome = DeliveryRunOutcome::default();
    let results = stream::iter(tasks)
        .buffer_unordered(WORKER_DELIVERY_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for result in results {
        let partial = result?;
        outcome.attempted += partial.attempted;
        outcome.delivered += partial.delivered;
        outcome.failed += partial.failed;
    }
    Ok(outcome)
}

async fn acquire_destination_with_lease(
    pool: &PgPool,
    delivery: &ClaimedDelivery,
    destination_limit: std::sync::Arc<tokio::sync::Semaphore>,
) -> Result<tokio::sync::OwnedSemaphorePermit, SubscriptionError> {
    let acquire = destination_limit.acquire_owned();
    tokio::pin!(acquire);
    loop {
        tokio::select! {
            permit = &mut acquire => {
                let permit = permit.map_err(|_| {
                    SubscriptionError::Validation("delivery semaphore closed".into())
                })?;
                renew_delivery_lease(pool, delivery, Utc::now()).await?;
                return Ok(permit);
            }
            () = tokio::time::sleep(Duration::from_secs(30)) => {
                renew_delivery_lease(pool, delivery, Utc::now()).await?;
            }
        }
    }
}

async fn renew_delivery_lease(
    pool: &PgPool,
    delivery: &ClaimedDelivery,
    now: DateTime<Utc>,
) -> Result<(), SubscriptionError> {
    let updated = sqlx::query(
        "UPDATE webhook_deliveries
         SET leased_until = $4 + INTERVAL '60 seconds', updated_at = $4
         WHERE id = $1 AND status = 'delivering'
           AND lease_owner = $2 AND lease_version = $3",
    )
    .bind(delivery.id)
    .bind(delivery.worker_id)
    .bind(delivery.lease_version)
    .bind(now)
    .execute(pool)
    .await?;
    if updated.rows_affected() == 0 {
        return Err(SubscriptionError::LeaseLost);
    }
    Ok(())
}

async fn deliver_claimed_webhook(
    pool: &PgPool,
    client: &Client,
    delivery: ClaimedDelivery,
    now: DateTime<Utc>,
    options: DeliveryOptions,
) -> Result<DeliveryRunOutcome, SubscriptionError> {
    let mut outcome = DeliveryRunOutcome {
        attempted: 1,
        ..DeliveryRunOutcome::default()
    };
    let attempt = delivery.attempts + 1;
    let started = Instant::now();
    let result = send_delivery(client, &delivery, now).await;
    let latency_ms = started.elapsed().as_millis().min(i64::MAX as u128) as i64;

    match result {
        Ok(response) if response.status.is_success() => {
            record_delivery_success(
                pool,
                SuccessRecord {
                    delivery_id: delivery.id,
                    worker_id: delivery.worker_id,
                    lease_version: delivery.lease_version,
                    subscription_id: delivery.subscription_id,
                    attempt,
                    status_code: response.status.as_u16(),
                    now,
                    latency_ms,
                },
            )
            .await?;
            outcome.delivered = 1;
            tracing::info!(
                delivery_id = delivery.id,
                subscription_id = %delivery.subscription_id,
                attempt,
                status_code = response.status.as_u16(),
                "webhook delivery accepted"
            );
        }
        Ok(response) => {
            if record_delivery_failure(
                pool,
                FailureRecord {
                    delivery_id: delivery.id,
                    subscription_id: delivery.subscription_id,
                    attempt,
                    max_attempts: if retryable_status(response.status) {
                        effective_max_attempts(&delivery, options)
                    } else {
                        attempt
                    },
                    now,
                    status_code: Some(response.status.as_u16()),
                    error_message: format!("subscriber returned HTTP {}", response.status.as_u16()),
                    latency_ms,
                    options,
                    worker_id: delivery.worker_id,
                    lease_version: delivery.lease_version,
                    expires_at: delivery.expires_at,
                    retry_after: response.retry_after,
                },
            )
            .await?
            {
                outcome.failed = 1;
            }
        }
        Err(err) => {
            if record_delivery_failure(
                pool,
                FailureRecord {
                    delivery_id: delivery.id,
                    subscription_id: delivery.subscription_id,
                    attempt,
                    max_attempts: effective_max_attempts(&delivery, options),
                    now,
                    status_code: None,
                    error_message: err.to_string(),
                    latency_ms,
                    options,
                    worker_id: delivery.worker_id,
                    lease_version: delivery.lease_version,
                    expires_at: delivery.expires_at,
                    retry_after: None,
                },
            )
            .await?
            {
                outcome.failed = 1;
            }
        }
    }
    Ok(outcome)
}

/// Spawn the long-running webhook delivery worker used by the API process.
#[must_use]
pub fn spawn_webhook_delivery_worker(
    pool: PgPool,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = run_webhook_delivery_worker(
            pool,
            shutdown,
            DeliveryOptions::default(),
            DEFAULT_DELIVERY_POLL_INTERVAL,
        )
        .await
        {
            tracing::error!(error = %err, "webhook delivery worker exited");
        }
    })
}

/// Run a polling worker that delivers due webhook rows until shutdown.
#[tracing::instrument(skip(pool, shutdown))]
pub async fn run_webhook_delivery_worker(
    pool: PgPool,
    shutdown: CancellationToken,
    options: DeliveryOptions,
    poll_interval: Duration,
) -> Result<(), SubscriptionError> {
    validate_delivery_options(options)?;
    if poll_interval.is_zero() {
        return Err(SubscriptionError::Validation(
            "poll_interval must be positive".into(),
        ));
    }

    let client = Client::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!("webhook delivery worker stopped");
                return Ok(());
            }
            () = tokio::time::sleep(poll_interval) => {
                match deliver_due_webhooks(&pool, &client, Utc::now(), options).await {
                    Ok(outcome) if outcome.attempted > 0 => {
                        tracing::info!(
                            attempted = outcome.attempted,
                            delivered = outcome.delivered,
                            failed = outcome.failed,
                            "webhook delivery sweep completed"
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, "webhook delivery sweep failed");
                    }
                }
            }
        }
    }
}

async fn create_subscription_record(
    pool: &PgPool,
    api_key_id: Uuid,
    request: CreateSubscriptionRequest,
) -> Result<CreateSubscriptionResponse, SubscriptionError> {
    validate_subscription_request(pool, &request).await?;

    let id = Uuid::new_v4();
    let (signing_secret, secret_bytes) = generate_signing_secret();
    let encrypted = WebhookKeyring::from_env()?.encrypt(id, &secret_bytes)?;
    let dataflow_ids = request
        .dataflow_ids
        .iter()
        .map(DataflowId::as_str)
        .collect::<Vec<_>>();

    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("subscriptions:{api_key_id}"))
        .execute(&mut *tx)
        .await?;
    let current: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM webhook_subscriptions
         WHERE api_key_id = $1 AND status IN ('pending_verification', 'active')",
    )
    .bind(api_key_id)
    .fetch_one(&mut *tx)
    .await?;
    if current >= MAX_SUBSCRIPTIONS_PER_KEY {
        return Err(SubscriptionError::Validation(
            "API key already has five active or pending subscriptions".into(),
        ));
    }
    let row = sqlx::query(
        "INSERT INTO webhook_subscriptions (
             id, api_key_id, target_url, dataflow_ids, status,
             secret_ciphertext, secret_nonce, secret_key_version
         )
         VALUES ($1, $2, $3, $4, 'pending_verification', $5, $6, $7)
         RETURNING created_at, updated_at, verified_at",
    )
    .bind(id)
    .bind(api_key_id)
    .bind(&request.url)
    .bind(dataflow_ids)
    .bind(encrypted.ciphertext)
    .bind(encrypted.nonce.as_slice())
    .bind(encrypted.key_version)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;

    tracing::info!(
        subscription_id = %id,
        api_key_id = %api_key_id,
        dataflow_count = request.dataflow_ids.len(),
        "webhook subscription created"
    );

    Ok(CreateSubscriptionResponse {
        subscription: SubscriptionDetails {
            id,
            url: request.url,
            dataflow_ids: request.dataflow_ids,
            status: "pending_verification".into(),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            verified_at: row.get("verified_at"),
        },
        signing_secret,
    })
}

async fn validate_subscription_request(
    pool: &PgPool,
    request: &CreateSubscriptionRequest,
) -> Result<(), SubscriptionError> {
    resolve_destination(&request.url).await?;

    let unique = request
        .dataflow_ids
        .iter()
        .map(DataflowId::as_str)
        .collect::<BTreeSet<_>>();
    if unique.len() != request.dataflow_ids.len() {
        return Err(SubscriptionError::Validation(
            "dataflow selectors must not contain duplicates".into(),
        ));
    }
    if request.dataflow_ids.len() > MAX_DATAFLOWS_PER_SUBSCRIPTION {
        return Err(SubscriptionError::Validation(
            "a subscription may select at most 20 active dataflows".into(),
        ));
    }
    let register =
        load_source_register().map_err(|error| SubscriptionError::Validation(error.to_string()))?;
    let active = register
        .dataflows
        .iter()
        .filter(|entry| entry.status == SourceStatus::Active)
        .map(|entry| entry.dataflow_id.as_str())
        .collect::<BTreeSet<_>>();
    if request.dataflow_ids.is_empty() && active.len() > MAX_DATAFLOWS_PER_SUBSCRIPTION {
        return Err(SubscriptionError::Validation(
            "the active dataflow set exceeds the subscription allowance".into(),
        ));
    }

    for dataflow_id in &request.dataflow_ids {
        if !active.contains(dataflow_id.as_str()) {
            return Err(SubscriptionError::Validation(format!(
                "dataflow `{dataflow_id}` is not active"
            )));
        }
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM dataflows WHERE id = $1)")
                .bind(dataflow_id.as_str())
                .fetch_one(pool)
                .await?;
        if !exists {
            return Err(SubscriptionError::Validation(format!(
                "unknown dataflow `{dataflow_id}`"
            )));
        }
    }

    Ok(())
}

fn generate_signing_secret() -> (String, [u8; 32]) {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    (URL_SAFE_NO_PAD.encode(bytes), bytes)
}

#[derive(Debug)]
struct EncryptedSecret {
    ciphertext: Vec<u8>,
    nonce: [u8; 12],
    key_version: i32,
}

#[derive(Debug)]
struct WebhookKeyring {
    active_version: i32,
    active_key: [u8; 32],
    previous: Option<(i32, [u8; 32])>,
}

impl WebhookKeyring {
    fn from_env() -> Result<Self, SubscriptionError> {
        let active_version = std::env::var("AU_KPIS_WEBHOOK_ENCRYPTION_KEY_VERSION")
            .ok()
            .map(|value| value.parse::<i32>())
            .transpose()
            .map_err(|_| SubscriptionError::Crypto)?
            .unwrap_or(1);
        let active_key = match std::env::var("AU_KPIS_WEBHOOK_ENCRYPTION_KEY") {
            Ok(value) => decode_encryption_key(&value)?,
            Err(_) if cfg!(debug_assertions) => DEVELOPMENT_ENCRYPTION_KEY,
            Err(_) => return Err(SubscriptionError::Crypto),
        };
        let previous = match (
            std::env::var("AU_KPIS_WEBHOOK_PREVIOUS_ENCRYPTION_KEY_VERSION").ok(),
            std::env::var("AU_KPIS_WEBHOOK_PREVIOUS_ENCRYPTION_KEY").ok(),
        ) {
            (Some(version), Some(key)) => Some((
                version.parse().map_err(|_| SubscriptionError::Crypto)?,
                decode_encryption_key(&key)?,
            )),
            _ => None,
        };
        Ok(Self {
            active_version,
            active_key,
            previous,
        })
    }

    fn encrypt(
        &self,
        subscription_id: Uuid,
        secret: &[u8],
    ) -> Result<EncryptedSecret, SubscriptionError> {
        let mut nonce = [0_u8; 12];
        OsRng.fill_bytes(&mut nonce);
        let cipher =
            Aes256Gcm::new_from_slice(&self.active_key).map_err(|_| SubscriptionError::Crypto)?;
        let aad = secret_aad(subscription_id, self.active_version);
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: secret,
                    aad: &aad,
                },
            )
            .map_err(|_| SubscriptionError::Crypto)?;
        Ok(EncryptedSecret {
            ciphertext,
            nonce,
            key_version: self.active_version,
        })
    }

    fn decrypt(
        &self,
        subscription_id: Uuid,
        ciphertext: &[u8],
        nonce: &[u8],
        key_version: i32,
    ) -> Result<Vec<u8>, SubscriptionError> {
        let key = if key_version == self.active_version {
            &self.active_key
        } else if let Some((_, key)) = self
            .previous
            .as_ref()
            .filter(|(version, _)| *version == key_version)
        {
            key
        } else {
            return Err(SubscriptionError::Crypto);
        };
        let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| SubscriptionError::Crypto)?;
        let aad = secret_aad(subscription_id, key_version);
        cipher
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| SubscriptionError::Crypto)
    }
}

fn decode_encryption_key(value: &str) -> Result<[u8; 32], SubscriptionError> {
    URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| SubscriptionError::Crypto)?
        .try_into()
        .map_err(|_| SubscriptionError::Crypto)
}

fn secret_aad(subscription_id: Uuid, key_version: i32) -> Vec<u8> {
    let mut aad = Vec::with_capacity(20);
    aad.extend_from_slice(subscription_id.as_bytes());
    aad.extend_from_slice(&key_version.to_be_bytes());
    aad
}

async fn load_owned_subscriptions(
    pool: &PgPool,
    api_key_id: Uuid,
) -> Result<Vec<SubscriptionDetails>, SubscriptionError> {
    sqlx::query(
        "SELECT id, target_url, dataflow_ids, status, created_at, updated_at, verified_at
         FROM webhook_subscriptions WHERE api_key_id = $1
         ORDER BY created_at DESC, id",
    )
    .bind(api_key_id)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(subscription_from_row)
    .collect()
}

async fn load_owned_subscription(
    pool: &PgPool,
    api_key_id: Uuid,
    id: Uuid,
) -> Result<SubscriptionDetails, SubscriptionError> {
    sqlx::query(
        "SELECT id, target_url, dataflow_ids, status, created_at, updated_at, verified_at
         FROM webhook_subscriptions WHERE api_key_id = $1 AND id = $2",
    )
    .bind(api_key_id)
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or(SubscriptionError::NotFound)
    .and_then(subscription_from_row)
}

fn subscription_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<SubscriptionDetails, SubscriptionError> {
    let ids: Vec<String> = row.try_get("dataflow_ids")?;
    Ok(SubscriptionDetails {
        id: row.try_get("id")?,
        url: row.try_get("target_url")?,
        dataflow_ids: ids
            .into_iter()
            .map(|value| {
                DataflowId::new(value)
                    .map_err(|error| SubscriptionError::Validation(error.to_string()))
            })
            .collect::<Result<_, _>>()?,
        status: row.try_get("status")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        verified_at: row.try_get("verified_at")?,
    })
}

async fn verify_owned_subscription(
    pool: &PgPool,
    api_key_id: Uuid,
    id: Uuid,
) -> Result<SubscriptionDetails, SubscriptionError> {
    let row = sqlx::query(
        "SELECT target_url, secret_ciphertext, secret_nonce, secret_key_version, status
         FROM webhook_subscriptions WHERE api_key_id = $1 AND id = $2 FOR UPDATE",
    )
    .bind(api_key_id)
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or(SubscriptionError::NotFound)?;
    let status: String = row.try_get("status")?;
    if status == "revoked" {
        return Err(SubscriptionError::Validation(
            "subscription is revoked".into(),
        ));
    }
    let target_url: String = row.try_get("target_url")?;
    let secret = WebhookKeyring::from_env()?.decrypt(
        id,
        &row.try_get::<Vec<u8>, _>("secret_ciphertext")?,
        &row.try_get::<Vec<u8>, _>("secret_nonce")?,
        row.try_get("secret_key_version")?,
    )?;
    let destination = resolve_destination(&target_url).await?;
    let mut challenge_bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut challenge_bytes);
    let challenge = URL_SAFE_NO_PAD.encode(challenge_bytes);
    let digest = Sha256::digest(challenge.as_bytes()).to_vec();
    let expires_at = Utc::now() + ChronoDuration::minutes(10);
    sqlx::query(
        "UPDATE webhook_subscriptions
         SET verification_digest = $3, verification_expires_at = $4, updated_at = now()
         WHERE api_key_id = $1 AND id = $2",
    )
    .bind(api_key_id)
    .bind(id)
    .bind(digest)
    .bind(expires_at)
    .execute(pool)
    .await?;

    let body = serde_json::to_vec(&json!({ "challenge": challenge }))?;
    let challenge_id = Uuid::new_v4();
    let timestamp = Utc::now().timestamp();
    let signature = sign_payload(&secret, challenge_id, timestamp, &body)?;
    let response = pinned_client(&destination)?
        .post(destination.url)
        .header("content-type", "application/json")
        .header("x-au-kpis-webhook-id", challenge_id.to_string())
        .header("x-au-kpis-webhook-timestamp", timestamp.to_string())
        .header("x-au-kpis-webhook-signature", signature)
        .body(body)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(SubscriptionError::Validation(format!(
            "verification endpoint returned HTTP {}",
            response.status().as_u16()
        )));
    }
    let echoed: Value = response.json().await?;
    if echoed.get("challenge").and_then(Value::as_str) != Some(challenge.as_str()) {
        return Err(SubscriptionError::Validation(
            "verification endpoint did not echo the signed challenge".into(),
        ));
    }
    sqlx::query(
        "UPDATE webhook_subscriptions
         SET status = 'active', verified_at = now(), verification_digest = NULL,
             verification_expires_at = NULL, updated_at = now()
         WHERE api_key_id = $1 AND id = $2",
    )
    .bind(api_key_id)
    .bind(id)
    .execute(pool)
    .await?;
    load_owned_subscription(pool, api_key_id, id).await
}

async fn rotate_owned_subscription_secret(
    pool: &PgPool,
    api_key_id: Uuid,
    id: Uuid,
) -> Result<RotateSubscriptionSecretResponse, SubscriptionError> {
    let (secret, secret_bytes) = generate_signing_secret();
    let encrypted = WebhookKeyring::from_env()?.encrypt(id, &secret_bytes)?;
    let result = sqlx::query(
        "UPDATE webhook_subscriptions
         SET previous_secret_ciphertext = secret_ciphertext,
             previous_secret_nonce = secret_nonce,
             previous_secret_key_version = secret_key_version,
             previous_secret_expires_at = now() + INTERVAL '24 hours',
             secret_ciphertext = $3, secret_nonce = $4, secret_key_version = $5,
             updated_at = now()
         WHERE api_key_id = $1 AND id = $2 AND status <> 'revoked'",
    )
    .bind(api_key_id)
    .bind(id)
    .bind(encrypted.ciphertext)
    .bind(encrypted.nonce.as_slice())
    .bind(encrypted.key_version)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(SubscriptionError::NotFound);
    }
    Ok(RotateSubscriptionSecretResponse {
        subscription: load_owned_subscription(pool, api_key_id, id).await?,
        signing_secret: secret,
    })
}

async fn revoke_owned_subscription(
    pool: &PgPool,
    api_key_id: Uuid,
    id: Uuid,
) -> Result<(), SubscriptionError> {
    let result = sqlx::query(
        "UPDATE webhook_subscriptions
         SET status = 'revoked', revoked_at = COALESCE(revoked_at, now()), updated_at = now(),
             secret_ciphertext = NULL, secret_nonce = NULL, secret_key_version = NULL,
             previous_secret_ciphertext = NULL, previous_secret_nonce = NULL,
             previous_secret_key_version = NULL, previous_secret_expires_at = NULL
         WHERE api_key_id = $1 AND id = $2",
    )
    .bind(api_key_id)
    .bind(id)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(SubscriptionError::NotFound);
    }
    Ok(())
}

#[derive(Debug)]
struct ResolvedDestination {
    url: url::Url,
    host: String,
    addresses: Vec<SocketAddr>,
}

async fn resolve_destination(target: &str) -> Result<ResolvedDestination, SubscriptionError> {
    let (url, host) = validate_destination_url(target)?;
    let addresses = tokio::net::lookup_host((host.as_str(), 443))
        .await
        .map_err(|error| SubscriptionError::Validation(format!("webhook DNS failed: {error}")))?
        .collect::<Vec<_>>();
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|address| !globally_routable(address.ip()))
    {
        return Err(SubscriptionError::Validation(
            "every webhook DNS result must be globally routable".into(),
        ));
    }
    Ok(ResolvedDestination {
        url,
        host,
        addresses,
    })
}

fn validate_destination_url(target: &str) -> Result<(url::Url, String), SubscriptionError> {
    let url = url::Url::parse(target)
        .map_err(|error| SubscriptionError::Validation(format!("invalid url: {error}")))?;
    if url.scheme() != "https"
        || url.port_or_known_default() != Some(443)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(SubscriptionError::Validation(
            "webhook URL must use HTTPS port 443 without userinfo or a fragment".into(),
        ));
    }
    let host = match url.host() {
        Some(url::Host::Domain(host)) => host.to_string(),
        _ => {
            return Err(SubscriptionError::Validation(
                "webhook URL must use a DNS hostname, not an IP literal".into(),
            ));
        }
    };
    Ok((url, host))
}

fn pinned_client(destination: &ResolvedDestination) -> Result<Client, SubscriptionError> {
    Client::builder()
        .redirect(Policy::none())
        .resolve_to_addrs(&destination.host, &destination.addresses)
        .timeout(WEBHOOK_REQUEST_TIMEOUT)
        .build()
        .map_err(Into::into)
}

fn globally_routable(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => globally_routable_v4(ip),
        IpAddr::V6(ip) => globally_routable_v6(ip),
    }
}

fn globally_routable_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || ip.is_unspecified()
        || a == 0
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 198 && (b == 18 || b == 19))
        || a >= 240)
}

fn globally_routable_v6(ip: Ipv6Addr) -> bool {
    let octets = ip.octets();
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return globally_routable_v4(mapped);
    }
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (octets[0] & 0xfe) == 0xfc
        || (octets[0] == 0xfe && (octets[1] & 0xc0) == 0x80)
        || (octets[0..4] == [0x20, 0x01, 0x0d, 0xb8]))
}

fn delivery_payload(event: &WebhookDeliveryEvent) -> Value {
    json!({
        "id": event.id,
        "schema_version": "1",
        "type": EVENT_DATA_UPDATED,
        "generation_id": event.generation_id,
        "dataflow_id": event.dataflow_id,
        "artifact_id": event.artifact_id,
        "observations_loaded": event.observations_loaded,
        "occurred_at": event.occurred_at,
    })
}

#[derive(Debug, Clone)]
struct ClaimedDelivery {
    id: i64,
    event_id: Uuid,
    subscription_id: Uuid,
    target_url: String,
    secret_ciphertext: Vec<u8>,
    secret_nonce: Vec<u8>,
    secret_key_version: i32,
    payload: Value,
    attempts: i32,
    max_attempts: i32,
    worker_id: Uuid,
    lease_version: i64,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
struct DeliveryResponse {
    status: StatusCode,
    retry_after: Option<Duration>,
}

async fn claim_due_deliveries(
    pool: &PgPool,
    worker_id: Uuid,
    now: DateTime<Utc>,
    batch_size: u32,
) -> Result<Vec<ClaimedDelivery>, SubscriptionError> {
    sqlx::query(
        "WITH expired AS (
             UPDATE webhook_deliveries
             SET status = 'dead_letter', next_attempt_at = NULL,
                 lease_owner = NULL, leased_until = NULL,
                 last_error = 'delivery window expired', updated_at = $1
             WHERE status IN ('pending', 'delivering') AND expires_at <= $1
             RETURNING subscription_id
         ), expired_counts AS (
             SELECT subscription_id, count(*)::INTEGER AS failures
             FROM expired GROUP BY subscription_id
         )
         UPDATE webhook_subscriptions AS subscription
         SET consecutive_failures = subscription.consecutive_failures + expired_counts.failures,
             status = CASE
                 WHEN subscription.consecutive_failures + expired_counts.failures >= 5
                 THEN 'paused' ELSE subscription.status END,
             paused_at = CASE
                 WHEN subscription.consecutive_failures + expired_counts.failures >= 5
                 THEN $1 ELSE subscription.paused_at END,
             updated_at = $1
         FROM expired_counts
         WHERE subscription.id = expired_counts.subscription_id
           AND subscription.status = 'active'",
    )
    .bind(now)
    .execute(pool)
    .await?;

    let rows = sqlx::query(
        "WITH due AS (
             SELECT id
             FROM webhook_deliveries
             WHERE ((status = 'pending' AND next_attempt_at <= $1)
                    OR (status = 'delivering' AND leased_until <= $1))
               AND expires_at > $1
             ORDER BY next_attempt_at ASC, id ASC
             LIMIT $2
             FOR UPDATE SKIP LOCKED
         )
         UPDATE webhook_deliveries d
         SET status = 'delivering',
             lease_owner = $3,
             lease_version = d.lease_version + 1,
             leased_until = $1 + INTERVAL '60 seconds',
             first_attempt_at = COALESCE(d.first_attempt_at, $1),
             updated_at = $1
         FROM due, webhook_subscriptions s
         WHERE d.id = due.id
           AND s.id = d.subscription_id
           AND s.status = 'active'
         RETURNING d.id, d.event_id, d.subscription_id, s.target_url,
                   s.secret_ciphertext, s.secret_nonce, s.secret_key_version,
                   d.payload, d.attempts, d.max_attempts, d.lease_version,
                   d.expires_at",
    )
    .bind(now)
    .bind(i64::from(batch_size))
    .bind(worker_id)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(ClaimedDelivery {
                id: row.try_get("id")?,
                event_id: row.try_get("event_id")?,
                subscription_id: row.try_get("subscription_id")?,
                target_url: row.try_get("target_url")?,
                secret_ciphertext: row.try_get("secret_ciphertext")?,
                secret_nonce: row.try_get("secret_nonce")?,
                secret_key_version: row.try_get("secret_key_version")?,
                payload: row.try_get("payload")?,
                attempts: row.try_get("attempts")?,
                max_attempts: row.try_get("max_attempts")?,
                worker_id,
                lease_version: row.try_get("lease_version")?,
                expires_at: row.try_get("expires_at")?,
            })
        })
        .collect()
}

async fn send_delivery(
    _client: &Client,
    delivery: &ClaimedDelivery,
    now: DateTime<Utc>,
) -> Result<DeliveryResponse, SubscriptionError> {
    let body = serde_json::to_vec(&delivery.payload)?;
    let timestamp = now.timestamp();
    let secret = WebhookKeyring::from_env()?.decrypt(
        delivery.subscription_id,
        &delivery.secret_ciphertext,
        &delivery.secret_nonce,
        delivery.secret_key_version,
    )?;
    let signature = sign_payload(&secret, delivery.event_id, timestamp, &body)?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        "x-au-kpis-webhook-id",
        HeaderValue::from_str(&delivery.event_id.to_string())
            .map_err(|err| SubscriptionError::Validation(err.to_string()))?,
    );
    headers.insert(
        "x-au-kpis-webhook-timestamp",
        HeaderValue::from_str(&timestamp.to_string())
            .map_err(|err| SubscriptionError::Validation(err.to_string()))?,
    );
    headers.insert(
        "x-au-kpis-webhook-signature",
        HeaderValue::from_str(&signature)
            .map_err(|err| SubscriptionError::Validation(err.to_string()))?,
    );

    let destination = resolve_destination(&delivery.target_url).await?;
    let response = tokio::time::timeout(
        WEBHOOK_REQUEST_TIMEOUT,
        pinned_client(&destination)?
            .post(destination.url)
            .headers(headers)
            .body(body)
            .send(),
    )
    .await
    .map_err(|_| SubscriptionError::Validation("webhook request timed out".into()))??;

    let status = StatusCode::from_u16(response.status().as_u16())
        .map_err(|err| SubscriptionError::Validation(err.to_string()))?;
    Ok(DeliveryResponse {
        status,
        retry_after: parse_retry_after(response.headers(), now),
    })
}

fn parse_retry_after(headers: &HeaderMap, now: DateTime<Utc>) -> Option<Duration> {
    let value = headers.get(header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let retry_at = httpdate::parse_http_date(value).ok()?;
    retry_at.duration_since(SystemTime::from(now)).ok()
}

fn sign_payload(
    secret: &[u8],
    event_id: Uuid,
    timestamp: i64,
    body: &[u8],
) -> Result<String, SubscriptionError> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret)?;
    mac.update(event_id.to_string().as_bytes());
    mac.update(b".");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

#[derive(Debug)]
struct SuccessRecord {
    delivery_id: i64,
    worker_id: Uuid,
    lease_version: i64,
    subscription_id: Uuid,
    attempt: i32,
    status_code: u16,
    now: DateTime<Utc>,
    latency_ms: i64,
}

async fn record_delivery_success(
    pool: &PgPool,
    success: SuccessRecord,
) -> Result<(), SubscriptionError> {
    let mut tx = pool.begin().await?;
    let updated = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'delivered',
             attempts = $2,
             next_attempt_at = NULL,
             delivered_at = $3,
             last_status_code = $4,
             last_error = NULL,
             lease_owner = NULL,
             leased_until = NULL,
             updated_at = $3
         WHERE id = $1 AND lease_owner = $5 AND lease_version = $6",
    )
    .bind(success.delivery_id)
    .bind(success.attempt)
    .bind(success.now)
    .bind(i32::from(success.status_code))
    .bind(success.worker_id)
    .bind(success.lease_version)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() == 0 {
        return Err(SubscriptionError::LeaseLost);
    }
    sqlx::query(
        "INSERT INTO webhook_delivery_attempts (
             delivery_id, attempt_no, success, status_code, attempted_at, latency_ms,
             lease_version
         )
         VALUES ($1, $2, TRUE, $3, $4, $5, $6)",
    )
    .bind(success.delivery_id)
    .bind(success.attempt)
    .bind(i32::from(success.status_code))
    .bind(success.now)
    .bind(success.latency_ms)
    .bind(success.lease_version)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE webhook_subscriptions
         SET consecutive_failures = 0, last_success_at = $2, updated_at = $2
         WHERE id = $1",
    )
    .bind(success.subscription_id)
    .bind(success.now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

#[derive(Debug)]
struct FailureRecord {
    delivery_id: i64,
    subscription_id: Uuid,
    attempt: i32,
    max_attempts: i32,
    now: DateTime<Utc>,
    status_code: Option<u16>,
    error_message: String,
    latency_ms: i64,
    options: DeliveryOptions,
    worker_id: Uuid,
    lease_version: i64,
    expires_at: DateTime<Utc>,
    retry_after: Option<Duration>,
}

async fn record_delivery_failure(
    pool: &PgPool,
    failure: FailureRecord,
) -> Result<bool, SubscriptionError> {
    let remaining = (failure.expires_at - failure.now)
        .to_std()
        .unwrap_or(Duration::ZERO);
    let exhausted = failure.attempt >= failure.max_attempts || remaining.is_zero();
    let next_attempt_at = if exhausted {
        None
    } else {
        let delay = failure
            .retry_after
            .map_or_else(
                || {
                    chrono_backoff(failure.attempt, failure.options).and_then(|value| {
                        value
                            .to_std()
                            .map_err(|error| SubscriptionError::Validation(error.to_string()))
                    })
                },
                Ok,
            )?
            .min(remaining);
        Some(
            failure.now
                + ChronoDuration::from_std(delay)
                    .map_err(|error| SubscriptionError::Validation(error.to_string()))?,
        )
    };
    let status = if exhausted { "dead_letter" } else { "pending" };
    let status_code = failure.status_code.map(i32::from);

    let mut tx = pool.begin().await?;
    let updated = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = $2,
             attempts = $3,
             next_attempt_at = $4,
             last_status_code = $5,
             last_error = $6,
             lease_owner = NULL,
             leased_until = NULL,
             updated_at = $7
         WHERE id = $1 AND lease_owner = $8 AND lease_version = $9",
    )
    .bind(failure.delivery_id)
    .bind(status)
    .bind(failure.attempt)
    .bind(next_attempt_at)
    .bind(status_code)
    .bind(&failure.error_message)
    .bind(failure.now)
    .bind(failure.worker_id)
    .bind(failure.lease_version)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() == 0 {
        return Err(SubscriptionError::LeaseLost);
    }
    sqlx::query(
        "INSERT INTO webhook_delivery_attempts (
             delivery_id, attempt_no, success, status_code, error_message,
             attempted_at, latency_ms, lease_version
         )
         VALUES ($1, $2, FALSE, $3, $4, $5, $6, $7)",
    )
    .bind(failure.delivery_id)
    .bind(failure.attempt)
    .bind(status_code)
    .bind(&failure.error_message)
    .bind(failure.now)
    .bind(failure.latency_ms)
    .bind(failure.lease_version)
    .execute(&mut *tx)
    .await?;
    if exhausted {
        sqlx::query(
            "UPDATE webhook_subscriptions
             SET consecutive_failures = consecutive_failures + 1,
                 status = CASE WHEN consecutive_failures + 1 >= 5 THEN 'paused' ELSE status END,
                 paused_at = CASE WHEN consecutive_failures + 1 >= 5 THEN $2 ELSE paused_at END,
                 updated_at = $2
             WHERE id = $1 AND status = 'active'",
        )
        .bind(failure.subscription_id)
        .bind(failure.now)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    if exhausted {
        tracing::warn!(
            delivery_id = failure.delivery_id,
            subscription_id = %failure.subscription_id,
            attempt = failure.attempt,
            error = %failure.error_message,
            "webhook delivery failed permanently"
        );
    } else {
        tracing::warn!(
            delivery_id = failure.delivery_id,
            subscription_id = %failure.subscription_id,
            attempt = failure.attempt,
            next_attempt_at = ?next_attempt_at,
            error = %failure.error_message,
            "webhook delivery scheduled for retry"
        );
    }

    Ok(exhausted)
}

fn retryable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 409 | 425 | 429) || status.is_server_error()
}

fn effective_max_attempts(delivery: &ClaimedDelivery, options: DeliveryOptions) -> i32 {
    delivery.max_attempts.min(options.max_attempts)
}

fn chrono_backoff(
    attempt: i32,
    options: DeliveryOptions,
) -> Result<ChronoDuration, SubscriptionError> {
    let exponent = u32::try_from((attempt - 1).max(0)).unwrap_or(0).min(30);
    let multiplier = 2_u32.saturating_pow(exponent);
    let delay = options
        .base_backoff
        .saturating_mul(multiplier)
        .min(options.max_backoff);
    let jitter_millis =
        rand::thread_rng().gen_range(0..=delay.as_millis().min(u128::from(u64::MAX)) as u64);
    ChronoDuration::from_std(Duration::from_millis(jitter_millis))
        .map_err(|err| SubscriptionError::Validation(err.to_string()))
}

fn validate_delivery_options(options: DeliveryOptions) -> Result<(), SubscriptionError> {
    if options.max_attempts <= 0 {
        return Err(SubscriptionError::Validation(
            "max_attempts must be positive".into(),
        ));
    }
    if options.batch_size == 0 {
        return Err(SubscriptionError::Validation(
            "batch_size must be positive".into(),
        ));
    }
    if options.base_backoff.is_zero() {
        return Err(SubscriptionError::Validation(
            "base_backoff must be positive".into(),
        ));
    }
    Ok(())
}

fn subscription_error_to_api_error(err: SubscriptionError) -> ApiError {
    match err {
        SubscriptionError::Validation(message) => ApiError::Validation(message),
        SubscriptionError::NotFound => ApiError::NotFound("subscription".into()),
        SubscriptionError::LeaseLost => {
            tracing::warn!("stale webhook worker result rejected by lease fencing");
            ApiError::Internal
        }
        SubscriptionError::Db(err) => ApiError::Db(err),
        SubscriptionError::Http(err) => {
            tracing::error!(error = %err, "webhook subscription HTTP error");
            ApiError::Internal
        }
        SubscriptionError::Json(err) => {
            tracing::error!(error = %err, "webhook subscription JSON error");
            ApiError::Internal
        }
        SubscriptionError::Hmac(err) => {
            tracing::error!(error = %err, "webhook subscription signing error");
            ApiError::Internal
        }
        SubscriptionError::Crypto => {
            tracing::error!("webhook subscription encryption error");
            ApiError::Internal
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use au_kpis_domain::ids::{ArtifactId, DataflowId};
    use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
    use chrono::TimeZone as _;
    use sqlx::postgres::PgPoolOptions;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{
        ClaimedDelivery, DeliveryOptions, FailureRecord, SubscriptionError, WebhookDeliveryEvent,
        WebhookKeyring, chrono_backoff, delivery_payload, effective_max_attempts,
        generate_signing_secret, globally_routable, parse_retry_after, record_delivery_failure,
        retryable_status, run_webhook_delivery_worker, sign_payload,
        subscription_error_to_api_error, validate_delivery_options, validate_destination_url,
    };

    #[test]
    fn subscription_validation_covers_destination_shapes() {
        validate_destination_url("https://subscriber.example.test/hook")
            .expect("valid HTTPS hostname");

        let unsupported = validate_destination_url("ftp://subscriber.example.test/hook")
            .expect_err("unsupported scheme");
        assert!(matches!(unsupported, SubscriptionError::Validation(_)));

        let malformed = validate_destination_url("not a url").expect_err("malformed URL");
        assert!(matches!(malformed, SubscriptionError::Validation(_)));
        assert!(validate_destination_url("https://127.0.0.1/hook").is_err());
        assert!(validate_destination_url("https://user@example.com/hook").is_err());
        assert!(validate_destination_url("https://example.com:8443/hook").is_err());
    }

    #[tokio::test]
    async fn delivery_failure_retry_state_is_computed_before_database_write() {
        let pool = lazy_pool();
        let now = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let options = DeliveryOptions::default();

        let retry = record_delivery_failure(
            &pool,
            FailureRecord {
                delivery_id: 1,
                subscription_id: Uuid::new_v4(),
                attempt: 1,
                max_attempts: 2,
                now,
                status_code: Some(500),
                error_message: "server error".into(),
                latency_ms: 10,
                options,
                worker_id: Uuid::new_v4(),
                lease_version: 1,
                expires_at: now + chrono::Duration::hours(24),
                retry_after: None,
            },
        )
        .await;
        assert!(retry.is_err());

        let exhausted = record_delivery_failure(
            &pool,
            FailureRecord {
                delivery_id: 2,
                subscription_id: Uuid::new_v4(),
                attempt: 2,
                max_attempts: 2,
                now,
                status_code: None,
                error_message: "timeout".into(),
                latency_ms: 20,
                options,
                worker_id: Uuid::new_v4(),
                lease_version: 1,
                expires_at: now + chrono::Duration::hours(24),
                retry_after: None,
            },
        )
        .await;
        assert!(exhausted.is_err());
    }

    #[test]
    fn webhook_payload_signing_and_backoff_helpers_are_stable() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let event_id = Uuid::new_v4();
        let generation_id = Uuid::new_v4();
        let event = WebhookDeliveryEvent {
            id: event_id,
            generation_id,
            dataflow_id: DataflowId::new("abs.cpi").unwrap(),
            artifact_id: Some(ArtifactId::of_content(b"artifact")),
            observations_loaded: 42,
            occurred_at: now,
        };
        let payload = delivery_payload(&event);
        assert_eq!(payload["id"], event_id.to_string());
        assert_eq!(payload["generation_id"], generation_id.to_string());
        assert_eq!(payload["type"], "data.updated");
        assert_eq!(payload["observations_loaded"], 42);

        let (secret, secret_bytes) = generate_signing_secret();
        assert_eq!(secret.len(), 43);
        let signature = sign_payload(&secret_bytes, event_id, now.timestamp(), br#"{"ok":true}"#)
            .expect("sign payload");
        assert_eq!(signature.len(), 43);

        let options = DeliveryOptions {
            base_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(5),
            ..DeliveryOptions::default()
        };
        let first = chrono_backoff(1, options).expect("first retry");
        assert!(first >= chrono::Duration::zero());
        assert!(first <= chrono::Duration::seconds(2));
        let capped = chrono_backoff(10, options).expect("capped retry");
        assert!(capped >= chrono::Duration::zero());
        assert!(capped <= chrono::Duration::seconds(5));

        let delivery = ClaimedDelivery {
            id: 1,
            event_id,
            subscription_id: Uuid::new_v4(),
            target_url: "https://subscriber.example.test/hook".into(),
            secret_ciphertext: Vec::new(),
            secret_nonce: Vec::new(),
            secret_key_version: 1,
            payload,
            attempts: 0,
            max_attempts: 3,
            worker_id: Uuid::new_v4(),
            lease_version: 1,
            expires_at: now + chrono::Duration::hours(24),
        };
        assert_eq!(
            effective_max_attempts(
                &delivery,
                DeliveryOptions {
                    max_attempts: 5,
                    ..DeliveryOptions::default()
                },
            ),
            3
        );
    }

    #[test]
    fn encrypted_secret_uses_subscription_aad_and_rejects_tampering() {
        let keyring = WebhookKeyring::from_env().expect("development keyring");
        let subscription_id = Uuid::new_v4();
        let secret = [0x5a_u8; 32];
        let encrypted = keyring
            .encrypt(subscription_id, &secret)
            .expect("encrypt secret");
        assert_eq!(encrypted.nonce.len(), 12);
        assert_ne!(encrypted.ciphertext, secret);
        assert_eq!(
            keyring
                .decrypt(
                    subscription_id,
                    &encrypted.ciphertext,
                    &encrypted.nonce,
                    encrypted.key_version,
                )
                .expect("decrypt secret"),
            secret
        );
        assert!(
            keyring
                .decrypt(
                    Uuid::new_v4(),
                    &encrypted.ciphertext,
                    &encrypted.nonce,
                    encrypted.key_version,
                )
                .is_err()
        );
        let mut tampered = encrypted.ciphertext;
        tampered[0] ^= 1;
        assert!(
            keyring
                .decrypt(
                    subscription_id,
                    &tampered,
                    &encrypted.nonce,
                    encrypted.key_version,
                )
                .is_err()
        );
    }

    #[test]
    fn destination_ip_and_retry_policies_cover_security_boundaries() {
        for private in [
            "127.0.0.1",
            "10.1.2.3",
            "169.254.169.254",
            "192.0.2.10",
            "100.64.0.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
        ] {
            assert!(
                !globally_routable(private.parse().unwrap()),
                "{private} must be rejected"
            );
        }
        assert!(globally_routable("1.1.1.1".parse().unwrap()));
        assert!(globally_routable("2606:4700:4700::1111".parse().unwrap()));

        for retryable in [408, 409, 425, 429, 500, 503] {
            assert!(retryable_status(StatusCode::from_u16(retryable).unwrap()));
        }
        for permanent in [300, 301, 400, 401, 404, 422] {
            assert!(!retryable_status(StatusCode::from_u16(permanent).unwrap()));
        }
    }

    #[test]
    fn retry_after_accepts_seconds_and_http_dates() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::RETRY_AFTER, HeaderValue::from_static("17"));
        assert_eq!(
            parse_retry_after(&headers, now),
            Some(Duration::from_secs(17))
        );

        let retry_at = std::time::SystemTime::from(now) + Duration::from_secs(23);
        headers.insert(
            header::RETRY_AFTER,
            HeaderValue::from_str(&httpdate::fmt_http_date(retry_at)).unwrap(),
        );
        assert_eq!(
            parse_retry_after(&headers, now),
            Some(Duration::from_secs(23))
        );
    }

    #[test]
    fn subscription_errors_map_to_api_error_variants() {
        drop(subscription_error_to_api_error(
            SubscriptionError::Validation("bad subscription".into()),
        ));
        drop(subscription_error_to_api_error(SubscriptionError::Db(
            sqlx::Error::RowNotFound,
        )));
        drop(subscription_error_to_api_error(SubscriptionError::Json(
            serde_json::from_str::<serde_json::Value>("not json").unwrap_err(),
        )));
    }

    #[test]
    fn delivery_options_reject_zero_values() {
        assert!(matches!(
            validate_delivery_options(DeliveryOptions {
                max_attempts: 0,
                ..DeliveryOptions::default()
            }),
            Err(SubscriptionError::Validation(_))
        ));
        assert!(matches!(
            validate_delivery_options(DeliveryOptions {
                batch_size: 0,
                ..DeliveryOptions::default()
            }),
            Err(SubscriptionError::Validation(_))
        ));
        assert!(matches!(
            validate_delivery_options(DeliveryOptions {
                base_backoff: Duration::ZERO,
                ..DeliveryOptions::default()
            }),
            Err(SubscriptionError::Validation(_))
        ));
    }

    #[tokio::test]
    async fn delivery_worker_exits_when_shutdown_is_already_cancelled() {
        let pool = lazy_pool();
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        run_webhook_delivery_worker(
            pool,
            shutdown,
            DeliveryOptions::default(),
            Duration::from_millis(1),
        )
        .await
        .expect("worker exits cleanly");
    }

    fn lazy_pool() -> sqlx::PgPool {
        PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://postgres:postgres@localhost/au_kpis")
            .expect("lazy postgres pool")
    }
}
