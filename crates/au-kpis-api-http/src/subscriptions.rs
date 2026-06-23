//! `/v1/subscriptions` and webhook delivery helpers.

use std::time::{Duration, Instant};

use au_kpis_domain::ids::{ArtifactId, DataflowId};
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use hmac::{Hmac, Mac, digest::InvalidLength};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::{PgPool, Row};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{ApiError, AppState, auth::RequiredApiKey};

type HmacSha256 = Hmac<Sha256>;

const EVENT_DATA_UPDATED: &str = "data.updated";
const DEFAULT_DELIVERY_MAX_ATTEMPTS: i32 = 5;
const DEFAULT_DELIVERY_BATCH_SIZE: u32 = 100;
const DEFAULT_DELIVERY_BASE_BACKOFF: Duration = Duration::from_secs(30);
const DEFAULT_DELIVERY_MAX_BACKOFF: Duration = Duration::from_secs(60 * 60);
const DEFAULT_DELIVERY_POLL_INTERVAL: Duration = Duration::from_secs(5);
const WEBHOOK_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// HMAC signing secret shown once at creation.
    pub signing_secret: String,
}

/// Response body for `POST /v1/subscriptions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateSubscriptionResponse {
    /// Created subscription.
    pub subscription: SubscriptionDetails,
}

/// Data-update event used to fan out webhook delivery rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookDeliveryEvent {
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
}

/// `POST /v1/subscriptions`.
#[utoipa::path(
    post,
    operation_id = "createSubscription",
    path = "/v1/subscriptions",
    request_body(content = CreateSubscriptionRequest, content_type = "application/json"),
    responses(
        (status = 201, description = "Subscription created.", body = CreateSubscriptionResponse, content_type = "application/json"),
        (status = 400, description = "Invalid subscription request.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 401, description = "Missing or invalid API key.", body = crate::ProblemDetails, content_type = "application/problem+json"),
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
    let response = create_subscription_record(&state.db, api_key.id, request)
        .await
        .map_err(subscription_error_to_api_error)?;
    Ok((StatusCode::CREATED, Json(response)).into_response())
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
             subscription_id, event_type, dataflow_id, artifact_id, payload,
             status, attempts, max_attempts, next_attempt_at
         )
         SELECT id, $2, $1, $3, $4, 'pending', 0, $5, $6
         FROM webhook_subscriptions
         WHERE status = 'active'
           AND (cardinality(dataflow_ids) = 0 OR $1 = ANY(dataflow_ids))",
    )
    .bind(event.dataflow_id.as_str())
    .bind(EVENT_DATA_UPDATED)
    .bind(artifact_bytes)
    .bind(payload)
    .bind(DEFAULT_DELIVERY_MAX_ATTEMPTS)
    .bind(event.occurred_at)
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
    let deliveries = claim_due_deliveries(pool, now, options.batch_size).await?;
    let mut outcome = DeliveryRunOutcome::default();

    for delivery in deliveries {
        outcome.attempted += 1;
        let attempt = delivery.attempts + 1;
        let started = Instant::now();
        let result = send_delivery(client, &delivery, now).await;
        let latency_ms = started.elapsed().as_millis().min(i64::MAX as u128) as i64;

        match result {
            Ok(status) if status.is_success() => {
                record_delivery_success(
                    pool,
                    delivery.id,
                    attempt,
                    status.as_u16(),
                    now,
                    latency_ms,
                )
                .await?;
                outcome.delivered += 1;
                tracing::info!(
                    delivery_id = delivery.id,
                    subscription_id = %delivery.subscription_id,
                    attempt,
                    status_code = status.as_u16(),
                    "webhook delivery accepted"
                );
            }
            Ok(status) => {
                if record_delivery_failure(
                    pool,
                    FailureRecord {
                        delivery_id: delivery.id,
                        subscription_id: delivery.subscription_id,
                        attempt,
                        max_attempts: effective_max_attempts(&delivery, options),
                        now,
                        status_code: Some(status.as_u16()),
                        error_message: format!("subscriber returned HTTP {}", status.as_u16()),
                        latency_ms,
                        options,
                    },
                )
                .await?
                {
                    outcome.failed += 1;
                }
            }
            Err(err) => {
                let failed = record_delivery_failure(
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
                    },
                )
                .await?;
                if failed {
                    outcome.failed += 1;
                }
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
    let signing_secret = generate_signing_secret();
    let dataflow_ids = request
        .dataflow_ids
        .iter()
        .map(DataflowId::as_str)
        .collect::<Vec<_>>();

    let row = sqlx::query(
        "INSERT INTO webhook_subscriptions (
             id, api_key_id, target_url, dataflow_ids, signing_secret
         )
         VALUES ($1, $2, $3, $4, $5)
         RETURNING created_at",
    )
    .bind(id)
    .bind(api_key_id)
    .bind(&request.url)
    .bind(dataflow_ids)
    .bind(&signing_secret)
    .fetch_one(pool)
    .await?;

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
            status: "active".into(),
            created_at: row.get("created_at"),
            signing_secret,
        },
    })
}

async fn validate_subscription_request(
    pool: &PgPool,
    request: &CreateSubscriptionRequest,
) -> Result<(), SubscriptionError> {
    let url = url::Url::parse(&request.url)
        .map_err(|err| SubscriptionError::Validation(format!("invalid url: {err}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(SubscriptionError::Validation(
            "url must use http or https".into(),
        ));
    }

    for dataflow_id in &request.dataflow_ids {
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

fn generate_signing_secret() -> String {
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    URL_SAFE_NO_PAD.encode(bytes)
}

fn delivery_payload(event: &WebhookDeliveryEvent) -> Value {
    json!({
        "event": EVENT_DATA_UPDATED,
        "dataflow_id": event.dataflow_id,
        "artifact_id": event.artifact_id,
        "observations_loaded": event.observations_loaded,
        "occurred_at": event.occurred_at,
    })
}

#[derive(Debug, Clone)]
struct ClaimedDelivery {
    id: i64,
    subscription_id: Uuid,
    target_url: String,
    signing_secret: String,
    payload: Value,
    attempts: i32,
    max_attempts: i32,
}

async fn claim_due_deliveries(
    pool: &PgPool,
    now: DateTime<Utc>,
    batch_size: u32,
) -> Result<Vec<ClaimedDelivery>, SubscriptionError> {
    let rows = sqlx::query(
        "WITH due AS (
             SELECT id
             FROM webhook_deliveries
             WHERE status = 'pending'
               AND next_attempt_at <= $1
             ORDER BY next_attempt_at ASC, id ASC
             LIMIT $2
             FOR UPDATE SKIP LOCKED
         )
         UPDATE webhook_deliveries d
         SET status = 'delivering',
             updated_at = $1
         FROM due, webhook_subscriptions s
         WHERE d.id = due.id
           AND s.id = d.subscription_id
         RETURNING d.id, d.subscription_id, s.target_url, s.signing_secret,
                   d.payload, d.attempts, d.max_attempts",
    )
    .bind(now)
    .bind(i64::from(batch_size))
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(ClaimedDelivery {
                id: row.try_get("id")?,
                subscription_id: row.try_get("subscription_id")?,
                target_url: row.try_get("target_url")?,
                signing_secret: row.try_get("signing_secret")?,
                payload: row.try_get("payload")?,
                attempts: row.try_get("attempts")?,
                max_attempts: row.try_get("max_attempts")?,
            })
        })
        .collect()
}

async fn send_delivery(
    client: &Client,
    delivery: &ClaimedDelivery,
    now: DateTime<Utc>,
) -> Result<StatusCode, SubscriptionError> {
    let body = serde_json::to_vec(&delivery.payload)?;
    let timestamp = now.to_rfc3339();
    let signature = sign_payload(&delivery.signing_secret, &timestamp, &body)?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        "x-au-kpis-webhook-id",
        HeaderValue::from_str(&delivery.id.to_string())
            .map_err(|err| SubscriptionError::Validation(err.to_string()))?,
    );
    headers.insert(
        "x-au-kpis-webhook-timestamp",
        HeaderValue::from_str(&timestamp)
            .map_err(|err| SubscriptionError::Validation(err.to_string()))?,
    );
    headers.insert(
        "x-au-kpis-webhook-signature",
        HeaderValue::from_str(&signature)
            .map_err(|err| SubscriptionError::Validation(err.to_string()))?,
    );

    let response = tokio::time::timeout(
        WEBHOOK_REQUEST_TIMEOUT,
        client
            .post(&delivery.target_url)
            .headers(headers)
            .body(body)
            .send(),
    )
    .await
    .map_err(|_| SubscriptionError::Validation("webhook request timed out".into()))??;

    StatusCode::from_u16(response.status().as_u16())
        .map_err(|err| SubscriptionError::Validation(err.to_string()))
}

fn sign_payload(secret: &str, timestamp: &str, body: &[u8]) -> Result<String, SubscriptionError> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    Ok(format!(
        "sha256={}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

async fn record_delivery_success(
    pool: &PgPool,
    delivery_id: i64,
    attempt: i32,
    status_code: u16,
    now: DateTime<Utc>,
    latency_ms: i64,
) -> Result<(), SubscriptionError> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO webhook_delivery_attempts (
             delivery_id, attempt_no, success, status_code, attempted_at, latency_ms
         )
         VALUES ($1, $2, TRUE, $3, $4, $5)",
    )
    .bind(delivery_id)
    .bind(attempt)
    .bind(i32::from(status_code))
    .bind(now)
    .bind(latency_ms)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'delivered',
             attempts = $2,
             next_attempt_at = NULL,
             delivered_at = $3,
             last_status_code = $4,
             last_error = NULL,
             updated_at = $3
         WHERE id = $1",
    )
    .bind(delivery_id)
    .bind(attempt)
    .bind(now)
    .bind(i32::from(status_code))
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
}

async fn record_delivery_failure(
    pool: &PgPool,
    failure: FailureRecord,
) -> Result<bool, SubscriptionError> {
    let exhausted = failure.attempt >= failure.max_attempts;
    let next_attempt_at = if exhausted {
        None
    } else {
        Some(failure.now + chrono_backoff(failure.attempt, failure.options)?)
    };
    let status = if exhausted { "failed" } else { "pending" };
    let status_code = failure.status_code.map(i32::from);

    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO webhook_delivery_attempts (
             delivery_id, attempt_no, success, status_code, error_message,
             attempted_at, latency_ms
         )
         VALUES ($1, $2, FALSE, $3, $4, $5, $6)",
    )
    .bind(failure.delivery_id)
    .bind(failure.attempt)
    .bind(status_code)
    .bind(&failure.error_message)
    .bind(failure.now)
    .bind(failure.latency_ms)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = $2,
             attempts = $3,
             next_attempt_at = $4,
             last_status_code = $5,
             last_error = $6,
             updated_at = $7
         WHERE id = $1",
    )
    .bind(failure.delivery_id)
    .bind(status)
    .bind(failure.attempt)
    .bind(next_attempt_at)
    .bind(status_code)
    .bind(&failure.error_message)
    .bind(failure.now)
    .execute(&mut *tx)
    .await?;
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
    ChronoDuration::from_std(delay).map_err(|err| SubscriptionError::Validation(err.to_string()))
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
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use au_kpis_domain::ids::{ArtifactId, DataflowId};
    use chrono::TimeZone as _;
    use sqlx::postgres::PgPoolOptions;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{
        ClaimedDelivery, CreateSubscriptionRequest, DeliveryOptions, FailureRecord,
        SubscriptionError, WebhookDeliveryEvent, chrono_backoff, delivery_payload,
        effective_max_attempts, generate_signing_secret, record_delivery_failure,
        run_webhook_delivery_worker, sign_payload, validate_delivery_options,
        validate_subscription_request,
    };

    #[tokio::test]
    async fn subscription_validation_covers_url_and_empty_filter_paths() {
        let pool = lazy_pool();

        validate_subscription_request(
            &pool,
            &CreateSubscriptionRequest {
                url: "https://subscriber.example.test/hook".into(),
                dataflow_ids: Vec::new(),
            },
        )
        .await
        .expect("valid URL with no dataflow filters does not query the database");

        let unsupported = validate_subscription_request(
            &pool,
            &CreateSubscriptionRequest {
                url: "ftp://subscriber.example.test/hook".into(),
                dataflow_ids: Vec::new(),
            },
        )
        .await
        .expect_err("unsupported scheme");
        assert!(matches!(unsupported, SubscriptionError::Validation(_)));

        let malformed = validate_subscription_request(
            &pool,
            &CreateSubscriptionRequest {
                url: "not a url".into(),
                dataflow_ids: Vec::new(),
            },
        )
        .await
        .expect_err("malformed URL");
        assert!(matches!(malformed, SubscriptionError::Validation(_)));
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
            },
        )
        .await;
        assert!(exhausted.is_err());
    }

    #[test]
    fn webhook_payload_signing_and_backoff_helpers_are_stable() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let event = WebhookDeliveryEvent {
            dataflow_id: DataflowId::new("abs.cpi").unwrap(),
            artifact_id: Some(ArtifactId::of_content(b"artifact")),
            observations_loaded: 42,
            occurred_at: now,
        };
        let payload = delivery_payload(&event);
        assert_eq!(payload["event"], "data.updated");
        assert_eq!(payload["observations_loaded"], 42);

        let secret = generate_signing_secret();
        assert!(secret.len() >= 43);
        let signature =
            sign_payload(&secret, &now.to_rfc3339(), br#"{"ok":true}"#).expect("sign payload");
        assert!(signature.starts_with("sha256="));

        let options = DeliveryOptions {
            base_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(5),
            ..DeliveryOptions::default()
        };
        assert_eq!(
            chrono_backoff(1, options).expect("first retry"),
            chrono::Duration::seconds(2)
        );
        assert_eq!(
            chrono_backoff(10, options).expect("capped retry"),
            chrono::Duration::seconds(5)
        );

        let delivery = ClaimedDelivery {
            id: 1,
            subscription_id: Uuid::new_v4(),
            target_url: "https://subscriber.example.test/hook".into(),
            signing_secret: secret,
            payload,
            attempts: 0,
            max_attempts: 3,
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
