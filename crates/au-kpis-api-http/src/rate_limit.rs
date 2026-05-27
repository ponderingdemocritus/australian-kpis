//! Redis token-bucket rate-limit middleware.

use std::time::Duration;

use au_kpis_auth::{ApiKeyManager, AuthError, VerifiedApiKey};
use au_kpis_cache::TokenBucketConfig;
use au_kpis_config::{RateLimitConfig, RateLimitQuotaConfig, RateLimitTierConfig};
use axum::{
    extract::{Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::{ApiError, AppState};

const X_RATE_LIMIT_LIMIT: HeaderName = HeaderName::from_static("x-ratelimit-limit");
const X_RATE_LIMIT_REMAINING: HeaderName = HeaderName::from_static("x-ratelimit-remaining");
const X_RATE_LIMIT_RESET: HeaderName = HeaderName::from_static("x-ratelimit-reset");

/// Enforce per-key and per-IP Redis token-bucket rate limits.
pub async fn rate_limit(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let verified = match verified_key(&state, &mut request).await {
        Ok(verified) => verified,
        Err(err) => return err.into_response(),
    };
    let client_ip = client_ip(request.headers());

    let outcome = match check_request_limit(&state, client_ip.as_deref(), verified.as_ref()).await {
        Ok(outcome) => outcome,
        Err(err) => return err.into_response(),
    };

    let mut response = next.run(request).await;
    insert_rate_limit_headers(response.headers_mut(), &outcome);
    response
}

async fn verified_key(
    state: &AppState,
    request: &mut Request,
) -> Result<Option<VerifiedApiKey>, ApiError> {
    if let Some(verified) = request.extensions().get::<VerifiedApiKey>().cloned() {
        return Ok(Some(verified));
    }

    let Some(header) = request.headers().get("x-api-key") else {
        return Ok(None);
    };
    let plaintext = header
        .to_str()
        .map_err(|_| ApiError::Unauthorized("missing or invalid API key".into()))?
        .to_owned();

    let manager = ApiKeyManager::new(state.db.clone(), state.cache.clone());
    match manager.verify(&plaintext).await {
        Ok(verified) => {
            request.extensions_mut().insert(verified.clone());
            Ok(Some(verified))
        }
        Err(AuthError::InvalidApiKey | AuthError::Validation(_)) => {
            Err(ApiError::Unauthorized("missing or invalid API key".into()))
        }
        Err(err) => {
            tracing::error!(error = %err, "api key verification failed during rate limiting");
            Err(ApiError::Internal)
        }
    }
}

async fn check_request_limit(
    state: &AppState,
    client_ip: Option<&str>,
    verified: Option<&VerifiedApiKey>,
) -> Result<RateLimitOutcome, ApiError> {
    let mut selected = None;
    let ip = client_ip.unwrap_or("unknown");

    let rate_limits = &state.config.rate_limits;
    let (ip_quota, key_quota) = match verified {
        Some(key) => {
            let tier = tier_config(rate_limits, &key.rate_limit_tier)?;
            (tier.per_ip, Some((key.id.to_string(), tier.per_key)))
        }
        None => (rate_limits.anonymous, None),
    };

    for bucket in buckets_for_quota(&format!("ip:{ip}"), ip_quota)? {
        let outcome = take_bucket(state, bucket).await?;
        if !outcome.allowed {
            return Err(outcome.into_rate_limited());
        }
        selected = Some(outcome);
    }

    if let Some((key_id, quota)) = key_quota {
        for bucket in buckets_for_quota(&format!("key:{key_id}"), quota)? {
            let outcome = take_bucket(state, bucket).await?;
            if !outcome.allowed {
                return Err(outcome.into_rate_limited());
            }
            selected = Some(outcome);
        }
    }

    selected.ok_or(ApiError::Internal)
}

fn tier_config<'a>(
    rate_limits: &'a RateLimitConfig,
    tier: &str,
) -> Result<&'a RateLimitTierConfig, ApiError> {
    rate_limits
        .tiers
        .get(tier)
        .or_else(|| rate_limits.tiers.get(&rate_limits.default_tier))
        .ok_or_else(|| ApiError::Validation("rate-limit tier is not configured".into()))
}

#[derive(Debug, Clone)]
struct BucketCheck {
    key: String,
    config: TokenBucketConfig,
    limit: u32,
    reset_after: Duration,
}

#[derive(Debug, Clone, Copy)]
struct RateLimitOutcome {
    allowed: bool,
    limit: u32,
    remaining: u32,
    retry_after: Duration,
    reset_after: Duration,
}

impl RateLimitOutcome {
    fn into_rate_limited(self) -> ApiError {
        ApiError::RateLimited {
            retry_after: self.retry_after,
            limit: self.limit,
            remaining: self.remaining,
            reset_after: self.retry_after,
        }
    }
}

fn buckets_for_quota(
    prefix: &str,
    quota: RateLimitQuotaConfig,
) -> Result<[BucketCheck; 2], ApiError> {
    Ok([
        bucket_check(
            prefix,
            "hour",
            quota.per_hour,
            Duration::from_secs(60 * 60),
            quota.burst_multiplier,
        )?,
        bucket_check(
            prefix,
            "second",
            quota.per_second,
            Duration::from_secs(1),
            quota.burst_multiplier,
        )?,
    ])
}

fn bucket_check(
    prefix: &str,
    window: &str,
    refill_tokens: u32,
    refill_interval: Duration,
    burst_multiplier: u32,
) -> Result<BucketCheck, ApiError> {
    let burst_multiplier = burst_multiplier.max(1);
    let capacity = refill_tokens.saturating_mul(burst_multiplier).max(1);
    let refill_tokens = refill_tokens.max(1);
    Ok(BucketCheck {
        key: format!("rate-limit:{prefix}:{window}"),
        config: TokenBucketConfig::new(capacity, refill_tokens, refill_interval)?,
        limit: capacity,
        reset_after: refill_interval,
    })
}

async fn take_bucket(state: &AppState, bucket: BucketCheck) -> Result<RateLimitOutcome, ApiError> {
    let decision = state
        .cache
        .take_token_bucket(&bucket.key, bucket.config, 1)
        .await?;
    Ok(RateLimitOutcome {
        allowed: decision.allowed,
        limit: bucket.limit,
        remaining: decision.remaining,
        retry_after: decision.retry_after,
        reset_after: bucket.reset_after,
    })
}

fn insert_rate_limit_headers(headers: &mut HeaderMap, outcome: &RateLimitOutcome) {
    insert_header(headers, X_RATE_LIMIT_LIMIT, outcome.limit);
    insert_header(headers, X_RATE_LIMIT_REMAINING, outcome.remaining);
    insert_header(
        headers,
        X_RATE_LIMIT_RESET,
        duration_header_secs(outcome.reset_after),
    );
}

fn insert_header<T: std::fmt::Display>(headers: &mut HeaderMap, name: HeaderName, value: T) {
    if let Ok(value) = HeaderValue::from_str(&value.to_string()) {
        headers.insert(name, value);
    }
}

fn client_ip(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
}

fn duration_header_secs(duration: Duration) -> u64 {
    if duration.is_zero() {
        return 0;
    }
    duration.as_secs() + u64::from(duration.subsec_nanos() > 0)
}

pub(crate) fn insert_rate_limit_error_headers(
    headers: &mut HeaderMap,
    retry_after: Duration,
    limit: u32,
    remaining: u32,
    reset_after: Duration,
) {
    insert_header(
        headers,
        header::RETRY_AFTER,
        duration_header_secs(retry_after),
    );
    insert_header(headers, X_RATE_LIMIT_LIMIT, limit);
    insert_header(headers, X_RATE_LIMIT_REMAINING, remaining);
    insert_header(
        headers,
        X_RATE_LIMIT_RESET,
        duration_header_secs(reset_after),
    );
}
