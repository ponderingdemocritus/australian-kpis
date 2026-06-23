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
    let Ok(plaintext) = header.to_str() else {
        return Ok(None);
    };

    let manager = ApiKeyManager::new(state.db.clone(), state.cache.clone());
    match manager.verify(plaintext).await {
        Ok(verified) => {
            request.extensions_mut().insert(verified.clone());
            Ok(Some(verified))
        }
        Err(AuthError::InvalidApiKey | AuthError::Validation(_)) => Ok(None),
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

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        fmt,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use au_kpis_cache::{
        CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig,
    };
    use au_kpis_config::{
        AppConfig, CacheConfig, DatabaseConfig, HttpConfig, RateLimitConfig, RateLimitQuotaConfig,
        RateLimitTierConfig, TelemetryConfig,
    };
    use au_kpis_telemetry::Telemetry;
    use axum::{
        body::Body,
        http::{HeaderMap, HeaderName, HeaderValue, Request, header},
    };
    use sqlx::postgres::PgPoolOptions;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use crate::{ApiError, AppState};

    use super::{
        RateLimitOutcome, X_RATE_LIMIT_LIMIT, X_RATE_LIMIT_REMAINING, X_RATE_LIMIT_RESET,
        bucket_check, buckets_for_quota, check_request_limit, client_ip, duration_header_secs,
        insert_rate_limit_error_headers, insert_rate_limit_headers, tier_config, verified_key,
    };

    #[test]
    fn client_ip_prefers_forwarded_for_and_falls_back_to_real_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", HeaderValue::from_static("198.51.100.9"));
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static(" 203.0.113.10, 198.51.100.1"),
        );
        assert_eq!(client_ip(&headers).as_deref(), Some("203.0.113.10"));

        headers.insert("x-forwarded-for", HeaderValue::from_static("   "));
        assert_eq!(client_ip(&headers).as_deref(), Some("198.51.100.9"));

        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_bytes(&[0xff]).expect("non-UTF8 header value"),
        );
        assert_eq!(client_ip(&headers).as_deref(), Some("198.51.100.9"));

        headers.clear();
        assert_eq!(client_ip(&headers), None);
    }

    #[test]
    fn duration_headers_round_fractional_seconds_up() {
        assert_eq!(duration_header_secs(Duration::ZERO), 0);
        assert_eq!(duration_header_secs(Duration::from_secs(3)), 3);
        assert_eq!(
            duration_header_secs(Duration::new(3, 1)),
            4,
            "fractional reset values must be conservative"
        );
    }

    #[test]
    fn rate_limit_headers_include_success_and_error_metadata() {
        let outcome = RateLimitOutcome {
            allowed: true,
            limit: 10,
            remaining: 7,
            retry_after: Duration::from_millis(1500),
            reset_after: Duration::from_millis(2500),
        };
        let mut headers = HeaderMap::new();
        insert_rate_limit_headers(&mut headers, &outcome);
        assert_eq!(headers[&X_RATE_LIMIT_LIMIT], "10");
        assert_eq!(headers[&X_RATE_LIMIT_REMAINING], "7");
        assert_eq!(headers[&X_RATE_LIMIT_RESET], "3");

        let limited = outcome.into_rate_limited();
        assert!(matches!(
            limited,
            ApiError::RateLimited {
                retry_after,
                limit: 10,
                remaining: 7,
                reset_after,
            } if retry_after == Duration::from_millis(1500)
                && reset_after == Duration::from_millis(1500)
        ));

        headers.clear();
        insert_rate_limit_error_headers(
            &mut headers,
            Duration::from_millis(1),
            12,
            0,
            Duration::from_secs(5),
        );
        assert_eq!(headers[header::RETRY_AFTER], "1");
        assert_eq!(headers[&X_RATE_LIMIT_LIMIT], "12");
        assert_eq!(headers[&X_RATE_LIMIT_REMAINING], "0");
        assert_eq!(headers[&X_RATE_LIMIT_RESET], "5");
    }

    #[test]
    fn tier_config_uses_requested_tier_then_default() {
        let free = RateLimitTierConfig {
            per_key: quota(60, 1_000, 2),
            per_ip: quota(10, 100, 2),
        };
        let pro = RateLimitTierConfig {
            per_key: quota(600, 10_000, 3),
            per_ip: quota(100, 1_000, 3),
        };
        let config = RateLimitConfig {
            default_tier: "free".into(),
            tiers: BTreeMap::from([("free".into(), free.clone()), ("pro".into(), pro.clone())]),
            anonymous: quota(1, 10, 1),
        };

        assert_eq!(
            tier_config(&config, "pro").unwrap().per_key.per_hour,
            10_000
        );
        assert_eq!(
            tier_config(&config, "unknown").unwrap().per_key.per_hour,
            1_000
        );

        let missing_default = RateLimitConfig {
            default_tier: "missing".into(),
            tiers: BTreeMap::new(),
            anonymous: quota(1, 10, 1),
        };
        assert!(tier_config(&missing_default, "unknown").is_err());
    }

    #[test]
    fn bucket_helpers_normalize_zero_quota_inputs() {
        let bucket = bucket_check("ip:unknown", "second", 0, Duration::from_secs(1), 0)
            .expect("zero rate inputs are normalized to one-token buckets");
        assert_eq!(bucket.key, "rate-limit:ip:unknown:second");
        assert_eq!(bucket.limit, 1);
        assert_eq!(bucket.reset_after, Duration::from_secs(1));

        let buckets = buckets_for_quota("key:abc", quota(0, 5, 0)).expect("buckets");
        assert_eq!(buckets[0].key, "rate-limit:key:abc:hour");
        assert_eq!(buckets[0].limit, 5);
        assert_eq!(buckets[1].key, "rate-limit:key:abc:second");
        assert_eq!(buckets[1].limit, 1);
    }

    #[test]
    fn invalid_display_values_are_not_inserted_as_headers() {
        struct InvalidDisplay;

        impl fmt::Display for InvalidDisplay {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bad\nvalue")
            }
        }

        let mut headers = HeaderMap::new();
        super::insert_header(
            &mut headers,
            HeaderName::from_static("x-test-invalid"),
            InvalidDisplay,
        );
        assert!(!headers.contains_key("x-test-invalid"));
    }

    #[tokio::test]
    async fn anonymous_limit_uses_ip_buckets_and_returns_last_success() {
        let state = state_with_rate_limits(
            RateLimitConfig::default(),
            vec![decision(true, 9), decision(true, 8)],
        );

        let outcome = check_request_limit(&state, Some("203.0.113.9"), None)
            .await
            .expect("anonymous request allowed");

        assert_eq!(outcome.remaining, 8);
    }

    #[tokio::test]
    async fn verified_key_reuses_extensions_and_ignores_absent_or_invalid_headers() {
        let state = state_with_rate_limits(RateLimitConfig::default(), vec![]);
        let verified = au_kpis_auth::VerifiedApiKey {
            id: Uuid::new_v4(),
            name: "client".into(),
            scopes: vec!["observations:read".into()],
            rate_limit_tier: "free".into(),
        };

        let mut with_extension = Request::builder().body(Body::empty()).unwrap();
        with_extension.extensions_mut().insert(verified.clone());
        assert_eq!(
            verified_key(&state, &mut with_extension).await.unwrap(),
            Some(verified)
        );

        let mut missing = Request::builder().body(Body::empty()).unwrap();
        assert_eq!(verified_key(&state, &mut missing).await.unwrap(), None);

        let mut non_utf8 = Request::builder().body(Body::empty()).unwrap();
        non_utf8.headers_mut().insert(
            "x-api-key",
            HeaderValue::from_bytes(&[0xff]).expect("non-UTF8 header"),
        );
        assert_eq!(verified_key(&state, &mut non_utf8).await.unwrap(), None);
    }

    #[tokio::test]
    async fn key_limit_uses_default_tier_and_reports_key_bucket_denial() {
        let state = state_with_rate_limits(
            RateLimitConfig::default(),
            vec![
                decision(true, 99),
                decision(true, 98),
                decision(true, 97),
                decision(false, 0),
            ],
        );
        let verified = au_kpis_auth::VerifiedApiKey {
            id: Uuid::new_v4(),
            name: "client".into(),
            scopes: vec!["observations:read".into()],
            rate_limit_tier: "missing-tier".into(),
        };

        let err = check_request_limit(&state, None, Some(&verified))
            .await
            .expect_err("key bucket should deny");
        let ApiError::RateLimited {
            limit,
            remaining,
            retry_after,
            reset_after,
        } = err
        else {
            panic!("expected rate limit error");
        };
        assert_eq!(limit, 120);
        assert_eq!(remaining, 0);
        assert_eq!(retry_after, Duration::from_secs(1));
        assert_eq!(reset_after, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn ip_bucket_denial_short_circuits_key_checks() {
        let state = state_with_rate_limits(RateLimitConfig::default(), vec![decision(false, 0)]);
        let verified = au_kpis_auth::VerifiedApiKey {
            id: Uuid::new_v4(),
            name: "client".into(),
            scopes: vec![],
            rate_limit_tier: "free".into(),
        };

        let err = check_request_limit(&state, Some("203.0.113.9"), Some(&verified))
            .await
            .expect_err("ip bucket should deny");

        assert!(matches!(err, ApiError::RateLimited { limit: 200, .. }));
    }

    fn quota(per_second: u32, per_hour: u32, burst_multiplier: u32) -> RateLimitQuotaConfig {
        RateLimitQuotaConfig {
            per_second,
            per_hour,
            burst_multiplier,
        }
    }

    fn decision(allowed: bool, remaining: u32) -> RateLimitDecision {
        RateLimitDecision {
            allowed,
            remaining,
            retry_after: Duration::from_secs(1),
        }
    }

    fn state_with_rate_limits(
        rate_limits: RateLimitConfig,
        decisions: Vec<RateLimitDecision>,
    ) -> AppState {
        let config = AppConfig {
            http: HttpConfig::default(),
            database: DatabaseConfig {
                url: "postgres://postgres:postgres@localhost/unused".into(),
            },
            cache: CacheConfig {
                url: "redis://localhost:6379".into(),
            },
            telemetry: TelemetryConfig::default(),
            rate_limits,
        };
        AppState::new(
            PgPoolOptions::new()
                .connect_lazy("postgres://postgres:postgres@localhost/unused")
                .expect("lazy pool"),
            Arc::new(CacheClient::from_backend(DecisionBackend::new(decisions))),
            Arc::new(config),
            Arc::new(Telemetry::disabled()),
            CancellationToken::new(),
        )
    }

    #[derive(Debug)]
    struct DecisionBackend {
        decisions: Mutex<VecDeque<RateLimitDecision>>,
    }

    impl DecisionBackend {
        fn new(decisions: Vec<RateLimitDecision>) -> Self {
            Self {
                decisions: Mutex::new(decisions.into()),
            }
        }
    }

    #[async_trait::async_trait]
    impl CacheBackend for DecisionBackend {
        async fn get(&self, _key: &str) -> Result<Option<String>, CacheError> {
            Ok(None)
        }

        async fn set(&self, _key: &str, _value: String, _ttl: Duration) -> Result<(), CacheError> {
            Ok(())
        }

        async fn delete(&self, _key: &str) -> Result<bool, CacheError> {
            Ok(false)
        }

        async fn take_token_bucket(
            &self,
            _key: &str,
            _config: TokenBucketConfig,
            _requested: u32,
            _now_ms: u64,
        ) -> Result<RateLimitDecision, CacheError> {
            Ok(self
                .decisions
                .lock()
                .expect("decision lock")
                .pop_front()
                .expect("rate-limit decision"))
        }
    }
}
