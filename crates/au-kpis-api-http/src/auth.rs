//! API-key middleware for protected routes.

use au_kpis_auth::{ApiKeyManager, AuthError, VerifiedApiKey};
use axum::{
    extract::{FromRequestParts, Request, State},
    http::{HeaderMap, request::Parts},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::{ApiError, AppState};

/// API-key identity required by protected handlers.
///
/// As a handler extractor this runs after axum has accepted the request method
/// for a route, so unsupported methods still receive axum's 405 response.
#[derive(Debug, Clone)]
pub struct RequiredApiKey {
    /// Verified API key identity.
    pub key: VerifiedApiKey,
}

#[axum::async_trait]
impl FromRequestParts<AppState> for RequiredApiKey {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if let Some(key) = parts.extensions.get::<VerifiedApiKey>() {
            return Ok(Self { key: key.clone() });
        }

        let key = verify_api_key_header(state, &parts.headers).await?;
        parts.extensions.insert(key.clone());
        Ok(Self { key })
    }
}

/// Validate the `X-API-Key` header and attach the verified key to extensions.
pub async fn require_api_key(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    if request.extensions().get::<VerifiedApiKey>().is_some() {
        return next.run(request).await;
    }

    match verify_api_key_header(&state, request.headers()).await {
        Ok(verified) => {
            request.extensions_mut().insert(verified);
            next.run(request).await
        }
        Err(err) => err.into_response(),
    }
}

/// Verify the `X-API-Key` header from a request.
pub async fn verify_api_key_header(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<VerifiedApiKey, ApiError> {
    let Some(header) = headers.get("x-api-key") else {
        return Err(unauthorized());
    };
    let Ok(plaintext) = header.to_str() else {
        return Err(unauthorized());
    };

    let manager = ApiKeyManager::new(state.db.clone(), state.cache.clone());
    match manager.verify(plaintext).await {
        Ok(verified) => Ok(verified),
        Err(AuthError::InvalidApiKey | AuthError::Validation(_)) => Err(unauthorized()),
        Err(err) => {
            tracing::error!(error = %err, "api key verification failed");
            Err(ApiError::Internal)
        }
    }
}

fn unauthorized() -> ApiError {
    ApiError::Unauthorized("missing or invalid API key".into())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use au_kpis_cache::{
        CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig,
    };
    use au_kpis_config::{
        AppConfig, CacheConfig, DatabaseConfig, HttpConfig, RateLimitConfig, TelemetryConfig,
    };
    use au_kpis_telemetry::Telemetry;
    use axum::http::{HeaderMap, HeaderValue};
    use sqlx::postgres::PgPoolOptions;
    use tokio_util::sync::CancellationToken;

    use super::verify_api_key_header;
    use crate::{ApiError, AppState};

    #[tokio::test]
    async fn api_key_header_rejects_missing_and_non_utf8_values() {
        let state = test_state();
        let missing = verify_api_key_header(&state, &HeaderMap::new())
            .await
            .expect_err("missing key");
        assert!(matches!(missing, ApiError::Unauthorized(_)));

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_bytes(&[0xff]).expect("non-UTF8 header"),
        );
        let invalid = verify_api_key_header(&state, &headers)
            .await
            .expect_err("invalid key");
        assert!(matches!(invalid, ApiError::Unauthorized(_)));

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("not-a-key"));
        let malformed = verify_api_key_header(&state, &headers)
            .await
            .expect_err("malformed key");
        assert!(matches!(malformed, ApiError::Unauthorized(_)));
    }

    fn test_state() -> AppState {
        AppState::new(
            PgPoolOptions::new()
                .connect_lazy("postgres://postgres:postgres@localhost/unused")
                .expect("lazy pool"),
            Arc::new(CacheClient::from_backend(NoopBackend)),
            Arc::new(AppConfig {
                http: HttpConfig::default(),
                database: DatabaseConfig {
                    url: "postgres://postgres:postgres@localhost/unused".into(),
                },
                cache: CacheConfig {
                    url: "redis://localhost:6379".into(),
                },
                telemetry: TelemetryConfig::default(),
                rate_limits: RateLimitConfig::default(),
            }),
            Arc::new(Telemetry::disabled()),
            CancellationToken::new(),
        )
    }

    #[derive(Debug)]
    struct NoopBackend;

    #[async_trait::async_trait]
    impl CacheBackend for NoopBackend {
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
            Ok(RateLimitDecision {
                allowed: true,
                remaining: 1,
                retry_after: Duration::ZERO,
            })
        }
    }
}
