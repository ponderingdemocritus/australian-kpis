//! Shared HTTP application state.

use std::sync::Arc;

use au_kpis_cache::CacheClient;
use au_kpis_config::AppConfig;
use au_kpis_telemetry::Telemetry;
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// Shared application state.
#[derive(Debug, Clone)]
pub struct AppState {
    /// Shared Postgres pool.
    pub db: PgPool,
    /// Shared cache client.
    pub cache: Arc<CacheClient>,
    /// Immutable runtime config.
    pub config: Arc<AppConfig>,
    /// Telemetry handle kept alive for process lifetime.
    pub telemetry: Arc<Telemetry>,
    /// Global shutdown token.
    pub shutdown: CancellationToken,
    pub(crate) short_admission: Arc<Semaphore>,
    pub(crate) bulk_admission: Arc<Semaphore>,
}

impl AppState {
    /// Construct a new shared application state bundle.
    pub fn new(
        db: PgPool,
        cache: Arc<CacheClient>,
        config: Arc<AppConfig>,
        telemetry: Arc<Telemetry>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            db,
            cache,
            config,
            telemetry,
            shutdown,
            short_admission: Arc::new(Semaphore::new(256)),
            bulk_admission: Arc::new(Semaphore::new(4)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use au_kpis_cache::{
        CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig,
    };
    use au_kpis_config::{
        AppConfig, CacheConfig, DatabaseConfig, HttpConfig, LogFormat, RateLimitConfig,
        TelemetryConfig,
    };
    use au_kpis_telemetry::Telemetry;
    use sqlx::postgres::PgPoolOptions;
    use tokio_util::sync::CancellationToken;

    use super::AppState;

    #[tokio::test]
    async fn per_replica_admission_capacities_are_fixed() {
        let state = test_state();
        let short = (0..256)
            .map(|_| state.short_admission.clone().try_acquire_owned().unwrap())
            .collect::<Vec<_>>();
        assert!(state.short_admission.clone().try_acquire_owned().is_err());
        drop(short);
        assert_eq!(state.short_admission.available_permits(), 256);

        let bulk = (0..4)
            .map(|_| state.bulk_admission.clone().try_acquire_owned().unwrap())
            .collect::<Vec<_>>();
        assert!(state.bulk_admission.clone().try_acquire_owned().is_err());
        drop(bulk);
        assert_eq!(state.bulk_admission.available_permits(), 4);
    }

    fn test_state() -> AppState {
        let db = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@127.0.0.1/au_kpis")
            .unwrap();
        AppState::new(
            db,
            Arc::new(CacheClient::from_backend(NoopCache)),
            Arc::new(AppConfig {
                http: HttpConfig {
                    bind: "127.0.0.1:0".into(),
                    cors_allowed_origins: Vec::new(),
                    shutdown_grace_period_secs: 30,
                },
                database: DatabaseConfig {
                    url: "postgres://postgres:postgres@127.0.0.1/au_kpis".into(),
                },
                cache: CacheConfig {
                    url: "redis://127.0.0.1:6379".into(),
                },
                telemetry: TelemetryConfig {
                    service_name: "test".into(),
                    log_format: LogFormat::Json,
                    log_level: "info".into(),
                    otlp_endpoint: None,
                },
                rate_limits: RateLimitConfig::default(),
            }),
            Arc::new(Telemetry::disabled()),
            CancellationToken::new(),
        )
    }

    #[derive(Debug)]
    struct NoopCache;

    #[async_trait]
    impl CacheBackend for NoopCache {
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
            unreachable!()
        }
    }
}
