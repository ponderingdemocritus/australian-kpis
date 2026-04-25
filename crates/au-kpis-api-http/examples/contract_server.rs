use std::{sync::Arc, time::Duration};

use au_kpis_api_http::{AppState, serve, shutdown_signal};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::{AppConfig, DatabaseConfig, HttpConfig, LogFormat, TelemetryConfig};
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct NoopCacheBackend;

#[async_trait::async_trait]
impl CacheBackend for NoopCacheBackend {
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
            remaining: 0,
            retry_after: Duration::ZERO,
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let addr = std::env::var("AU_KPIS_CONTRACT_ADDR")
        .ok()
        .unwrap_or_else(|| "127.0.0.1:38080".into());
    let shutdown = CancellationToken::new();
    let db = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://postgres:postgres@localhost/au_kpis")
        .expect("lazy postgres pool");
    let config = AppConfig {
        http: HttpConfig { bind: addr.clone() },
        database: DatabaseConfig {
            url: "postgres://postgres:postgres@localhost/au_kpis".into(),
        },
        telemetry: TelemetryConfig {
            service_name: "au-kpis-contract-server".into(),
            log_format: LogFormat::Json,
            log_level: "info".into(),
            otlp_endpoint: None,
        },
    };
    let state = AppState::new(
        Arc::new(db),
        Arc::new(CacheClient::from_backend(NoopCacheBackend)),
        Arc::new(config),
        shutdown.clone(),
    );
    tokio::spawn(shutdown_signal(shutdown));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind contract server listener");
    serve(listener, state).await.expect("serve contract server");
}
