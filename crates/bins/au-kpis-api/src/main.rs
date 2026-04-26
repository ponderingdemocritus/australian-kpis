//! API server binary.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{future::pending, sync::Arc, time::Duration};

use anyhow::Context;
use au_kpis_api_http::{AppState, router};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::load;
use au_kpis_telemetry::{Telemetry, init as init_telemetry};
use sqlx::postgres::PgPoolOptions;
use tokio::{net::TcpListener, signal};
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let config = Arc::new(load(None).context("load config")?);
    let telemetry = Arc::new(init_or_disabled(&config.telemetry)?);
    let db = PgPoolOptions::new()
        .connect_lazy(&config.database.url)
        .context("create lazy postgres pool")?;
    let shutdown = CancellationToken::new();
    let state = AppState::new(
        db,
        Arc::new(CacheClient::from_backend(NoopCacheBackend)),
        config.clone(),
        telemetry,
        shutdown.clone(),
    );

    let app = router(state).context("build router")?;
    let listener = TcpListener::bind(&config.http.bind)
        .await
        .with_context(|| format!("bind listener on {}", config.http.bind))?;

    tokio::spawn(shutdown_signal(shutdown.clone()));

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .context("serve api")?;

    Ok(())
}

fn init_or_disabled(config: &au_kpis_config::TelemetryConfig) -> anyhow::Result<Telemetry> {
    match init_telemetry(config) {
        Ok(telemetry) => Ok(telemetry),
        Err(err) if err.to_string() == "global telemetry subscriber already installed" => {
            Ok(Telemetry::disabled())
        }
        Err(err) => Err(err).context("initialize telemetry"),
    }
}

async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                let _ = stream.recv().await;
            }
            Err(_) => pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }

    token.cancel();
}
