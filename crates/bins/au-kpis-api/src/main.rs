//! API server binary.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{env, future::IntoFuture, sync::Arc, time::Duration};

use anyhow::Context;
use au_kpis_api_http::{AppState, router};
use au_kpis_cache::CacheClient;
use au_kpis_config::load;
use au_kpis_db::connect as connect_db;
use au_kpis_telemetry::{Telemetry, init as init_telemetry};
use tokio::{net::TcpListener, signal};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let config = Arc::new(load(None).context("load config")?);
    let telemetry = Arc::new(init_or_disabled(&config.telemetry)?);
    let db = connect_db(&config.database)
        .await
        .context("connect postgres database")?;
    let cache = Arc::new(
        CacheClient::connect(&config.cache.url)
            .await
            .context("connect redis cache")?,
    );
    let shutdown = CancellationToken::new();
    let state = AppState::new(db, cache, config.clone(), telemetry, shutdown.clone());

    let app = router(state).context("build router")?;
    let listener = TcpListener::bind(&config.http.bind)
        .await
        .with_context(|| format!("bind listener on {}", config.http.bind))?;
    write_startup_notify(&listener)
        .await
        .context("write startup notification")?;

    let drain_window = Duration::from_secs(config.http.shutdown_grace_period_secs);
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    tokio::pin!(server);
    let shutdown_listener = shutdown_signal(shutdown);
    tokio::pin!(shutdown_listener);

    tokio::select! {
        result = &mut server => {
            result.context("serve api")?;
        }
        result = &mut shutdown_listener => {
            result.context("listen for shutdown signal")?;
            match tokio::time::timeout(drain_window, &mut server).await {
                Ok(result) => result.context("serve api")?,
                Err(_) => {
                    tracing::warn!(
                        drain_window_secs = drain_window.as_secs(),
                        "graceful shutdown drain window elapsed; forcing server exit"
                    );
                }
            }
        }
    }

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

async fn shutdown_signal(token: CancellationToken) -> anyhow::Result<()> {
    let ctrl_c = async { signal::ctrl_c().await.context("install Ctrl-C handler") };

    #[cfg(unix)]
    let terminate = async {
        let mut stream = signal::unix::signal(signal::unix::SignalKind::terminate())
            .context("install SIGTERM handler")?;
        stream.recv().await.context("SIGTERM stream closed")
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<anyhow::Result<()>>();

    tokio::select! {
        result = ctrl_c => result?,
        result = terminate => result?,
    }

    token.cancel();
    Ok(())
}

async fn write_startup_notify(listener: &TcpListener) -> anyhow::Result<()> {
    let Some(path) = env::var_os("AU_KPIS_STARTUP_NOTIFY_FILE") else {
        return Ok(());
    };

    let addr = listener
        .local_addr()
        .context("read bound listener address")?;
    tokio::fs::write(path, addr.to_string())
        .await
        .context("persist bound listener address")?;
    Ok(())
}
