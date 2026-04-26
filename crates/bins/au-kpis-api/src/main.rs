//! API server binary.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    env,
    future::{IntoFuture, pending},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use au_kpis_api_http::{AppState, router};
use au_kpis_cache::CacheClient;
use au_kpis_config::load;
use au_kpis_telemetry::{Telemetry, init as init_telemetry};
use sqlx::postgres::PgPoolOptions;
use tokio::{net::TcpListener, signal};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let config = Arc::new(load(None).context("load config")?);
    let telemetry = Arc::new(init_or_disabled(&config.telemetry)?);
    let db = PgPoolOptions::new()
        .connect_lazy(&config.database.url)
        .context("create lazy postgres pool")?;
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
    write_startup_notify(&listener).context("write startup notification")?;

    tokio::spawn(shutdown_signal(shutdown.clone()));

    let drain_window = Duration::from_secs(config.http.shutdown_grace_period_secs);
    let cancel_watch = shutdown.clone();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => {
            result.context("serve api")?;
        }
        _ = cancel_watch.cancelled() => {
            tokio::time::timeout(drain_window, &mut server)
                .await
                .with_context(|| format!(
                    "graceful shutdown exceeded {}s drain window",
                    drain_window.as_secs()
                ))?
                .context("serve api")?;
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

fn write_startup_notify(listener: &TcpListener) -> anyhow::Result<()> {
    let Some(path) = env::var_os("AU_KPIS_STARTUP_NOTIFY_FILE") else {
        return Ok(());
    };

    let addr = listener
        .local_addr()
        .context("read bound listener address")?;
    std::fs::write(path, addr.to_string()).context("persist bound listener address")?;
    Ok(())
}
