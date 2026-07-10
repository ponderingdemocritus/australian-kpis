//! Leased webhook delivery worker binary.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{sync::Arc, time::Duration};

use anyhow::Context;
use au_kpis_api_http::{DeliveryOptions, run_webhook_delivery_worker};
use au_kpis_config::load;
use au_kpis_db::connect as connect_db;
use au_kpis_telemetry::{Telemetry, init as init_telemetry};
use tokio::signal;
use tokio_util::sync::CancellationToken;

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let config = Arc::new(load(None).context("load config")?);
    let _telemetry = init_or_disabled(&config.telemetry)?;
    let db = connect_db(&config.database)
        .await
        .context("connect postgres database")?;
    let shutdown = CancellationToken::new();
    let worker = run_webhook_delivery_worker(
        db,
        shutdown.clone(),
        DeliveryOptions::default(),
        POLL_INTERVAL,
    );
    tokio::pin!(worker);

    tokio::select! {
        result = &mut worker => result.context("run webhook delivery worker")?,
        result = shutdown_signal() => {
            result.context("listen for shutdown signal")?;
            shutdown.cancel();
            tokio::time::timeout(SHUTDOWN_GRACE, &mut worker)
                .await
                .context("webhook worker shutdown grace elapsed")?
                .context("drain webhook delivery worker")?;
        }
    }
    Ok(())
}

fn init_or_disabled(config: &au_kpis_config::TelemetryConfig) -> anyhow::Result<Telemetry> {
    match init_telemetry(config) {
        Ok(telemetry) => Ok(telemetry),
        Err(error) if error.to_string() == "global telemetry subscriber already installed" => {
            Ok(Telemetry::disabled())
        }
        Err(error) => Err(error).context("initialize telemetry"),
    }
}

async fn shutdown_signal() -> anyhow::Result<()> {
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
        result = ctrl_c => result,
        result = terminate => result,
    }
}
