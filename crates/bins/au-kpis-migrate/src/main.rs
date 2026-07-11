//! One-shot database migration job.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use anyhow::Context;
use au_kpis_config::load;
use au_kpis_db::{connect, migrate};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let config = load(None).context("load migration configuration")?;
    let pool = connect(&config.database)
        .await
        .context("connect with DDL migration credential")?;
    migrate(&pool).await.context("apply database migrations")?;
    pool.close().await;
    Ok(())
}
