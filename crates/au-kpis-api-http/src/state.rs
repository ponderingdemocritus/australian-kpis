//! Shared HTTP application state.

use std::sync::Arc;

use au_kpis_cache::CacheClient;
use au_kpis_config::AppConfig;
use au_kpis_telemetry::Telemetry;
use sqlx::PgPool;
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
        }
    }
}
