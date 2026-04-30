//! sqlx + Timescale pool and migration bundle.
//!
//! Other crates depend on this crate to acquire a configured [`PgPool`]
//! without duplicating connect-time wiring. Queries that consumers need
//! will land in this crate over time (issues #11, #22, #27); for now it
//! owns the pool bootstrap and the embedded migration set that defines
//! the canonical schema (see `Spec.md § Database schema`).
//!
//! # Usage
//!
//! ```no_run
//! # async fn demo() -> Result<(), au_kpis_db::DbError> {
//! use au_kpis_config::DatabaseConfig;
//! use au_kpis_db::{connect, migrate};
//!
//! let cfg = DatabaseConfig { url: "postgres://localhost/au_kpis".into() };
//! let pool = connect(&cfg).await?;
//! migrate(&pool).await?;
//! # Ok(()) }
//! ```
//!
//! # Migration bundle
//!
//! Migrations live in `infra/migrations/` (canonical home per
//! `AGENTS.md § 2`) and are embedded into the binary at compile time
//! via the [`sqlx::migrate!`] macro. Consumers deploying this code do
//! **not** need the `infra/` tree at runtime — only the produced
//! binary — and CLI users can still run `sqlx migrate run --source
//! infra/migrations` against a live DB.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::time::Duration;

use au_kpis_config::DatabaseConfig;
use au_kpis_domain::{Artifact, SourceId, ids::ArtifactId};
use au_kpis_error::{Classify, CoreError, ErrorClass};
/// PostgreSQL connection pool used by database helpers.
pub use sqlx::PgPool;
use sqlx::{migrate::Migrator, postgres::PgPoolOptions};
use thiserror::Error;
use tracing::instrument;

/// Embedded migrations — applied by [`migrate`].
///
/// Points at `infra/migrations` relative to this crate's `Cargo.toml`;
/// the macro compiles the SQL into the binary so runtime has no
/// filesystem dependency on `infra/`.
pub static MIGRATOR: Migrator = sqlx::migrate!("../../infra/migrations");

/// Errors returned by this crate.
#[derive(Debug, Error)]
pub enum DbError {
    /// Shared validation or JSON failure.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// Establishing (or growing) the pool failed.
    #[error("db connect: {0}")]
    Connect(#[source] sqlx::Error),

    /// A query executed against the pool failed.
    #[error("db query: {0}")]
    Query(#[source] sqlx::Error),

    /// Applying (or reverting) a migration failed.
    #[error("migration: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

impl Classify for DbError {
    fn class(&self) -> ErrorClass {
        match self {
            DbError::Core(err) => err.class(),
            // Connect/query failures are usually transient (restart,
            // lock contention, network); retrying with backoff is the
            // right default. Callers that know otherwise wrap in a
            // permanent variant of their own error type.
            DbError::Connect(_) | DbError::Query(_) => ErrorClass::Transient,
            // A failed migration means the schema is not in the state
            // the binary expects; retrying in-place won't change that.
            DbError::Migrate(_) => ErrorClass::Permanent,
        }
    }
}

/// Insert a raw source artifact row.
///
/// The artifact id is the SHA-256 digest of the stored bytes; using it as
/// the primary key makes repeated fetches of identical upstream content
/// idempotent. On duplicate content the original provenance row is preserved
/// unchanged.
#[instrument(skip(pool, artifact))]
pub async fn upsert_artifact(pool: &PgPool, artifact: &Artifact) -> Result<(), DbError> {
    upsert_artifact_record(pool, artifact).await?;
    Ok(())
}

/// Insert a raw source artifact row and return the durable row.
///
/// Duplicate content is a no-op at the database layer, so callers receive the
/// already-persisted provenance instead of their candidate metadata.
#[instrument(skip(pool, artifact))]
pub async fn upsert_artifact_record(
    pool: &PgPool,
    artifact: &Artifact,
) -> Result<Artifact, DbError> {
    let size_bytes = i64::try_from(artifact.size_bytes).map_err(|_| {
        CoreError::Validation(format!(
            "artifact `{}` size {} exceeds BIGINT",
            artifact.id, artifact.size_bytes
        ))
    })?;
    let response_headers =
        serde_json::to_value(&artifact.response_headers).map_err(CoreError::from)?;

    sqlx::query!(
        r#"INSERT INTO artifacts (
             id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO NOTHING"#,
        artifact.id.digest().as_bytes().as_slice(),
        artifact.source_id.as_str(),
        &artifact.source_url,
        &artifact.content_type,
        response_headers,
        size_bytes,
        &artifact.storage_key,
        artifact.fetched_at,
    )
    .execute(pool)
    .await
    .map_err(DbError::Query)?;

    get_artifact(pool, artifact.id)
        .await?
        .ok_or_else(|| DbError::Core(CoreError::NotFound(format!("artifact `{}`", artifact.id))))
}

/// Load a raw source artifact row by content hash.
#[instrument(skip(pool))]
pub async fn get_artifact(pool: &PgPool, id: ArtifactId) -> Result<Option<Artifact>, DbError> {
    let row = sqlx::query!(
        r#"SELECT id,
                  source_id,
                  source_url,
                  content_type,
                  response_headers AS "response_headers!: serde_json::Value",
                  size_bytes,
                  storage_key,
                  fetched_at
         FROM artifacts
         WHERE id = $1"#,
        id.digest().as_bytes().as_slice(),
    )
    .fetch_optional(pool)
    .await
    .map_err(DbError::Query)?;

    row.map(|row| {
        let id_array = artifact_id_bytes(row.id)?;
        let source_id =
            SourceId::new(row.source_id).map_err(|err| CoreError::Validation(err.to_string()))?;
        let response_headers =
            serde_json::from_value(row.response_headers).map_err(CoreError::from)?;
        let size_bytes = u64::try_from(row.size_bytes).map_err(|_| {
            CoreError::Validation(format!(
                "artifact size from database is negative: {}",
                row.size_bytes
            ))
        })?;

        Ok(Artifact {
            id: ArtifactId::from_digest(au_kpis_domain::ids::Sha256Digest::from_bytes(id_array)),
            source_id,
            source_url: row.source_url,
            content_type: row.content_type,
            response_headers,
            size_bytes,
            storage_key: row.storage_key,
            fetched_at: row.fetched_at,
        })
    })
    .transpose()
}

fn artifact_id_bytes(id_bytes: Vec<u8>) -> Result<[u8; 32], DbError> {
    id_bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| {
            CoreError::Validation(format!(
                "artifact id from database has {} bytes, expected 32",
                bytes.len()
            ))
        })
        .map_err(DbError::Core)
}

/// Tunables for [`connect_with`]. Defaults suit a single API binary; workers
/// that open long-running transactions should raise `max_connections`.
#[derive(Debug, Clone, Copy)]
pub struct ConnectOptions {
    /// Upper bound on pooled connections.
    pub max_connections: u32,
    /// Minimum pool size kept warm.
    pub min_connections: u32,
    /// Maximum time a caller waits to acquire a connection from the pool.
    pub acquire_timeout: Duration,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            max_connections: 8,
            min_connections: 0,
            acquire_timeout: Duration::from_secs(5),
        }
    }
}

/// Connect using [`ConnectOptions::default`] and enable the TimescaleDB
/// extension on the target database.
pub async fn connect(cfg: &DatabaseConfig) -> Result<PgPool, DbError> {
    connect_with(cfg, ConnectOptions::default()).await
}

/// Connect with caller-supplied pool options and enable the TimescaleDB
/// extension on the target database.
///
/// The extension is created with `IF NOT EXISTS`, which is a no-op on
/// managed Timescale Cloud (where it's pre-installed) and a one-shot
/// install step in local / test environments backed by the upstream
/// `timescale/timescaledb` image.
#[instrument(skip(cfg), fields(url.len = cfg.url.len()))]
pub async fn connect_with(cfg: &DatabaseConfig, opts: ConnectOptions) -> Result<PgPool, DbError> {
    let pool = PgPoolOptions::new()
        .max_connections(opts.max_connections)
        .min_connections(opts.min_connections)
        .acquire_timeout(opts.acquire_timeout)
        .connect(&cfg.url)
        .await
        .map_err(DbError::Connect)?;
    ensure_timescale(&pool).await?;
    Ok(pool)
}

/// Ensure the `timescaledb` extension is loaded on the target database.
///
/// Idempotent: safe to call multiple times. Surfaced separately so
/// tests and tooling can reach it without going through [`connect`].
#[instrument(skip(pool))]
pub async fn ensure_timescale(pool: &PgPool) -> Result<(), DbError> {
    sqlx::query("CREATE EXTENSION IF NOT EXISTS timescaledb")
        .execute(pool)
        .await
        .map_err(DbError::Query)?;
    Ok(())
}

/// Installed TimescaleDB extension version, or `None` if the extension
/// is absent (e.g. a fresh database before [`ensure_timescale`] has
/// run). Returns the raw `pg_extension.extversion` string so callers
/// can decide whether to compare `SemVer` or log-and-continue.
///
/// Compile-checked via the `sqlx::query_scalar!` macro; `.sqlx/`
/// prepared metadata in-repo lets CI build without a live database.
#[instrument(skip(pool))]
pub async fn timescale_version(pool: &PgPool) -> Result<Option<String>, DbError> {
    sqlx::query_scalar!("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        .fetch_optional(pool)
        .await
        .map_err(DbError::Query)
}

/// Apply every pending migration from the embedded [`MIGRATOR`].
///
/// Uses sqlx's advisory-lock guard so concurrent callers on the same
/// database serialise naturally — safe to call on every binary start.
#[instrument(skip(pool))]
pub async fn migrate(pool: &PgPool) -> Result<(), DbError> {
    MIGRATOR.run(pool).await?;
    Ok(())
}

/// Revert the most recently applied migration. Intended for tooling
/// and the idempotency test; production binaries should call [`migrate`]
/// instead.
#[instrument(skip(pool))]
pub async fn revert_latest(pool: &PgPool) -> Result<(), DbError> {
    MIGRATOR.undo(pool, -1).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_error_is_transient() {
        let err = DbError::Connect(sqlx::Error::PoolClosed);
        assert_eq!(err.class(), ErrorClass::Transient);
    }

    #[test]
    fn core_error_classification_flows_through() {
        let err = DbError::Core(CoreError::Validation("bad artifact".into()));
        assert_eq!(err.class(), ErrorClass::Validation);
    }

    #[test]
    fn migrate_error_is_permanent() {
        let err = DbError::Migrate(sqlx::migrate::MigrateError::VersionMismatch(1));
        assert_eq!(err.class(), ErrorClass::Permanent);
    }

    #[test]
    fn migrator_is_non_empty() {
        // Catches the "forgot to commit a migration" footgun: if the
        // `infra/migrations` tree ever becomes empty the macro silently
        // produces an empty bundle.
        assert!(
            MIGRATOR.iter().next().is_some(),
            "embedded migrator must contain at least one migration"
        );
    }

    #[test]
    fn default_connect_options_are_conservative() {
        let opts = ConnectOptions::default();
        assert!(opts.max_connections >= 1);
        assert!(opts.min_connections <= opts.max_connections);
        assert!(opts.acquire_timeout >= Duration::from_secs(1));
    }
}
