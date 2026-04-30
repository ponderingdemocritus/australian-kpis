//! Ingestion orchestration (discover -> fetch -> parse -> load).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::sync::Arc;

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterHttpClient, ArtifactRecorder, ArtifactRecorderRef, FetchCtx,
};
use au_kpis_db::PgPool;
use au_kpis_domain::Artifact;
use au_kpis_error::Classify;
use au_kpis_storage::BlobStore;
use chrono::{DateTime, Utc};

/// DB-backed artifact provenance recorder for fetch workers.
#[derive(Debug, Clone)]
pub struct DbArtifactRecorder {
    pool: PgPool,
}

impl DbArtifactRecorder {
    /// Construct a recorder that writes artifact rows through `au-kpis-db`.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Return this recorder behind the trait object expected by [`FetchCtx`].
    #[must_use]
    pub fn shared(self) -> ArtifactRecorderRef {
        Arc::new(self)
    }
}

#[async_trait]
impl ArtifactRecorder for DbArtifactRecorder {
    async fn get(
        &self,
        id: au_kpis_domain::ids::ArtifactId,
    ) -> Result<Option<Artifact>, AdapterError> {
        au_kpis_db::get_artifact(&self.pool, id)
            .await
            .map_err(|err| AdapterError::artifact_record(err.to_string(), err.class()))
    }

    async fn record(&self, artifact: &Artifact) -> Result<Artifact, AdapterError> {
        au_kpis_db::upsert_artifact_record(&self.pool, artifact)
            .await
            .map_err(|err| AdapterError::artifact_record(err.to_string(), err.class()))
    }

    async fn repair_storage_key(
        &self,
        artifact: &Artifact,
        observed_storage_key: &str,
    ) -> Result<Artifact, AdapterError> {
        au_kpis_db::repair_artifact_storage_key(&self.pool, artifact, observed_storage_key)
            .await
            .map_err(|err| AdapterError::artifact_record(err.to_string(), err.class()))
    }
}

/// Build the fetch context used by ingestion workers.
#[must_use]
pub fn fetch_ctx(
    http: AdapterHttpClient,
    blob_store: BlobStore,
    started_at: DateTime<Utc>,
    pool: PgPool,
) -> FetchCtx {
    FetchCtx::new(
        http,
        blob_store,
        started_at,
        DbArtifactRecorder::new(pool).shared(),
    )
}
