use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterHttpClient, AdapterManifest, Adapters, ArtifactRef, DiscoveredJob,
    DiscoveryCtx, FetchCtx, NoopArtifactRecorder, ObservationStream, ParseCtx, RateLimit,
    SourceAdapter, retry_after_delta,
};
use au_kpis_domain::{
    Artifact, ArtifactId, DataflowId, MeasureId, Observation, ObservationStatus, SeriesDescriptor,
    SourceId, TimePrecision,
    ids::{CodeId, DimensionId, SeriesKey},
};
use au_kpis_error::{Classify, CoreError, ErrorClass};
use au_kpis_storage::BlobStore;
use au_kpis_storage::StorageError;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::{StreamExt, stream};
use object_store::memory::InMemory;

#[derive(Debug)]
struct StubAdapter {
    manifest: AdapterManifest,
}

impl StubAdapter {
    fn new() -> Self {
        Self {
            manifest: AdapterManifest {
                source_id: SourceId::new("stub").unwrap(),
                name: "Stub source".into(),
                version: "test".into(),
                rate_limit: RateLimit::new(60, Duration::from_secs(60)).unwrap(),
                dataflows: vec![DataflowId::new("stub.cpi").unwrap()],
            },
        }
    }
}

#[async_trait]
impl SourceAdapter for StubAdapter {
    fn id(&self) -> &'static str {
        "stub"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    #[tracing::instrument(skip(self, _ctx), fields(source = self.id()))]
    async fn discover(&self, _ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        Ok(vec![DiscoveredJob {
            id: "job-1".into(),
            source_id: self.manifest.source_id.clone(),
            dataflow_id: self.manifest.dataflows[0].clone(),
            source_url: "https://example.test/cpi.json".into(),
            metadata: BTreeMap::from([("kind".into(), "fixture".into())]),
        }])
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id(), job_id = %job.id))]
    async fn fetch(&self, job: DiscoveredJob, ctx: &FetchCtx) -> Result<ArtifactRef, AdapterError> {
        let bytes = Bytes::from_static(br#"{"value": 123.4}"#);
        let id = ctx.blob_store.put_artifact(bytes.clone()).await?;
        Ok(ArtifactRef {
            id,
            source_id: job.source_id,
            source_url: job.source_url,
            content_type: "application/json".into(),
            response_headers: BTreeMap::from([(
                "content-type".into(),
                vec!["application/json".into()],
            )]),
            storage_key: format!("artifacts/{}", id.to_hex()),
            size_bytes: bytes.len() as u64,
            fetched_at: ctx.started_at,
        })
    }

    fn parse<'a>(&'a self, artifact: ArtifactRef, ctx: &'a ParseCtx) -> ObservationStream<'a> {
        let dataflow_id = self.manifest.dataflows[0].clone();
        let dimensions = BTreeMap::from([(
            DimensionId::new("region").unwrap(),
            CodeId::new("AUS").unwrap(),
        )]);
        let series_key = SeriesKey::derive(
            &dataflow_id,
            dimensions
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        let descriptor = SeriesDescriptor {
            series_key,
            dataflow_id,
            measure_id: MeasureId::new("index").unwrap(),
            dimensions,
            unit: "index".into(),
        };
        let observation = Observation {
            series_key,
            time: Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
            time_precision: TimePrecision::Quarter,
            value: Some(123.4),
            status: ObservationStatus::Normal,
            revision_no: 0,
            attributes: BTreeMap::new(),
            ingested_at: ctx.started_at,
            source_artifact_id: artifact.id,
        };

        Box::pin(stream::iter([Ok((descriptor, observation))]))
    }
}

#[tokio::test]
async fn registry_dispatches_discover_fetch_and_parse() {
    let mut builder = Adapters::builder();
    builder.register(StubAdapter::new()).unwrap();
    let adapters = builder.build();

    let http = AdapterHttpClient::new(RateLimit::new(60, Duration::from_secs(60)).unwrap());
    let blob_store = BlobStore::new(InMemory::new());
    let started_at = Utc.with_ymd_and_hms(2026, 4, 28, 0, 0, 0).unwrap();

    let jobs = adapters
        .discover("stub", &DiscoveryCtx::new(http.clone(), started_at))
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, "job-1");

    let artifact = adapters
        .fetch(
            "stub",
            jobs[0].clone(),
            &FetchCtx::new(
                http.clone(),
                blob_store.clone(),
                started_at,
                Arc::new(NoopArtifactRecorder),
            ),
        )
        .await
        .unwrap();
    assert_eq!(artifact.id, ArtifactId::of_content(br#"{"value": 123.4}"#));

    let rows: Vec<_> = adapters
        .parse(
            "stub",
            artifact,
            &ParseCtx::new(http, blob_store, started_at),
        )
        .unwrap()
        .collect()
        .await;
    assert_eq!(rows.len(), 1);
    let (series, observation) = rows.into_iter().next().unwrap().unwrap();
    assert_eq!(series.unit, "index");
    assert_eq!(observation.value, Some(123.4));
}

#[test]
fn registry_rejects_duplicate_adapter_ids() {
    let mut builder = Adapters::builder();
    builder.register(StubAdapter::new()).unwrap();
    let err = builder.register(StubAdapter::new()).unwrap_err();
    assert!(matches!(err, AdapterError::DuplicateAdapter(id) if id == "stub"));
}

#[test]
fn registry_rejects_manifest_id_mismatch() {
    #[derive(Debug)]
    struct BadAdapter {
        inner: StubAdapter,
    }

    #[async_trait]
    impl SourceAdapter for BadAdapter {
        fn id(&self) -> &'static str {
            "other"
        }

        fn manifest(&self) -> &AdapterManifest {
            self.inner.manifest()
        }

        async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
            self.inner.discover(ctx).await
        }

        async fn fetch(
            &self,
            job: DiscoveredJob,
            ctx: &FetchCtx,
        ) -> Result<ArtifactRef, AdapterError> {
            self.inner.fetch(job, ctx).await
        }

        fn parse<'a>(&'a self, artifact: ArtifactRef, ctx: &'a ParseCtx) -> ObservationStream<'a> {
            self.inner.parse(artifact, ctx)
        }
    }

    let err = Adapters::builder()
        .register(BadAdapter {
            inner: StubAdapter::new(),
        })
        .unwrap_err();
    assert!(matches!(err, AdapterError::Validation(message) if message.contains("does not match")));
}

#[test]
fn registry_reports_unknown_adapter() {
    let adapters = Adapters::builder().build();
    let err = adapters.get("missing").unwrap_err();
    assert!(matches!(err, AdapterError::UnknownAdapter(id) if id == "missing"));
}

#[test]
fn register_arc_supports_shared_adapter_values() {
    let adapter: Arc<dyn SourceAdapter> = Arc::new(StubAdapter::new());
    let mut builder = Adapters::builder();
    builder.register_arc(adapter).unwrap();
    assert_eq!(builder.build().len(), 1);
}

#[test]
fn rate_limit_rejects_invalid_configurations() {
    let zero_requests = RateLimit::new(0, Duration::from_secs(1)).unwrap_err();
    assert!(
        matches!(zero_requests, AdapterError::Validation(message) if message.contains("max_requests"))
    );

    let zero_window = RateLimit::new(1, Duration::ZERO).unwrap_err();
    assert!(matches!(zero_window, AdapterError::Validation(message) if message.contains("window")));
}

#[test]
fn adapter_error_classification_matches_retry_policy() {
    let core = AdapterError::Core(CoreError::Validation("bad input".into()));
    assert_eq!(core.class(), ErrorClass::Validation);

    let storage = AdapterError::Storage(StorageError::Source("upstream reset".into()));
    assert_eq!(storage.class(), ErrorClass::Transient);

    assert_eq!(
        AdapterError::UnknownAdapter("missing".into()).class(),
        ErrorClass::Permanent
    );
    assert_eq!(
        AdapterError::DuplicateAdapter("stub".into()).class(),
        ErrorClass::Permanent
    );
    assert_eq!(
        AdapterError::FormatDrift("schema hash changed".into()).class(),
        ErrorClass::Permanent
    );
    assert_eq!(
        AdapterError::Validation("missing source_url".into()).class(),
        ErrorClass::Validation
    );
    assert_eq!(
        AdapterError::UpstreamStatus {
            status: reqwest::StatusCode::REQUEST_TIMEOUT,
            retry_after: None,
            response_headers: BTreeMap::new(),
        }
        .class(),
        ErrorClass::Transient
    );
    assert_eq!(
        AdapterError::UpstreamStatus {
            status: reqwest::StatusCode::BAD_REQUEST,
            retry_after: None,
            response_headers: BTreeMap::new(),
        }
        .class(),
        ErrorClass::Permanent
    );
}

#[test]
fn retry_after_delta_accepts_http_date_values() {
    let retry_after = retry_after_delta(&BTreeMap::from([(
        "retry-after".into(),
        vec!["Fri, 01 Jan 2100 00:00:00 GMT".into()],
    )]))
    .expect("parse HTTP-date Retry-After");

    assert!(retry_after > Duration::from_secs(24 * 60 * 60));
}

#[test]
fn artifact_ref_roundtrips_domain_artifact() {
    let fetched_at = Utc.with_ymd_and_hms(2026, 4, 28, 1, 2, 3).unwrap();
    let artifact = Artifact {
        id: ArtifactId::of_content(b"fixture"),
        source_id: SourceId::new("stub").unwrap(),
        source_url: "https://example.test/fixture.json".into(),
        content_type: "application/json".into(),
        response_headers: BTreeMap::from([("etag".into(), vec!["\"fixture\"".into()])]),
        size_bytes: 7,
        storage_key: "artifacts/fixture".into(),
        fetched_at,
    };

    let reference: ArtifactRef = artifact.clone().into();
    assert_eq!(reference.id, artifact.id);
    assert_eq!(Artifact::from(reference), artifact);
}

#[test]
fn empty_registry_reports_empty_and_parse_unknown_adapter() {
    let adapters = Adapters::builder().build();
    assert!(adapters.is_empty());
    assert_eq!(adapters.len(), 0);

    let http = AdapterHttpClient::new(RateLimit::new(60, Duration::from_secs(60)).unwrap());
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = ArtifactRef {
        id: ArtifactId::of_content(b"fixture"),
        source_id: SourceId::new("stub").unwrap(),
        source_url: "https://example.test/fixture.json".into(),
        content_type: "application/json".into(),
        response_headers: BTreeMap::new(),
        storage_key: "artifacts/fixture".into(),
        size_bytes: 7,
        fetched_at: Utc.with_ymd_and_hms(2026, 4, 28, 1, 2, 3).unwrap(),
    };

    let err = match adapters.parse(
        "missing",
        artifact,
        &ParseCtx::new(
            http,
            blob_store,
            Utc.with_ymd_and_hms(2026, 4, 28, 1, 2, 3).unwrap(),
        ),
    ) {
        Ok(_) => panic!("missing adapter unexpectedly returned a parse stream"),
        Err(err) => err,
    };
    assert!(matches!(err, AdapterError::UnknownAdapter(id) if id == "missing"));
}
