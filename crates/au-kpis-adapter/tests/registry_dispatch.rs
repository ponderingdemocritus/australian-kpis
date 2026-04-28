use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterHttpClient, AdapterManifest, Adapters, ArtifactRef, DiscoveredJob,
    DiscoveryCtx, FetchCtx, ObservationStream, ParseCtx, RateLimit, SourceAdapter,
};
use au_kpis_domain::{
    ArtifactId, DataflowId, MeasureId, Observation, ObservationStatus, SeriesDescriptor, SourceId,
    TimePrecision,
    ids::{CodeId, DimensionId, SeriesKey},
};
use au_kpis_storage::BlobStore;
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
            &FetchCtx::new(http.clone(), blob_store.clone(), started_at),
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
