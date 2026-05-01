use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{AdapterError, AdapterHttpClient, ArtifactRecorder, FetchCtx, SourceAdapter};
use au_kpis_adapter_abs::AbsAdapter;
use au_kpis_domain::{Artifact, ArtifactId};
use au_kpis_error::{Classify, ErrorClass};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const SDMX_FIXTURE: &[u8] = br#"{"data":{"dataSets":[{"observations":{"0:0":[123.4]}}]}}"#;

#[derive(Debug, Default)]
struct RecordingArtifactRecorder {
    artifacts: tokio::sync::Mutex<Vec<Artifact>>,
}

#[derive(Debug, Default)]
struct FailingArtifactRecorder;

#[derive(Debug)]
struct ExistingColdArtifactRecorder {
    artifact: Artifact,
}

#[derive(Debug)]
struct BackfillingExistingArtifactRecorder {
    artifact: tokio::sync::Mutex<Artifact>,
    record_calls: tokio::sync::Mutex<usize>,
}

#[derive(Debug)]
struct RacingColdArtifactRecorder {
    artifact: Artifact,
}

#[async_trait]
impl ArtifactRecorder for RecordingArtifactRecorder {
    async fn get(&self, id: ArtifactId) -> Result<Option<Artifact>, AdapterError> {
        Ok(self
            .artifacts
            .lock()
            .await
            .iter()
            .find(|artifact| artifact.id == id)
            .cloned())
    }

    async fn record(&self, artifact: &Artifact) -> Result<Artifact, AdapterError> {
        self.artifacts.lock().await.push(artifact.clone());
        Ok(artifact.clone())
    }

    async fn repair_storage_key(
        &self,
        artifact: &Artifact,
        _observed_storage_key: &str,
    ) -> Result<Artifact, AdapterError> {
        let mut artifacts = self.artifacts.lock().await;
        if let Some(stored) = artifacts.iter_mut().find(|stored| stored.id == artifact.id) {
            stored.storage_key.clone_from(&artifact.storage_key);
            return Ok(stored.clone());
        }
        artifacts.push(artifact.clone());
        Ok(artifact.clone())
    }
}

#[async_trait]
impl ArtifactRecorder for ExistingColdArtifactRecorder {
    async fn get(&self, id: ArtifactId) -> Result<Option<Artifact>, AdapterError> {
        Ok((self.artifact.id == id).then(|| self.artifact.clone()))
    }

    async fn record(&self, _artifact: &Artifact) -> Result<Artifact, AdapterError> {
        Ok(self.artifact.clone())
    }

    async fn repair_storage_key(
        &self,
        artifact: &Artifact,
        _observed_storage_key: &str,
    ) -> Result<Artifact, AdapterError> {
        Ok(artifact.clone())
    }
}

#[async_trait]
impl ArtifactRecorder for BackfillingExistingArtifactRecorder {
    async fn get(&self, id: ArtifactId) -> Result<Option<Artifact>, AdapterError> {
        let artifact = self.artifact.lock().await;
        Ok((artifact.id == id).then(|| artifact.clone()))
    }

    async fn record(&self, artifact: &Artifact) -> Result<Artifact, AdapterError> {
        *self.record_calls.lock().await += 1;
        let mut stored = self.artifact.lock().await;
        if stored.response_headers.is_empty() && !artifact.response_headers.is_empty() {
            stored.response_headers = artifact.response_headers.clone();
        }
        Ok(stored.clone())
    }

    async fn repair_storage_key(
        &self,
        _artifact: &Artifact,
        _observed_storage_key: &str,
    ) -> Result<Artifact, AdapterError> {
        panic!("verified duplicate artifact should backfill with record, not repair")
    }
}

#[async_trait]
impl ArtifactRecorder for RacingColdArtifactRecorder {
    async fn get(&self, _id: ArtifactId) -> Result<Option<Artifact>, AdapterError> {
        Ok(None)
    }

    async fn record(&self, _artifact: &Artifact) -> Result<Artifact, AdapterError> {
        Ok(self.artifact.clone())
    }

    async fn repair_storage_key(
        &self,
        _artifact: &Artifact,
        _observed_storage_key: &str,
    ) -> Result<Artifact, AdapterError> {
        panic!("durable cold blob exists, so repair should not run")
    }
}

#[async_trait]
impl ArtifactRecorder for FailingArtifactRecorder {
    async fn get(&self, _id: ArtifactId) -> Result<Option<Artifact>, AdapterError> {
        Ok(None)
    }

    async fn record(&self, _artifact: &Artifact) -> Result<Artifact, AdapterError> {
        Err(AdapterError::artifact_record(
            "db unavailable",
            ErrorClass::Transient,
        ))
    }

    async fn repair_storage_key(
        &self,
        _artifact: &Artifact,
        _observed_storage_key: &str,
    ) -> Result<Artifact, AdapterError> {
        Err(AdapterError::artifact_record(
            "db unavailable",
            ErrorClass::Transient,
        ))
    }
}

fn recording_recorder() -> Arc<RecordingArtifactRecorder> {
    Arc::new(RecordingArtifactRecorder::default())
}

fn cpi_job(source_url: String) -> au_kpis_adapter::DiscoveredJob {
    let job = AbsAdapter::current_jobs(
        &AbsAdapter::parse_dataflow_listing(
            r#"{
              "data": {
                "dataflows": [{
                  "id": "CPI",
                  "agencyID": "ABS",
                  "version": "2.0.0",
                  "name": "Consumer Price Index",
                  "updated": "2026-04-28T00:00:00Z",
                  "links": [
                    { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0", "rel": "self" }
                  ]
                }]
              }
            }"#,
        )
        .expect("parse dataflow listing"),
    )
    .into_iter()
    .next()
    .expect("one CPI job");

    au_kpis_adapter::DiscoveredJob { source_url, ..job }
}

async fn serve_artifact_once() -> (String, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);

        assert!(request.starts_with(
            "GET /rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD HTTP/1.1"
        ));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-abs/")
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("accept: application/vnd.sdmx.data+json")
        );

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/vnd.sdmx.data+json\r\netag: \"fixture-etag\"\r\nlast-modified: Wed, 29 Apr 2026 00:00:00 GMT\r\nx-audit: first\r\nx-audit: second\r\ncontent-length: {}\r\n\r\n",
            SDMX_FIXTURE.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write headers");
        stream.write_all(SDMX_FIXTURE).await.expect("write body");
    });

    (
        format!("http://{addr}/rest"),
        format!("http://{addr}/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD"),
    )
}

async fn serve_throttle_once() -> (String, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await.expect("read request");
        stream
            .write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nretry-after: 17\r\nx-request-id: throttle-fixture\r\ncontent-length: 0\r\n\r\n",
            )
            .await
            .expect("write throttle response");
    });

    (
        format!("http://{addr}/rest"),
        format!("http://{addr}/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD"),
    )
}

async fn serve_redirect_once() -> (String, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await.expect("read request");
        stream
            .write_all(
                b"HTTP/1.1 302 Found\r\nlocation: http://169.254.169.254/latest/meta-data\r\ncontent-length: 0\r\n\r\n",
            )
            .await
            .expect("write redirect response");
    });

    (
        format!("http://{addr}/rest"),
        format!("http://{addr}/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD"),
    )
}

async fn serve_non_utf8_header_once() -> (String, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await.expect("read request");
        let mut response =
            b"HTTP/1.1 200 OK\r\ncontent-type: application/vnd.sdmx.data+json\r\nx-raw: ".to_vec();
        response.extend_from_slice(&[0xff, 0xfe]);
        response.extend_from_slice(b"\r\nx-text: bytes:hex:fffe");
        response.extend_from_slice(
            format!("\r\ncontent-length: {}\r\n\r\n", SDMX_FIXTURE.len()).as_bytes(),
        );
        stream
            .write_all(&response)
            .await
            .expect("write response headers");
        stream.write_all(SDMX_FIXTURE).await.expect("write body");
    });

    (
        format!("http://{addr}/rest"),
        format!("http://{addr}/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_streams_abs_sdmx_json_to_content_addressed_storage() {
    let (base_url, source_url) = serve_artifact_once().await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let job = cpi_job(source_url);
    let started_at = Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let blob_store = BlobStore::new(InMemory::new());
    let recorder = Arc::new(RecordingArtifactRecorder::default());

    let artifact = adapter
        .fetch(
            job.clone(),
            &FetchCtx::new(http, blob_store.clone(), started_at, recorder.clone()),
        )
        .await
        .expect("fetch ABS artifact");

    let expected_id = ArtifactId::of_content(SDMX_FIXTURE);
    assert_eq!(artifact.id, expected_id);
    assert_eq!(artifact.source_id.as_str(), "abs");
    assert_eq!(artifact.source_url, job.source_url);
    assert_eq!(artifact.content_type, "application/vnd.sdmx.data+json");
    assert_eq!(artifact.response_headers["etag"], ["\"fixture-etag\""]);
    assert_eq!(
        artifact.response_headers["last-modified"],
        ["Wed, 29 Apr 2026 00:00:00 GMT"]
    );
    assert_eq!(artifact.response_headers["x-audit"], ["first", "second"]);
    assert_eq!(artifact.size_bytes, SDMX_FIXTURE.len() as u64);
    assert!(
        artifact.fetched_at > started_at,
        "fetched_at should be captured after the streamed write completes"
    );
    assert_eq!(
        artifact.storage_key,
        format!("artifacts/{}", expected_id.to_hex())
    );

    let recorded = recorder.artifacts.lock().await;
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0], Artifact::from(artifact.clone()));

    let mut stored = blob_store
        .get(&StorageKey::from_persisted(&artifact.storage_key))
        .await
        .expect("read stored artifact");
    let mut bytes = Vec::new();
    while let Some(chunk) = stored.next().await {
        bytes.extend_from_slice(&chunk.expect("stored chunk"));
    }
    assert_eq!(bytes, SDMX_FIXTURE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_keeps_canonical_blob_for_retry_when_primary_record_fails() {
    let (base_url, source_url) = serve_artifact_once().await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let blob_store = BlobStore::new(InMemory::new());
    let expected_id = ArtifactId::of_content(SDMX_FIXTURE);

    let err = adapter
        .fetch(
            cpi_job(source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                blob_store.clone(),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                Arc::new(FailingArtifactRecorder),
            ),
        )
        .await
        .expect_err("provenance persistence failure should fail fetch");

    assert_eq!(err.class(), ErrorClass::Transient);
    assert!(
        blob_store
            .exists(&StorageKey::canonical_for(&expected_id))
            .await
            .expect("check blob"),
        "raw blob remains available for retry/reconciliation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_skips_hot_copy_when_durable_row_uses_rewritten_storage_key() {
    let (base_url, source_url) = serve_artifact_once().await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let expected_id = ArtifactId::of_content(SDMX_FIXTURE);
    let cold_key = format!("cold/{}", expected_id.to_hex());
    let backend = Arc::new(InMemory::new());
    backend
        .put(
            &ObjectPath::from(cold_key.clone()),
            Bytes::from_static(SDMX_FIXTURE).into(),
        )
        .await
        .expect("seed durable cold artifact");
    let blob_store = BlobStore::from_arc(backend);
    let existing = Artifact {
        id: expected_id,
        source_id: au_kpis_domain::SourceId::new("abs").unwrap(),
        source_url: source_url.clone(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::new(),
        storage_key: cold_key.clone(),
        size_bytes: SDMX_FIXTURE.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 4, 28, 0, 0, 0).unwrap(),
    };

    let artifact = adapter
        .fetch(
            cpi_job(source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                blob_store.clone(),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                Arc::new(ExistingColdArtifactRecorder { artifact: existing }),
            ),
        )
        .await
        .expect("fetch duplicate artifact with rewritten durable storage key");

    assert_eq!(artifact.storage_key, cold_key);
    assert!(
        !blob_store
            .exists(&StorageKey::canonical_for(&expected_id))
            .await
            .expect("check hot canonical copy"),
        "duplicate fetch should not create a hot canonical copy when durable row points elsewhere"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_backfills_headers_for_duplicate_durable_artifact() {
    let (base_url, source_url) = serve_artifact_once().await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let expected_id = ArtifactId::of_content(SDMX_FIXTURE);
    let storage_key = format!("artifacts/{}", expected_id.to_hex());
    let backend = Arc::new(InMemory::new());
    backend
        .put(
            &ObjectPath::from(storage_key.clone()),
            Bytes::from_static(SDMX_FIXTURE).into(),
        )
        .await
        .expect("seed durable canonical artifact");
    let blob_store = BlobStore::from_arc(backend);
    let existing = Artifact {
        id: expected_id,
        source_id: au_kpis_domain::SourceId::new("abs").unwrap(),
        source_url: source_url.clone(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::new(),
        storage_key: storage_key.clone(),
        size_bytes: SDMX_FIXTURE.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 4, 28, 0, 0, 0).unwrap(),
    };
    let recorder = Arc::new(BackfillingExistingArtifactRecorder {
        artifact: tokio::sync::Mutex::new(existing),
        record_calls: tokio::sync::Mutex::new(0),
    });

    let artifact = adapter
        .fetch(
            cpi_job(source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                blob_store.clone(),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                recorder.clone(),
            ),
        )
        .await
        .expect("fetch duplicate artifact and backfill headers");

    assert_eq!(artifact.storage_key, storage_key);
    assert_eq!(artifact.response_headers["etag"], ["\"fixture-etag\""]);
    assert_eq!(artifact.response_headers["x-audit"], ["first", "second"]);
    assert_eq!(*recorder.record_calls.lock().await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_removes_hot_copy_when_rewrite_races_after_lookup() {
    let (base_url, source_url) = serve_artifact_once().await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let expected_id = ArtifactId::of_content(SDMX_FIXTURE);
    let cold_key = format!("cold/{}", expected_id.to_hex());
    let backend = Arc::new(InMemory::new());
    backend
        .put(
            &ObjectPath::from(cold_key.clone()),
            Bytes::from_static(SDMX_FIXTURE).into(),
        )
        .await
        .expect("seed durable cold artifact");
    let blob_store = BlobStore::from_arc(backend);
    let existing = Artifact {
        id: expected_id,
        source_id: au_kpis_domain::SourceId::new("abs").unwrap(),
        source_url: source_url.clone(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::new(),
        storage_key: cold_key.clone(),
        size_bytes: SDMX_FIXTURE.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 4, 28, 0, 0, 0).unwrap(),
    };

    let artifact = adapter
        .fetch(
            cpi_job(source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                blob_store.clone(),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                Arc::new(RacingColdArtifactRecorder { artifact: existing }),
            ),
        )
        .await
        .expect("fetch handles cold rewrite race");

    assert_eq!(artifact.storage_key, cold_key);
    assert!(
        !blob_store
            .exists(&StorageKey::canonical_for(&expected_id))
            .await
            .expect("check hot canonical copy"),
        "race cleanup should remove the unreferenced canonical copy"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_repairs_rewritten_storage_key_when_durable_blob_is_missing() {
    let (base_url, source_url) = serve_artifact_once().await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let blob_store = BlobStore::new(InMemory::new());
    let expected_id = ArtifactId::of_content(SDMX_FIXTURE);
    let missing_key = format!("cold/{}", expected_id.to_hex());
    let existing = Artifact {
        id: expected_id,
        source_id: au_kpis_domain::SourceId::new("abs").unwrap(),
        source_url: source_url.clone(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::new(),
        storage_key: missing_key,
        size_bytes: SDMX_FIXTURE.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 4, 28, 0, 0, 0).unwrap(),
    };

    let artifact = adapter
        .fetch(
            cpi_job(source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                blob_store.clone(),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                Arc::new(ExistingColdArtifactRecorder { artifact: existing }),
            ),
        )
        .await
        .expect("fetch repairs missing durable storage key");

    assert_eq!(
        artifact.storage_key,
        format!("artifacts/{}", expected_id.to_hex())
    );
    assert!(
        blob_store
            .exists(&StorageKey::canonical_for(&expected_id))
            .await
            .expect("check repaired canonical copy"),
        "repair should keep the streamed canonical blob durable"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_repairs_rewritten_storage_key_when_durable_blob_hash_mismatches() {
    let (base_url, source_url) = serve_artifact_once().await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let expected_id = ArtifactId::of_content(SDMX_FIXTURE);
    let cold_key = format!("cold/{}", expected_id.to_hex());
    let backend = Arc::new(InMemory::new());
    backend
        .put(
            &ObjectPath::from(cold_key.clone()),
            Bytes::from_static(b"not the ABS artifact").into(),
        )
        .await
        .expect("seed corrupt cold artifact");
    let blob_store = BlobStore::from_arc(backend);
    let existing = Artifact {
        id: expected_id,
        source_id: au_kpis_domain::SourceId::new("abs").unwrap(),
        source_url: source_url.clone(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::new(),
        storage_key: cold_key,
        size_bytes: SDMX_FIXTURE.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 4, 28, 0, 0, 0).unwrap(),
    };

    let artifact = adapter
        .fetch(
            cpi_job(source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                blob_store.clone(),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                Arc::new(ExistingColdArtifactRecorder { artifact: existing }),
            ),
        )
        .await
        .expect("fetch repairs corrupt durable storage key");

    assert_eq!(
        artifact.storage_key,
        format!("artifacts/{}", expected_id.to_hex())
    );
    assert!(
        blob_store
            .matches_artifact_id(&StorageKey::canonical_for(&expected_id), expected_id)
            .await
            .expect("hash repaired canonical copy"),
        "repair should preserve a canonical blob matching the artifact id"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_preserves_retry_after_on_upstream_throttle() {
    let (base_url, source_url) = serve_throttle_once().await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let err = adapter
        .fetch(
            cpi_job(source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                BlobStore::new(InMemory::new()),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                recording_recorder(),
            ),
        )
        .await
        .expect_err("429 should surface structured upstream status");

    assert_eq!(err.class(), ErrorClass::Transient);
    assert_eq!(err.retry_after(), Some(Duration::from_secs(17)));
    match err {
        AdapterError::UpstreamStatus {
            status,
            response_headers,
            ..
        } => {
            assert_eq!(status.as_u16(), 429);
            assert_eq!(response_headers["retry-after"], ["17"]);
            assert_eq!(response_headers["x-request-id"], ["throttle-fixture"]);
        }
        other => panic!("expected upstream status, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rejects_upstream_redirect_without_following_location() {
    let (base_url, source_url) = serve_redirect_once().await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let err = adapter
        .fetch(
            cpi_job(source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                BlobStore::new(InMemory::new()),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                recording_recorder(),
            ),
        )
        .await
        .expect_err("redirect should be surfaced instead of followed");

    match err {
        AdapterError::UpstreamStatus {
            status,
            response_headers,
            ..
        } => {
            assert_eq!(status.as_u16(), 302);
            assert_eq!(
                response_headers["location"],
                ["http://169.254.169.254/latest/meta-data"]
            );
        }
        other => panic!("expected redirect upstream status, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_encodes_non_utf8_response_headers_losslessly() {
    let (base_url, source_url) = serve_non_utf8_header_once().await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let artifact = adapter
        .fetch(
            cpi_job(source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                BlobStore::new(InMemory::new()),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                recording_recorder(),
            ),
        )
        .await
        .expect("fetch artifact with non-UTF8 response header");

    assert_eq!(artifact.response_headers["x-raw"], ["bytes:hex:fffe"]);
    assert_eq!(artifact.response_headers["x-text"], ["text:bytes:hex:fffe"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rejects_abs_metadata_that_conflicts_with_typed_dataflow() {
    let adapter = AbsAdapter::default();
    let mut job = cpi_job(
        "https://data.api.abs.gov.au/rest/data/ABS,WPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD"
            .into(),
    );
    job.metadata.insert("abs_dataflow_id".into(), "WPI".into());

    let err = adapter
        .fetch(
            job,
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                BlobStore::new(InMemory::new()),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                recording_recorder(),
            ),
        )
        .await
        .expect_err("metadata conflict should fail before HTTP");

    assert!(err.to_string().contains("does not match dataflow"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rejects_jobs_for_other_sources() {
    let adapter = AbsAdapter::default();
    let mut job = AbsAdapter::current_jobs(
        &AbsAdapter::parse_dataflow_listing(
            r#"{"data":{"dataflows":[{"id":"CPI","agencyID":"ABS","version":"2.0.0","name":"Consumer Price Index","links":[{"href":"https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0","rel":"self"}]}]}}"#,
        )
        .expect("parse dataflow listing"),
    )
    .into_iter()
    .next()
    .expect("one CPI job");
    job.source_id = au_kpis_domain::SourceId::new("rba").unwrap();

    let err = adapter
        .fetch(
            job,
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                BlobStore::new(InMemory::new()),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                recording_recorder(),
            ),
        )
        .await
        .expect_err("source mismatch should fail");

    assert!(err.to_string().contains("source"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rejects_non_canonical_source_urls() {
    let adapter = AbsAdapter::default();
    let mut job = AbsAdapter::current_jobs(
        &AbsAdapter::parse_dataflow_listing(
            r#"{"data":{"dataflows":[{"id":"CPI","agencyID":"ABS","version":"2.0.0","name":"Consumer Price Index","links":[{"href":"https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0","rel":"self"}]}]}}"#,
        )
        .expect("parse dataflow listing"),
    )
    .into_iter()
    .next()
    .expect("one CPI job");
    job.source_url = "http://169.254.169.254/latest/meta-data".into();

    let err = adapter
        .fetch(
            job,
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                BlobStore::new(InMemory::new()),
                Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
                recording_recorder(),
            ),
        )
        .await
        .expect_err("non-canonical URL should fail before HTTP");

    assert!(err.to_string().contains("canonical URL"));
}
