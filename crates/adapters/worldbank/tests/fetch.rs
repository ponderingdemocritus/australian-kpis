use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterHttpClient, ArtifactRecorder, DiscoveredJob, FetchCtx, SourceAdapter,
};
use au_kpis_adapter_worldbank::WorldbankAdapter;
use au_kpis_domain::{Artifact, ArtifactId, DataflowId, SourceId};
use au_kpis_storage::BlobStore;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const CSV_FIXTURE: &[u8] = include_bytes!("fixtures/bready_australia.csv");
const API_FIXTURE: &[u8] = include_bytes!("fixtures/bready_australia_api.json");

#[derive(Debug, Default)]
struct RecordingArtifactRecorder {
    artifacts: tokio::sync::Mutex<Vec<Artifact>>,
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
        Ok(artifact.clone())
    }
}

fn recording_recorder() -> Arc<RecordingArtifactRecorder> {
    Arc::new(RecordingArtifactRecorder::default())
}

async fn serve_artifact_once(body: &'static [u8]) -> Option<(String, String)> {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping local HTTP fixture: loopback bind denied by sandbox");
            return None;
        }
        Err(err) => panic!("bind fixture server: {err}"),
    };
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /en/businessready/bready-australia-2026.csv HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-worldbank/")
        );

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/csv\r\nx-worldbank-fixture: bready\r\ncontent-length: {}\r\n\r\n",
            body.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");
        stream.write_all(body).await.expect("write response body");
    });

    let base_url = format!("http://{addr}/en/businessready");
    let source_url = format!("{base_url}/bready-australia-2026.csv");
    Some((base_url, source_url))
}

async fn serve_api_artifact_once(body: &'static [u8]) -> Option<String> {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping local HTTP fixture: loopback bind denied by sandbox");
            return None;
        }
        Err(err) => panic!("bind fixture server: {err}"),
    };
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /v2/country/AUS/indicator/IC.BRE.BE.OS?"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-worldbank/")
        );

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-worldbank-fixture: bready-api\r\ncontent-length: {}\r\n\r\n",
            body.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");
        stream.write_all(body).await.expect("write response body");
    });

    Some(format!(
        "http://{addr}/v2/country/AUS/indicator/IC.BRE.BE.OS?format=json&source=2&per_page=100"
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_persists_raw_bready_artifact() {
    let Some((index_url, source_url)) = serve_artifact_once(CSV_FIXTURE).await else {
        return;
    };
    let adapter = WorldbankAdapter::builder().index_url(index_url).build();
    let release = WorldbankAdapter::parse_bready_releases(&format!(
        r#"<a href="{source_url}" data-updated="2026-05-01">B-READY Australia 2026</a>"#
    ))
    .expect("parse release link")
    .into_iter()
    .next()
    .expect("release discovered");
    let job = WorldbankAdapter::current_jobs_with_started_at(
        &[release],
        Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
    )
    .into_iter()
    .next()
    .expect("job emitted");
    let recorder = recording_recorder();
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let blob_store = BlobStore::from_arc(object_store.clone());
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = FetchCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
        recorder.clone(),
    );

    let artifact = adapter
        .fetch(job.clone(), &ctx)
        .await
        .expect("fetch B-READY release");

    assert_eq!(artifact.source_id.as_str(), "worldbank");
    assert_eq!(artifact.source_url, job.source_url);
    assert_eq!(artifact.content_type, "text/csv");
    assert_eq!(
        artifact.response_headers["x-worldbank-fixture"],
        vec!["bready"]
    );
    assert_eq!(artifact.size_bytes, CSV_FIXTURE.len() as u64);
    assert_eq!(artifact.id, ArtifactId::of_content(CSV_FIXTURE));

    let stored = object_store
        .get(&ObjectPath::from(artifact.storage_key.clone()))
        .await
        .expect("stored artifact")
        .bytes()
        .await
        .expect("artifact bytes");
    assert_eq!(stored, Bytes::from_static(CSV_FIXTURE));
    assert_eq!(recorder.artifacts.lock().await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_transforms_world_bank_indicator_api_into_bready_csv() {
    let Some(source_url) = serve_api_artifact_once(API_FIXTURE).await else {
        return;
    };
    let adapter = WorldbankAdapter::default();
    let job = DiscoveredJob {
        id: "worldbank:bready-australia-api:2026-04-08".into(),
        source_id: SourceId::new("worldbank").unwrap(),
        dataflow_id: DataflowId::new("worldbank.bready").unwrap(),
        source_url: source_url.clone(),
        trace_parent: None,
        metadata: BTreeMap::from([
            ("adapter".into(), "worldbank".into()),
            ("artifact_format".into(), "worldbank-json".into()),
            ("release_id".into(), "bready-australia-api".into()),
            (
                "revision_key".into(),
                "WORLDBANK:bready-australia-api".into(),
            ),
            ("revision_version".into(), "2026-04-08".into()),
        ]),
    };
    let recorder = recording_recorder();
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let blob_store = BlobStore::from_arc(object_store.clone());
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = FetchCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
        recorder.clone(),
    );

    let artifact = adapter
        .fetch(job.clone(), &ctx)
        .await
        .expect("fetch B-READY API release");

    assert_eq!(artifact.source_id.as_str(), "worldbank");
    assert_eq!(artifact.source_url, source_url);
    assert_eq!(artifact.content_type, "text/csv");
    assert_eq!(
        artifact.response_headers["x-worldbank-fixture"],
        vec!["bready-api"]
    );

    let stored = object_store
        .get(&ObjectPath::from(artifact.storage_key.clone()))
        .await
        .expect("stored artifact")
        .bytes()
        .await
        .expect("artifact bytes");
    let csv = String::from_utf8(stored.to_vec()).expect("csv utf8");
    assert!(csv.starts_with("period,country,measure_id,measure_name,value,unit,status\n"));
    assert!(csv.contains(
        "2025,AUS,business_entry_score,B-READY: Business Entry: Overall Score,,index,normal\n"
    ));
    assert_eq!(artifact.id, ArtifactId::of_content(csv.as_bytes()));
    assert_eq!(recorder.artifacts.lock().await.len(), 1);
}
