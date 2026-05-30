use std::sync::Arc;

use async_trait::async_trait;
use au_kpis_adapter::{AdapterError, AdapterHttpClient, ArtifactRecorder, FetchCtx, SourceAdapter};
use au_kpis_adapter_asx::{AsxAdapter, AsxEodFile};
use au_kpis_domain::{Artifact, ArtifactId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{NaiveDate, TimeZone, Utc};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const EOD_CSV: &[u8] = b"ticker,date,open,high,low,close,volume,company_name\nBHP,2026-05-29,42.10,42.80,41.95,42.55,18234567,BHP Group Limited\n";

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

async fn serve_artifact_once(body: &'static [u8]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /asx/eod/latest.csv HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-asx/")
        );

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/csv\r\nx-asx-fixture: eod\r\ncontent-length: {}\r\n\r\n",
            body.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");
        stream.write_all(body).await.expect("write response body");
    });

    format!("http://{addr}/asx/eod/latest.csv")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_persists_raw_asx_eod_csv_artifact_with_response_headers() {
    let eod_url = serve_artifact_once(EOD_CSV).await;
    let adapter = AsxAdapter::builder().eod_file_url(&eod_url).build();
    let eod_file = AsxEodFile::new(
        &eod_url,
        NaiveDate::from_ymd_opt(2026, 5, 29).unwrap(),
        Utc.with_ymd_and_hms(2026, 5, 29, 8, 0, 0).unwrap(),
    );
    let job = AsxAdapter::current_jobs_with_started_at(
        &[],
        &[eod_file],
        Utc.with_ymd_and_hms(2026, 5, 29, 8, 5, 0).unwrap(),
    )
    .into_iter()
    .next()
    .expect("EOD job emitted");
    let recorder = Arc::new(RecordingArtifactRecorder::default());
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let blob_store = BlobStore::from_arc(object_store.clone());
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = FetchCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 29, 8, 6, 0).unwrap(),
        recorder.clone(),
    );

    let artifact = adapter
        .fetch(job.clone(), &ctx)
        .await
        .expect("fetch EOD CSV");

    assert_eq!(artifact.source_id.as_str(), "asx");
    assert_eq!(artifact.source_url, job.source_url);
    assert_eq!(artifact.content_type, "text/csv");
    assert_eq!(artifact.response_headers["x-asx-fixture"], vec!["eod"]);
    assert_eq!(artifact.size_bytes, EOD_CSV.len() as u64);
    assert_eq!(artifact.id, ArtifactId::of_content(EOD_CSV));
    assert_eq!(
        artifact.storage_key,
        StorageKey::canonical_for(&artifact.id).to_string()
    );

    let stored = object_store
        .get(&ObjectPath::from(artifact.storage_key.clone()))
        .await
        .expect("stored artifact")
        .bytes()
        .await
        .expect("artifact bytes");
    assert_eq!(stored, Bytes::from_static(EOD_CSV));
    assert_eq!(recorder.artifacts.lock().await.len(), 1);
}
