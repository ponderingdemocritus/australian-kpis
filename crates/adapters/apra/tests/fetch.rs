use std::sync::Arc;

use async_trait::async_trait;
use au_kpis_adapter::{AdapterError, AdapterHttpClient, ArtifactRecorder, FetchCtx, SourceAdapter};
use au_kpis_adapter_apra::ApraAdapter;
use au_kpis_domain::{Artifact, ArtifactId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const XLSX_FIXTURE: &[u8] = include_bytes!("fixtures/performance.xlsx");

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

async fn serve_artifact_once(body: &'static [u8]) -> (String, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /sites/default/files/2026-03/performance.xlsx HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-apra/")
        );

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\nx-apra-fixture: performance\r\ncontent-length: {}\r\n\r\n",
            body.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");
        stream.write_all(body).await.expect("write response body");
    });

    let release_url = format!("http://{addr}/quarterly-adi-statistics");
    let source_url = format!("http://{addr}/sites/default/files/2026-03/performance.xlsx");
    (release_url, source_url)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_persists_raw_apra_xls_artifact_with_response_headers() {
    let (release_url, source_url) = serve_artifact_once(XLSX_FIXTURE).await;
    let adapter = ApraAdapter::builder().release_url(&release_url).build();
    let release = ApraAdapter::parse_release_calendar(&format!(
        r#"<a href="{source_url}" data-updated="2026-03-18">Quarterly authorised deposit-taking institution performance</a>"#
    ))
    .expect("parse release link")
    .into_iter()
    .next()
    .expect("release discovered");
    let job = ApraAdapter::current_jobs_with_started_at(
        &[release],
        Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
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
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
        recorder.clone(),
    );

    let artifact = adapter
        .fetch(job.clone(), &ctx)
        .await
        .expect("fetch APRA XLS");

    assert_eq!(artifact.source_id.as_str(), "apra");
    assert_eq!(artifact.source_url, job.source_url);
    assert_eq!(
        artifact.content_type,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    assert_eq!(
        artifact.response_headers["x-apra-fixture"],
        vec!["performance"]
    );
    assert_eq!(artifact.size_bytes, XLSX_FIXTURE.len() as u64);
    assert_eq!(artifact.id, ArtifactId::of_content(XLSX_FIXTURE));
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
    assert_eq!(stored, Bytes::from_static(XLSX_FIXTURE));
    assert_eq!(recorder.artifacts.lock().await.len(), 1);
}
