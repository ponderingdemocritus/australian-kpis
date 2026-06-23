use std::sync::Arc;

use async_trait::async_trait;
use au_kpis_adapter::{AdapterError, AdapterHttpClient, ArtifactRecorder, FetchCtx, SourceAdapter};
use au_kpis_adapter_pc::PcAdapter;
use au_kpis_domain::{Artifact, ArtifactId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const CSV_FIXTURE: &[u8] = include_bytes!("fixtures/productivity_bulletin.csv");

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
        assert!(request.starts_with(
            "GET /ongoing/productivity-insights/productivity-bulletin-2026.csv HTTP/1.1"
        ));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-pc/")
        );

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/csv\r\nx-pc-fixture: productivity\r\ncontent-length: {}\r\n\r\n",
            body.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");
        stream.write_all(body).await.expect("write response body");
    });

    let base_url = format!("http://{addr}/ongoing/productivity-insights/");
    let source_url = format!("{base_url}productivity-bulletin-2026.csv");
    Some((base_url, source_url))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_persists_raw_productivity_bulletin_artifact() {
    let Some((index_url, source_url)) = serve_artifact_once(CSV_FIXTURE).await else {
        return;
    };
    let adapter = PcAdapter::builder().index_url(index_url).build();
    let bulletin = PcAdapter::parse_productivity_bulletins(&format!(
        r#"<a href="{source_url}" data-updated="2026-06-03">Productivity Bulletin 2026</a>"#
    ))
    .expect("parse bulletin link")
    .into_iter()
    .next()
    .expect("bulletin discovered");
    let job = PcAdapter::current_jobs_with_started_at(
        &[bulletin],
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
        .expect("fetch PC bulletin");

    assert_eq!(artifact.source_id.as_str(), "pc");
    assert_eq!(artifact.source_url, job.source_url);
    assert_eq!(artifact.content_type, "text/csv");
    assert_eq!(
        artifact.response_headers["x-pc-fixture"],
        vec!["productivity"]
    );
    assert_eq!(artifact.size_bytes, CSV_FIXTURE.len() as u64);
    assert_eq!(artifact.id, ArtifactId::of_content(CSV_FIXTURE));
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
    assert_eq!(stored, Bytes::from_static(CSV_FIXTURE));
    assert_eq!(recorder.artifacts.lock().await.len(), 1);
}
