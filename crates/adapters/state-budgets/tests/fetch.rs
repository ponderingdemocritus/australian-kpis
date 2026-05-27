use std::sync::Arc;

use async_trait::async_trait;
use au_kpis_adapter::{AdapterError, AdapterHttpClient, ArtifactRecorder, FetchCtx, SourceAdapter};
use au_kpis_adapter_state_budgets::{NswBudgetAdapter, NswBudgetPublication};
use au_kpis_domain::{Artifact, ArtifactId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const PDF_FIXTURE: &[u8] = b"%PDF-1.7\n% nsw budget fixture\n%%EOF\n";

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
        assert!(
            request.starts_with(
                "GET /sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf HTTP/1.1"
            ),
            "{request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-state-budgets/")
        );

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/pdf\r\nx-nsw-budget-fixture: bp1\r\ncontent-length: {}\r\n\r\n",
            body.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");
        stream.write_all(body).await.expect("write response body");
    });

    format!("http://{addr}/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_persists_raw_nsw_budget_pdf_with_response_headers() {
    let source_url = serve_artifact_once(PDF_FIXTURE).await;
    let publication = NswBudgetPublication {
        budget_year: "2025-26".into(),
        paper: "Budget Paper No. 1".into(),
        paper_slug: "bp1-budget-statement".into(),
        title: "Budget Statement".into(),
        source_url,
        last_updated: Some("2025-06-24".into()),
    };
    let adapter = NswBudgetAdapter::builder()
        .publications(vec![publication.clone()])
        .build();
    let job = NswBudgetAdapter::current_jobs_with_started_at(
        &[publication],
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
        .expect("fetch NSW PDF");

    assert_eq!(artifact.source_id.as_str(), "state-budgets");
    assert_eq!(artifact.source_url, job.source_url);
    assert_eq!(artifact.content_type, "application/pdf");
    assert_eq!(
        artifact.response_headers["x-nsw-budget-fixture"],
        vec!["bp1"]
    );
    assert_eq!(artifact.size_bytes, PDF_FIXTURE.len() as u64);
    assert_eq!(artifact.id, ArtifactId::of_content(PDF_FIXTURE));
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
    assert_eq!(stored, Bytes::from_static(PDF_FIXTURE));
    assert_eq!(recorder.artifacts.lock().await.len(), 1);
}
