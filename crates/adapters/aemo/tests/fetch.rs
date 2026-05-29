use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{AdapterError, AdapterHttpClient, ArtifactRecorder, FetchCtx, SourceAdapter};
use au_kpis_adapter_aemo::AemoAdapter;
use au_kpis_domain::{Artifact, ArtifactId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const DISPATCH_CSV: &[u8] = b"C,NEMP.WORLD,DISPATCHIS,AEMO,PUBLIC,2026/05/29,11:05:12,0000000519886550,DISPATCHIS,0000000519886549\r\n";

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

fn zip_fixture() -> Vec<u8> {
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file(
            "PUBLIC_DISPATCHIS_202605291110_0000000519886550.CSV",
            zip::write::SimpleFileOptions::default(),
        )
        .expect("start zip file");
    std::io::Write::write_all(&mut writer, DISPATCH_CSV).expect("write zip csv");
    writer.finish().expect("finish zip").into_inner()
}

async fn serve_artifact_once(status: &'static str, body: Vec<u8>) -> (String, String) {
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
                "GET /Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip HTTP/1.1"
            ),
            "{request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-aemo/")
        );

        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/zip\r\nx-aemo-fixture: dispatchis\r\ncontent-length: {}\r\n\r\n",
            body.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");
        stream.write_all(&body).await.expect("write response body");
    });

    let listing_url = format!("http://{addr}/Reports/CURRENT/DispatchIS_Reports/");
    let source_url = format!("{listing_url}PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip");
    (listing_url, source_url)
}

async fn serve_rate_limited_artifact_once() -> (String, String) {
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
                b"HTTP/1.1 429 Too Many Requests\r\nretry-after: 11\r\ncontent-length: 0\r\n\r\n",
            )
            .await
            .expect("write rate-limit response");
    });

    let listing_url = format!("http://{addr}/Reports/CURRENT/DispatchIS_Reports/");
    let source_url = format!("{listing_url}PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip");
    (listing_url, source_url)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_persists_raw_aemo_dispatch_zip_with_response_headers() {
    let zip = zip_fixture();
    let (listing_url, source_url) = serve_artifact_once("200 OK", zip.clone()).await;
    let adapter = AemoAdapter::builder()
        .dispatch_listing_url(&listing_url)
        .build();
    let file = AemoAdapter::parse_dispatch_listing(&format!(
        r#"Friday, May 29, 2026 11:06 AM        {} <A HREF="{source_url}">PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip</A>"#,
        zip.len()
    ))
    .expect("parse fixture listing")
    .into_iter()
    .next()
    .expect("dispatch file discovered");
    let job = AemoAdapter::current_jobs_with_started_at(
        &[file],
        Utc.with_ymd_and_hms(2026, 5, 29, 1, 10, 0).unwrap(),
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
        Utc.with_ymd_and_hms(2026, 5, 29, 1, 11, 0).unwrap(),
        recorder.clone(),
    );

    let artifact = adapter
        .fetch(job.clone(), &ctx)
        .await
        .expect("fetch AEMO ZIP");

    assert_eq!(artifact.source_id.as_str(), "aemo");
    assert_eq!(artifact.source_url, job.source_url);
    assert_eq!(artifact.content_type, "application/zip");
    assert_eq!(
        artifact.response_headers["x-aemo-fixture"],
        vec!["dispatchis"]
    );
    assert_eq!(artifact.size_bytes, zip.len() as u64);
    assert_eq!(artifact.id, ArtifactId::of_content(&zip));
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
    assert_eq!(stored, Bytes::from(zip));
    assert_eq!(recorder.artifacts.lock().await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_returns_retry_after_when_nemweb_rate_limits() {
    let (listing_url, source_url) = serve_rate_limited_artifact_once().await;
    let adapter = AemoAdapter::builder()
        .dispatch_listing_url(&listing_url)
        .build();
    let file = AemoAdapter::parse_dispatch_listing(&format!(
        r#"Friday, May 29, 2026 11:06 AM        1234 <A HREF="{source_url}">PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip</A>"#,
    ))
    .expect("parse fixture listing")
    .into_iter()
    .next()
    .expect("dispatch file discovered");
    let job = AemoAdapter::current_jobs_with_started_at(
        &[file],
        Utc.with_ymd_and_hms(2026, 5, 29, 1, 10, 0).unwrap(),
    )
    .into_iter()
    .next()
    .expect("job emitted");
    let ctx = FetchCtx::new(
        AdapterHttpClient::new(adapter.manifest().rate_limit),
        BlobStore::from_arc(Arc::new(InMemory::new())),
        Utc.with_ymd_and_hms(2026, 5, 29, 1, 11, 0).unwrap(),
        recording_recorder(),
    );

    let err = adapter
        .fetch(job, &ctx)
        .await
        .expect_err("rate limit should be surfaced as retryable upstream status");

    match err {
        AdapterError::UpstreamStatus {
            status,
            retry_after,
            ..
        } => {
            assert_eq!(status, reqwest::StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(retry_after, Some(Duration::from_secs(11)));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
