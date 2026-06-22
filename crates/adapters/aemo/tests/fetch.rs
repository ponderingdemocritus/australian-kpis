use std::{collections::BTreeMap, io, sync::Arc, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterHttpClient, ArtifactRecorder, DiscoveredJob, FetchCtx, SourceAdapter,
};
use au_kpis_adapter_aemo::AemoAdapter;
use au_kpis_domain::{Artifact, ArtifactId, DataflowId, SourceId};
use au_kpis_storage::BlobStore;
use chrono::{TimeZone, Utc};
use object_store::memory::InMemory;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Debug, Default)]
struct RecordingArtifactRecorder;

#[async_trait]
impl ArtifactRecorder for RecordingArtifactRecorder {
    async fn get(&self, _id: ArtifactId) -> Result<Option<Artifact>, AdapterError> {
        Ok(None)
    }

    async fn record(&self, artifact: &Artifact) -> Result<Artifact, AdapterError> {
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

fn dispatch_job(source_url: String) -> DiscoveredJob {
    DiscoveredJob {
        id: "aemo:dispatch:PUBLIC_DISPATCHIS_202606191705_0000000523261987.zip".into(),
        source_id: SourceId::new("aemo").unwrap(),
        dataflow_id: DataflowId::new("aemo.dispatch").unwrap(),
        source_url,
        trace_parent: None,
        metadata: BTreeMap::new(),
    }
}

async fn serve_throttle_once() -> Option<String> {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
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
        assert!(
            request.starts_with(
                "GET /Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202606191705_0000000523261987.zip HTTP/1.1",
            ),
            "{request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-aemo/"),
            "{request}"
        );

        stream
            .write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nretry-after: 17\r\nx-request-id: aemo-throttle-fixture\r\ncontent-length: 0\r\n\r\n",
            )
            .await
            .expect("write throttle response");
    });

    Some(format!(
        "http://{addr}/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202606191705_0000000523261987.zip"
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_preserves_retry_after_on_nemweb_throttle() {
    let Some(source_url) = serve_throttle_once().await else {
        return;
    };
    let adapter = AemoAdapter::default();
    let err = adapter
        .fetch(
            dispatch_job(source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                BlobStore::new(InMemory::new()),
                Utc.with_ymd_and_hms(2026, 6, 19, 7, 5, 0).unwrap(),
                Arc::new(RecordingArtifactRecorder),
            ),
        )
        .await
        .expect_err("429 should surface structured upstream status");

    match err {
        AdapterError::UpstreamStatus {
            status,
            retry_after,
            response_headers,
        } => {
            assert_eq!(status.as_u16(), 429);
            assert_eq!(retry_after, Some(Duration::from_secs(17)));
            assert_eq!(response_headers["retry-after"], ["17"]);
            assert_eq!(response_headers["x-request-id"], ["aemo-throttle-fixture"]);
        }
        other => panic!("expected upstream status, got {other:?}"),
    }
}
