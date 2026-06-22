use std::{collections::BTreeMap, io, sync::Arc, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterHttpClient, ArtifactRecorder, DiscoveredJob, FetchCtx, SourceAdapter,
};
use au_kpis_adapter_state_planning::StatePlanningAdapter;
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

fn planning_job(id: &str, dataflow_id: &str, source_url: String) -> DiscoveredJob {
    DiscoveredJob {
        id: id.into(),
        source_id: SourceId::new("state-planning").unwrap(),
        dataflow_id: DataflowId::new(dataflow_id).unwrap(),
        source_url,
        trace_parent: None,
        metadata: BTreeMap::new(),
    }
}

async fn serve_throttle_once(expected_path: &'static str) -> Option<String> {
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
            request.starts_with(&format!("GET {expected_path} HTTP/1.1")),
            "{request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-state-planning/"),
            "{request}"
        );

        stream
            .write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nretry-after: 23\r\nx-request-id: state-planning-throttle\r\ncontent-length: 0\r\n\r\n",
            )
            .await
            .expect("write throttle response");
    });

    Some(format!("http://{addr}{expected_path}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_preserves_retry_after_on_nsw_throttle() {
    assert_fetch_throttle_for(
        "state-planning:nsw-da-processing-2026",
        "state_planning.nsw_da_processing",
        "/data/nsw-da-processing-2026.csv",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_preserves_retry_after_on_vic_throttle() {
    assert_fetch_throttle_for(
        "state-planning:vic-permit-activity-2026",
        "state_planning.vic_permit_activity",
        "/data/vic-permit-activity-2026.csv",
    )
    .await;
}

async fn assert_fetch_throttle_for(id: &str, dataflow_id: &str, expected_path: &'static str) {
    let Some(source_url) = serve_throttle_once(expected_path).await else {
        return;
    };
    let adapter = StatePlanningAdapter::default();
    let err = adapter
        .fetch(
            planning_job(id, dataflow_id, source_url),
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                BlobStore::new(InMemory::new()),
                Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
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
            assert_eq!(retry_after, Some(Duration::from_secs(23)));
            assert_eq!(response_headers["retry-after"], ["23"]);
            assert_eq!(
                response_headers["x-request-id"],
                ["state-planning-throttle"]
            );
        }
        other => panic!("expected upstream status, got {other:?}"),
    }
}
