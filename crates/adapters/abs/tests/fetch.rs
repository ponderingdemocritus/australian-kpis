use au_kpis_adapter::{AdapterHttpClient, FetchCtx, SourceAdapter};
use au_kpis_adapter_abs::AbsAdapter;
use au_kpis_domain::ArtifactId;
use au_kpis_storage::{BlobStore, StorageKey};
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const SDMX_FIXTURE: &[u8] = br#"{"data":{"dataSets":[{"observations":{"0:0":[123.4]}}]}}"#;

async fn serve_artifact_once() -> String {
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
            "HTTP/1.1 200 OK\r\ncontent-type: application/vnd.sdmx.data+json\r\netag: \"fixture-etag\"\r\nlast-modified: Wed, 29 Apr 2026 00:00:00 GMT\r\ncontent-length: {}\r\n\r\n",
            SDMX_FIXTURE.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write headers");
        stream.write_all(SDMX_FIXTURE).await.expect("write body");
    });

    format!("http://{addr}/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_streams_abs_sdmx_json_to_content_addressed_storage() {
    let source_url = serve_artifact_once().await;
    let adapter = AbsAdapter::default();
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
    let job = au_kpis_adapter::DiscoveredJob { source_url, ..job };
    let started_at = Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let blob_store = BlobStore::new(InMemory::new());

    let artifact = adapter
        .fetch(
            job.clone(),
            &FetchCtx::new(http, blob_store.clone(), started_at),
        )
        .await
        .expect("fetch ABS artifact");

    let expected_id = ArtifactId::of_content(SDMX_FIXTURE);
    assert_eq!(artifact.id, expected_id);
    assert_eq!(artifact.source_id.as_str(), "abs");
    assert_eq!(artifact.source_url, job.source_url);
    assert_eq!(artifact.content_type, "application/vnd.sdmx.data+json");
    assert_eq!(artifact.response_headers["etag"], "\"fixture-etag\"");
    assert_eq!(
        artifact.response_headers["last-modified"],
        "Wed, 29 Apr 2026 00:00:00 GMT"
    );
    assert_eq!(artifact.size_bytes, SDMX_FIXTURE.len() as u64);
    assert_eq!(artifact.fetched_at, started_at);
    assert_eq!(
        artifact.storage_key,
        format!("artifacts/{}", expected_id.to_hex())
    );

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
            ),
        )
        .await
        .expect_err("source mismatch should fail");

    assert!(err.to_string().contains("source"));
}
