use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_worldbank::{WorldbankAdapter, WorldbankBreadyRelease};
use chrono::{TimeZone, Utc};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

const INDEX_FIXTURE: &str = r#"
<!doctype html>
<main>
  <h1>Business Ready</h1>
  <a href="/en/businessready/bready-australia-2026.csv" data-updated="2026-05-01">B-READY Australia 2026</a>
  <a href="/en/businessready/bready-australia-2025.csv" data-updated="2025-05-01">B-READY Australia 2025</a>
  <a href="/en/businessready/methodology.pdf">PDF ignored</a>
</main>
"#;

const API_FIXTURE: &str = include_str!("fixtures/bready_australia_api.json");

async fn serve_index_once(body: &'static str) -> Option<String> {
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
        assert!(request.starts_with("GET /en/businessready HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-worldbank/")
        );

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/html\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });

    Some(format!("http://{addr}/en/businessready"))
}

async fn serve_api_once(body: &'static str) -> Option<String> {
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
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });

    Some(format!(
        "http://{addr}/v2/country/AUS/indicator/IC.BRE.BE.OS?format=json&source=2&per_page=100"
    ))
}

#[test]
fn parse_business_ready_page_discovers_australia_csvs() {
    let releases =
        WorldbankAdapter::parse_bready_releases(INDEX_FIXTURE).expect("parse B-READY index");

    let snapshot = releases
        .iter()
        .map(|release| {
            json!({
                "release_id": release.release_id,
                "title": release.title,
                "source_url": release.source_url,
                "last_updated": release.last_updated,
            })
        })
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn discoverable_jobs_apply_revisions_and_source_metadata() {
    let current =
        WorldbankAdapter::parse_bready_releases(INDEX_FIXTURE).expect("parse B-READY index");
    let known_revisions = BTreeMap::from([(
        "WORLDBANK:bready-australia-2025".to_string(),
        WorldbankBreadyRelease::revision_for("2025-05-01", Some("2025-05-01")),
    )]);
    let jobs = WorldbankAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].source_id.as_str(), "worldbank");
    assert_eq!(jobs[0].dataflow_id.as_str(), "worldbank.bready");
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(
        jobs[0].metadata["attribution"],
        "Source: World Bank B-READY"
    );
    assert_eq!(jobs[0].metadata["license"], "World Bank terms");
    assert_eq!(
        jobs[0].metadata["revision_key"],
        "WORLDBANK:bready-australia-2026"
    );
    assert_eq!(jobs[0].metadata["revision_version"], "2026-05-01");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_scrapes_business_ready_page_over_http() {
    let Some(index_url) = serve_index_once(INDEX_FIXTURE).await else {
        return;
    };
    let adapter = WorldbankAdapter::builder().index_url(&index_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover B-READY releases");

    assert_eq!(jobs.len(), 2);
    assert_eq!(
        jobs[0].source_url,
        format!("{index_url}/bready-australia-2025.csv")
    );
    assert_eq!(jobs[0].metadata["release_id"], "bready-australia-2025");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_uses_world_bank_indicator_api() {
    let Some(api_url) = serve_api_once(API_FIXTURE).await else {
        return;
    };
    let adapter = WorldbankAdapter::builder().api_url(&api_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter.discover(&ctx).await.expect("discover B-READY API");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].source_id.as_str(), "worldbank");
    assert_eq!(jobs[0].dataflow_id.as_str(), "worldbank.bready");
    assert_eq!(jobs[0].source_url, api_url);
    assert_eq!(jobs[0].metadata["artifact_format"], "worldbank-json");
    assert_eq!(jobs[0].metadata["release_id"], "bready-australia-api");
    assert_eq!(
        jobs[0].metadata["revision_key"],
        "WORLDBANK:bready-australia-api"
    );
    assert_eq!(jobs[0].metadata["revision_version"], "2026-04-08");
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
}

#[test]
fn manifest_declares_worldbank_rate_limit_and_dataflow_metadata() {
    let adapter = WorldbankAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "worldbank");
    assert_eq!(manifest.rate_limit.max_requests, 30);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![au_kpis_domain::DataflowId::new("worldbank.bready").unwrap()]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 1);
    assert_eq!(dataflows[0].id.as_str(), "worldbank.bready");
    assert_eq!(dataflows[0].source_id.as_str(), "worldbank");
    assert!(
        dataflows[0]
            .measures
            .iter()
            .any(|measure| measure.as_str() == "business_entry_score")
    );
    assert_eq!(dataflows[0].attribution, "Source: World Bank B-READY");
    assert_eq!(
        dataflows[0].source_url,
        "https://www.worldbank.org/en/businessready"
    );
}
