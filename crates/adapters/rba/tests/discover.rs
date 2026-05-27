use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_rba::{RbaAdapter, RbaTableRevision};
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
  <h1>Statistical Tables</h1>
  <p>These tables are subject to revisions and may be withdrawn.</p>
  <h2>Interest Rates</h2>
  <a href="/statistics/tables/csv/f01d.csv" data-updated="2026-05-20">F1 - Data</a>
  <a href="/statistics/tables/xls/f02d.xls" data-updated="2026-05-19">F2 - Data</a>
  <h2>Inflation and Inflation Expectations</h2>
  <a href="https://www.rba.gov.au/statistics/tables/csv/g01.csv">G1 - Data</a>
  <a href="/statistics/tables/pdf/not-data.pdf">PDF ignored</a>
  <a href="/chart-pack/data.csv">Chart pack ignored</a>
</main>
"#;

async fn serve_index_once(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /statistics/tables/ HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-rba/")
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

    format!("http://{addr}/statistics/tables/")
}

#[test]
fn parse_statistical_tables_index_discovers_tabular_artifacts() {
    let tables =
        RbaAdapter::parse_statistical_tables_index(INDEX_FIXTURE).expect("parse RBA index");

    let snapshot = tables
        .iter()
        .map(|table| {
            json!({
                "table_id": table.table_id,
                "title": table.title,
                "format": table.format.as_str(),
                "source_url": table.source_url,
                "last_updated": table.last_updated,
            })
        })
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn discoverable_jobs_apply_weekly_revision_and_source_metadata() {
    let current =
        RbaAdapter::parse_statistical_tables_index(INDEX_FIXTURE).expect("parse RBA index");
    let known_revisions = BTreeMap::from([(
        "RBA:F2:xls".to_string(),
        RbaTableRevision::new("2026-05-19", Some("2026-05-19")),
    )]);
    let jobs = RbaAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| job.source_id.as_str() == "rba"));
    assert!(
        jobs.iter()
            .all(|job| job.dataflow_id.as_str() == "rba.statistical_tables")
    );
    assert!(
        jobs.iter()
            .all(|job| job.trace_parent.as_deref() == Some(TRACE_PARENT))
    );
    assert!(
        jobs.iter()
            .all(|job| job.metadata["attribution"] == "Source: Reserve Bank of Australia")
    );
    assert!(
        jobs.iter()
            .all(|job| job.metadata["license"] == "RBA Copyright and Disclaimer Notice")
    );
    assert_eq!(jobs[0].metadata["cadence"], "weekly");
    assert_eq!(jobs[0].metadata["revision_key"], "RBA:F1:csv");
    assert_eq!(jobs[1].metadata["revision_key"], "RBA:G1:csv");
}

#[test]
fn current_jobs_use_started_iso_week_when_index_has_no_update_stamp() {
    let current =
        RbaAdapter::parse_statistical_tables_index(INDEX_FIXTURE).expect("parse RBA index");
    let weekly_job = current
        .iter()
        .find(|table| table.table_id == "G1")
        .expect("G1 discovered");
    let jobs = RbaAdapter::discoverable_jobs_with_started_at(
        std::slice::from_ref(weekly_job),
        &BTreeMap::new(),
        Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
        None,
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].metadata["revision_version"], "2026-W22");
    assert_eq!(jobs[0].id, "rba:G1:csv:2026-W22");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_scrapes_rba_statistical_tables_index_over_http() {
    let index_url = serve_index_once(INDEX_FIXTURE).await;
    let adapter = RbaAdapter::builder().index_url(&index_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter.discover(&ctx).await.expect("discover RBA tables");

    assert_eq!(jobs.len(), 3);
    assert_eq!(jobs[0].source_url, format!("{index_url}csv/f01d.csv"));
    assert_eq!(jobs[0].metadata["table_id"], "F1");
    assert_eq!(jobs[0].metadata["artifact_format"], "csv");
}

#[test]
fn manifest_declares_weekly_rba_rate_limit_and_dataflow_metadata() {
    let adapter = RbaAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "rba");
    assert_eq!(manifest.rate_limit.max_requests, 60);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![au_kpis_domain::DataflowId::new("rba.statistical_tables").unwrap()]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 1);
    assert_eq!(dataflows[0].id.as_str(), "rba.statistical_tables");
    assert_eq!(
        dataflows[0].attribution,
        "Source: Reserve Bank of Australia"
    );
    assert_eq!(
        dataflows[0].source_url,
        "https://www.rba.gov.au/statistics/tables/"
    );
}
