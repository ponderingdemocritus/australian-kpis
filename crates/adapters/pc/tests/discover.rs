use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_pc::{PcAdapter, PcProductivityBulletin};
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
  <h1>Productivity insights</h1>
  <a href="/ongoing/productivity-insights/productivity-bulletin-2026.csv" data-updated="2026-06-03">Productivity Bulletin 2026</a>
  <a href="/ongoing/productivity-insights/productivity-bulletin-2025.csv" data-updated="2025-06-04">Productivity Bulletin 2025</a>
  <a href="/ongoing/productivity-insights/research-paper.pdf">PDF ignored</a>
</main>
"#;

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
        assert!(request.starts_with("GET /ongoing/productivity-insights/ HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-pc/")
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

    Some(format!("http://{addr}/ongoing/productivity-insights/"))
}

#[test]
fn parse_productivity_listing_discovers_csv_bulletins() {
    let bulletins = PcAdapter::parse_productivity_bulletins(INDEX_FIXTURE).expect("parse PC index");

    let snapshot = bulletins
        .iter()
        .map(|bulletin| {
            json!({
                "bulletin_id": bulletin.bulletin_id,
                "title": bulletin.title,
                "source_url": bulletin.source_url,
                "last_updated": bulletin.last_updated,
            })
        })
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn discoverable_jobs_apply_revisions_and_source_metadata() {
    let current = PcAdapter::parse_productivity_bulletins(INDEX_FIXTURE).expect("parse PC index");
    let known_revisions = BTreeMap::from([(
        "PC:productivity-bulletin-2025".to_string(),
        PcProductivityBulletin::revision_for("2025-06-04", Some("2025-06-04")),
    )]);
    let jobs = PcAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].source_id.as_str(), "pc");
    assert_eq!(jobs[0].dataflow_id.as_str(), "pc.productivity_bulletin");
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(
        jobs[0].metadata["attribution"],
        "Source: Productivity Commission"
    );
    assert_eq!(jobs[0].metadata["license"], "CC-BY-4.0");
    assert_eq!(
        jobs[0].metadata["revision_key"],
        "PC:productivity-bulletin-2026"
    );
    assert_eq!(jobs[0].metadata["revision_version"], "2026-06-03");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_scrapes_productivity_index_over_http() {
    let Some(index_url) = serve_index_once(INDEX_FIXTURE).await else {
        return;
    };
    let adapter = PcAdapter::builder().index_url(&index_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter.discover(&ctx).await.expect("discover PC bulletins");

    assert_eq!(jobs.len(), 2);
    assert_eq!(
        jobs[0].source_url,
        format!("{index_url}productivity-bulletin-2025.csv")
    );
    assert_eq!(
        jobs[0].metadata["bulletin_id"],
        "productivity-bulletin-2025"
    );
}

#[test]
fn manifest_declares_pc_rate_limit_and_dataflow_metadata() {
    let adapter = PcAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "pc");
    assert_eq!(manifest.rate_limit.max_requests, 30);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![au_kpis_domain::DataflowId::new("pc.productivity_bulletin").unwrap()]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 1);
    assert_eq!(dataflows[0].id.as_str(), "pc.productivity_bulletin");
    assert_eq!(dataflows[0].source_id.as_str(), "pc");
    assert_eq!(dataflows[0].measures.len(), 2);
    assert!(
        dataflows[0]
            .measures
            .iter()
            .any(|measure| measure.as_str() == "market_sector_growth")
    );
    assert_eq!(dataflows[0].attribution, "Source: Productivity Commission");
    assert_eq!(
        dataflows[0].source_url,
        "https://www.pc.gov.au/ongoing/productivity-insights"
    );
}
