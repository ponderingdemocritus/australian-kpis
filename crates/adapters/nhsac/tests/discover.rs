use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_nhsac::{NhsacAdapter, NhsacHousingAccordRelease};
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
  <h1>Publications</h1>
  <a href="/publications/housing-accord-progress-2026.csv" data-updated="2026-03-25">Housing Accord progress 2026</a>
  <a href="/publications/housing-accord-progress-2025.csv" data-updated="2025-05-21">Housing Accord progress 2025</a>
  <a href="/publications/state-of-the-housing-system-2026">Report ignored</a>
</main>
"#;

const REPORTS_FIXTURE: &str = r#"
<!doctype html>
<main>
  <h1>Reports and submissions</h1>
  <a href="/reports-and-submissions/quarterly-report-march-2026" data-updated="2026-03-25">Quarterly Report &ndash; March 2026</a>
  <a href="/reports-and-submissions/other-report">Report ignored</a>
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
        assert!(request.starts_with("GET /publications HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-nhsac/")
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

    Some(format!("http://{addr}/publications"))
}

#[test]
fn parse_publications_discovers_housing_accord_csvs() {
    let releases =
        NhsacAdapter::parse_housing_accord_releases(INDEX_FIXTURE).expect("parse NHSAC index");

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
        NhsacAdapter::parse_housing_accord_releases(INDEX_FIXTURE).expect("parse NHSAC index");
    let known_revisions = BTreeMap::from([(
        "NHSAC:housing-accord-progress-2025".to_string(),
        NhsacHousingAccordRelease::revision_for("2025-05-21", Some("2025-05-21")),
    )]);
    let jobs = NhsacAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].source_id.as_str(), "nhsac");
    assert_eq!(
        jobs[0].dataflow_id.as_str(),
        "nhsac.housing_accord_progress"
    );
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(
        jobs[0].metadata["attribution"],
        "Source: National Housing Supply and Affordability Council"
    );
    assert_eq!(jobs[0].metadata["license"], "NHSAC copyright");
    assert_eq!(
        jobs[0].metadata["revision_key"],
        "NHSAC:housing-accord-progress-2026"
    );
    assert_eq!(jobs[0].metadata["revision_version"], "2026-03-25");
}

#[test]
fn parse_reports_discovers_quarterly_housing_accord_html() {
    let releases =
        NhsacAdapter::parse_housing_accord_releases(REPORTS_FIXTURE).expect("parse NHSAC reports");

    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0].release_id, "quarterly-report-march-2026");
    assert_eq!(releases[0].title, "Quarterly Report - March 2026");
    assert_eq!(
        releases[0].source_url,
        "https://nhsac.gov.au/reports-and-submissions/quarterly-report-march-2026"
    );

    let jobs = NhsacAdapter::current_jobs_with_started_at(
        &releases,
        Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].metadata["artifact_format"], "html");
    assert_eq!(
        jobs[0].metadata["revision_key"],
        "NHSAC:quarterly-report-march-2026"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_scrapes_publications_over_http() {
    let Some(index_url) = serve_index_once(INDEX_FIXTURE).await else {
        return;
    };
    let adapter = NhsacAdapter::builder().index_url(&index_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover NHSAC releases");

    assert_eq!(jobs.len(), 2);
    assert_eq!(
        jobs[0].source_url,
        format!("{index_url}/housing-accord-progress-2025.csv")
    );
    assert_eq!(
        jobs[0].metadata["release_id"],
        "housing-accord-progress-2025"
    );
}

#[test]
fn manifest_declares_nhsac_rate_limit_and_dataflow_metadata() {
    let adapter = NhsacAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "nhsac");
    assert_eq!(manifest.rate_limit.max_requests, 30);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![au_kpis_domain::DataflowId::new("nhsac.housing_accord_progress").unwrap()]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 1);
    assert_eq!(dataflows[0].id.as_str(), "nhsac.housing_accord_progress");
    assert_eq!(dataflows[0].source_id.as_str(), "nhsac");
    assert_eq!(dataflows[0].measures.len(), 3);
    assert!(
        dataflows[0]
            .measures
            .iter()
            .any(|measure| measure.as_str() == "progress_to_target_pct")
    );
    assert_eq!(
        dataflows[0].attribution,
        "Source: National Housing Supply and Affordability Council"
    );
    assert_eq!(dataflows[0].source_url, "https://nhsac.gov.au/publications");
}
