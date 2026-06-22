use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_apra::{ApraAdapter, ApraReleaseRevision};
use chrono::{TimeZone, Utc};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

const RELEASE_FIXTURE: &str = r#"
<!doctype html>
<main>
  <h1>Quarterly authorised deposit-taking institution statistics</h1>
  <a href="/sites/default/files/2026-03/Quarterly%20authorised%20deposit-taking%20institution%20performance-September%202004%20to%20December%202025.xlsx" data-updated="2026-03-18">
    Quarterly authorised deposit-taking institution performance - September 2004 to December 2025
  </a>
  <a href="/sites/default/files/2026-03/Authorised%20deposit-taking%20institution%20centralised%20publication%20-%20March%202013%20to%20December%202025.xlsx" data-updated="2026-03-18">
    Authorised deposit-taking institution centralised publication - March 2013 to December 2025
  </a>
  <a href="/sites/default/files/2026-03/Quarterly%20authorised%20deposit-taking%20institution%20property%20exposures%20statistics%20December%202025.xlsx" data-updated="2026-03-18">
    Quarterly authorised deposit-taking institution property exposures statistics December 2025
  </a>
  <a href="/sites/default/files/2026-03/Explanatory%20notes.pdf">PDF ignored</a>
</main>
"#;

const SYSTEM_FILES_RELEASE_FIXTURE: &str = r#"
<!doctype html>
<main>
  <h1>Quarterly authorised deposit-taking institution statistics</h1>
  <a href="/system/files/2026-05/Quarterly%20authorised%20deposit-taking%20institution%20performance-September%202004%20to%20December%202025.xlsx">
    Quarterly authorised deposit-taking institution performance - September 2004 to December 2025
  </a>
  <a href="/system/files/2026-05/Authorised%20deposit-taking%20institution%20centralised%20publication%20-%20March%202013%20to%20December%202025.xlsx">
    Authorised deposit-taking institution centralised publication - March 2013 to December 2025
  </a>
  <a href="/system/files/2026-05/Quarterly%20authorised%20deposit-taking%20institution%20property%20exposures%20statistics%20December%202025.xlsx">
    Quarterly authorised deposit-taking institution property exposures statistics December 2025
  </a>
</main>
"#;

const SUPER_RELEASE_FIXTURE: &str = r#"
<!doctype html>
<main>
  <h1>Quarterly superannuation statistics</h1>
  <a href="/sites/default/files/2026-05/Quarterly%20superannuation%20performance%20statistics%20-%20December%202004%20to%20March%202026.xlsx" data-updated="2026-05-28">
    Quarterly superannuation performance statistics - December 2004 to March 2026 XLSX
  </a>
  <a href="/sites/default/files/2026-05/Quarterly%20MySuper%20statistics%20from%20September%202020%20to%20March%202026.xlsx" data-updated="2026-05-28">
    Quarterly MySuper statistics from September 2020 to March 2026 XLSX
  </a>
</main>
"#;

async fn serve_release_calendar_once(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /quarterly-adi-statistics HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-apra/")
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

    format!("http://{addr}/quarterly-adi-statistics")
}

#[test]
fn parse_release_calendar_discovers_xls_artifacts() {
    let releases =
        ApraAdapter::parse_release_calendar(RELEASE_FIXTURE).expect("parse APRA release calendar");

    let snapshot = releases
        .iter()
        .map(|release| {
            json!({
                "publication_slug": release.publication_slug,
                "title": release.title,
                "format": release.format.as_str(),
                "source_url": release.source_url,
                "last_updated": release.last_updated,
            })
        })
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn parse_release_calendar_discovers_current_system_files_xls_artifacts() {
    let releases = ApraAdapter::parse_release_calendar(SYSTEM_FILES_RELEASE_FIXTURE)
        .expect("parse APRA release calendar with current system file paths");

    assert_eq!(releases.len(), 3);
    assert_eq!(releases[0].publication_slug, "adi-centralised");
    assert_eq!(
        releases[0].source_url,
        "https://www.apra.gov.au/system/files/2026-05/Authorised%20deposit-taking%20institution%20centralised%20publication%20-%20March%202013%20to%20December%202025.xlsx"
    );
    assert_eq!(releases[1].publication_slug, "adi-performance");
    assert_eq!(releases[2].publication_slug, "adi-property-exposures");
}

#[test]
fn discoverable_jobs_apply_release_revision_and_source_metadata() {
    let current =
        ApraAdapter::parse_release_calendar(RELEASE_FIXTURE).expect("parse APRA release calendar");
    let known_revisions = BTreeMap::from([(
        "APRA:adi-performance".to_string(),
        ApraReleaseRevision::new("2026-03-18", Some("2026-03-18")),
    )]);
    let jobs = ApraAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| job.source_id.as_str() == "apra"));
    assert!(
        jobs.iter()
            .all(|job| job.dataflow_id.as_str() == "apra.quarterly_statistics")
    );
    assert!(
        jobs.iter()
            .all(|job| job.trace_parent.as_deref() == Some(TRACE_PARENT))
    );
    assert!(
        jobs.iter().all(|job| job.metadata["attribution"]
            == "Source: Australian Prudential Regulation Authority")
    );
    assert!(
        jobs.iter()
            .all(|job| job.metadata["license"]
                == "Creative Commons Attribution 3.0 Australia Licence")
    );
    assert!(
        jobs.iter()
            .all(|job| job.metadata["schema_drift_policy"] == "hash-schema-per-release")
    );
    assert_eq!(jobs[0].metadata["cadence"], "quarterly");
    assert_eq!(jobs[0].metadata["revision_key"], "APRA:adi-centralised");
    assert_eq!(
        jobs[1].metadata["revision_key"],
        "APRA:adi-property-exposures"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_scrapes_apra_release_calendar_over_http() {
    let release_url = serve_release_calendar_once(RELEASE_FIXTURE).await;
    let adapter = ApraAdapter::builder()
        .super_release_url(&release_url)
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover APRA releases");

    assert_eq!(jobs.len(), 3);
    let origin = release_url
        .trim_end_matches("/quarterly-adi-statistics")
        .to_string();
    assert_eq!(
        jobs[0].source_url,
        format!(
            "{origin}/sites/default/files/2026-03/Authorised%20deposit-taking%20institution%20centralised%20publication%20-%20March%202013%20to%20December%202025.xlsx"
        )
    );
    assert_eq!(jobs[0].metadata["publication_slug"], "adi-centralised");
    assert_eq!(jobs[0].metadata["artifact_format"], "xls");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_honours_super_asset_allocation_dataflow_scope() {
    let release_url = serve_release_calendar_once(SUPER_RELEASE_FIXTURE).await;
    let adapter = ApraAdapter::builder().release_url(&release_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 28, 0, 0, 0).unwrap())
        .with_requested_dataflow_id(
            au_kpis_domain::DataflowId::new("apra.super_asset_allocation").unwrap(),
        );

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover APRA super asset allocation release");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].dataflow_id.as_str(), "apra.super_asset_allocation");
    assert_eq!(jobs[0].metadata["publication_slug"], "super-performance");
    assert_eq!(jobs[0].metadata["cadence"], "quarterly");
}

#[test]
fn manifest_declares_apra_rate_limit_and_dataflow_metadata() {
    let adapter = ApraAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "apra");
    assert_eq!(manifest.rate_limit.max_requests, 30);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![
            au_kpis_domain::DataflowId::new("apra.quarterly_statistics").unwrap(),
            au_kpis_domain::DataflowId::new("apra.super_asset_allocation").unwrap(),
        ]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 2);
    assert_eq!(dataflows[0].id.as_str(), "apra.quarterly_statistics");
    assert_eq!(dataflows[0].frequency, au_kpis_domain::Frequency::Quarterly);
    assert_eq!(
        dataflows[0].attribution,
        "Source: Australian Prudential Regulation Authority"
    );
    assert_eq!(
        dataflows[0].source_url,
        "https://www.apra.gov.au/quarterly-authorised-deposit-taking-institution-statistics"
    );
    assert_eq!(dataflows[1].id.as_str(), "apra.super_asset_allocation");
    assert_eq!(
        dataflows[1]
            .dimensions
            .iter()
            .map(au_kpis_domain::DimensionId::as_str)
            .collect::<Vec<_>>(),
        vec!["fund_type", "asset_category", "mapping"]
    );
}
