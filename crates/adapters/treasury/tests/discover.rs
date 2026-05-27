use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_treasury::{TreasuryAdapter, TreasuryBudgetRevision};
use chrono::{TimeZone, Utc};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

const BUDGET_FIXTURE: &str = r#"
<!doctype html>
<html>
  <head><title>Budget Paper No. 4 | Budget 2026&ndash;27</title></head>
  <body>
    <main>
      <h1>Budget Paper No. 4</h1>
      <table>
        <tr>
          <td id="agency-2026">Agency resourcing table</td>
          <td><a href="/content/bp4/download/bp4_05_agency_resourcing_tables.pdf" data-updated="2026-05-12">PDF 1.97 MB</a></td>
        </tr>
        <tr>
          <td id="agency-2025">Agency resourcing table</td>
          <td><a href="https://archive.budget.gov.au/2025-26/bp4/download/bp4_05_agency_resourcing_tables.pdf" data-updated="2025-05-13">PDF 1.79 MB</a></td>
        </tr>
        <tr>
          <td id="agency-2024">Agency resourcing table</td>
          <td><a href="https://archive.budget.gov.au/2024-25/bp4/download/bp4_05_agency_resourcing_tables.pdf" data-updated="2024-05-14">PDF 1.37 MB</a></td>
        </tr>
        <tr>
          <td>Preliminaries</td>
          <td><a href="/content/bp4/download/bp4_01_prelims.pdf">PDF ignored</a></td>
        </tr>
        <tr>
          <td>Agency resourcing table</td>
          <td><a href="/content/bp4/download/bp4_05_agency_resourcing_tables.docx">DOCX ignored</a></td>
        </tr>
      </table>
    </main>
  </body>
</html>
"#;

async fn serve_budget_page_once(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /content/bp4/index.htm HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-treasury/")
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

    format!("http://{addr}/content/bp4/index.htm")
}

#[test]
fn parse_budget_publications_page_discovers_budget_paper_pdf_artifacts() {
    let publications = TreasuryAdapter::parse_budget_publications_page(BUDGET_FIXTURE)
        .expect("parse Treasury budget page");

    let snapshot = publications
        .iter()
        .map(|publication| {
            json!({
                "budget_year": publication.budget_year,
                "paper": publication.paper,
                "paper_slug": publication.paper_slug,
                "title": publication.title,
                "source_url": publication.source_url,
                "last_updated": publication.last_updated,
            })
        })
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn discoverable_jobs_apply_annual_revision_and_source_metadata() {
    let current = TreasuryAdapter::parse_budget_publications_page(BUDGET_FIXTURE)
        .expect("parse Treasury budget page");
    let known_revisions = BTreeMap::from([(
        "TREASURY:bp4-agency-resourcing:2025-26".to_string(),
        TreasuryBudgetRevision::new("2025-05-13", Some("2025-05-13")),
    )]);
    let jobs = TreasuryAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| job.source_id.as_str() == "treasury"));
    assert!(
        jobs.iter()
            .all(|job| job.dataflow_id.as_str() == "treasury.budget_papers")
    );
    assert!(
        jobs.iter()
            .all(|job| job.trace_parent.as_deref() == Some(TRACE_PARENT))
    );
    assert!(
        jobs.iter()
            .all(|job| job.metadata["attribution"] == "Source: Australian Government, The Treasury")
    );
    assert!(
        jobs.iter()
            .all(|job| job.metadata["license"] == "CC-BY-4.0")
    );
    assert!(
        jobs.iter()
            .all(|job| job.metadata["schema_drift_policy"] == "hash-pdf-table-candidates")
    );
    assert_eq!(jobs[0].metadata["cadence"], "annual");
    assert_eq!(
        jobs[0].metadata["revision_key"],
        "TREASURY:bp4-agency-resourcing:2024-25"
    );
    assert_eq!(
        jobs[1].metadata["revision_key"],
        "TREASURY:bp4-agency-resourcing:2026-27"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_scrapes_treasury_budget_publications_page_over_http() {
    let budget_url = serve_budget_page_once(BUDGET_FIXTURE).await;
    let adapter = TreasuryAdapter::builder().budget_url(&budget_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover Treasury PDFs");

    assert_eq!(jobs.len(), 3);
    let origin = budget_url
        .trim_end_matches("/content/bp4/index.htm")
        .to_string();
    assert_eq!(
        jobs[0].source_url,
        "https://archive.budget.gov.au/2024-25/bp4/download/bp4_05_agency_resourcing_tables.pdf"
    );
    assert_eq!(
        jobs[2].source_url,
        format!("{origin}/content/bp4/download/bp4_05_agency_resourcing_tables.pdf")
    );
    assert_eq!(jobs[2].metadata["budget_year"], "2026-27");
    assert_eq!(jobs[2].metadata["artifact_format"], "pdf");
}

#[test]
fn manifest_declares_treasury_rate_limit_and_dataflow_metadata() {
    let adapter = TreasuryAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "treasury");
    assert_eq!(manifest.rate_limit.max_requests, 30);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![au_kpis_domain::DataflowId::new("treasury.budget_papers").unwrap()]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 1);
    assert_eq!(dataflows[0].id.as_str(), "treasury.budget_papers");
    assert_eq!(dataflows[0].frequency, au_kpis_domain::Frequency::Annual);
    assert_eq!(dataflows[0].license, au_kpis_domain::License::CcBy40);
    assert_eq!(
        dataflows[0].attribution,
        "Source: Australian Government, The Treasury"
    );
    assert_eq!(
        dataflows[0].source_url,
        "https://budget.gov.au/content/bp4/index.htm"
    );
}
