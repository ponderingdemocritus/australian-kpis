use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_state_capital::{StateCapitalAdapter, StateCapitalPublication};
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
  <h1>State capital performance publications</h1>
  <a href="/fixtures/vago-major-projects-2026.json" data-dataflow="state_capital.vic_major_projects" data-updated="2026-06-12">
    VAGO major projects performance 2026
  </a>
  <a href="/fixtures/vic-budget-capital-2026.json" data-dataflow="state_capital.budget_capital_papers" data-updated="2026-05-05">
    Victorian budget capital program 2026
  </a>
  <a href="/fixtures/unrelated.pdf">Unrelated PDF ignored</a>
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
        assert!(request.starts_with("GET /capital HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-state-capital/")
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

    Some(format!("http://{addr}/capital"))
}

#[test]
fn parse_index_discovers_state_capital_sidecar_artifacts() {
    let publications =
        StateCapitalAdapter::parse_publications(INDEX_FIXTURE).expect("parse index fixture");

    let snapshot = publications
        .iter()
        .map(|publication| {
            json!({
                "publication_id": publication.publication_id,
                "dataflow_id": publication.dataflow_id.as_str(),
                "title": publication.title,
                "source_url": publication.source_url,
                "last_updated": publication.last_updated,
            })
        })
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn discoverable_jobs_apply_revisions_and_source_metadata() {
    let current =
        StateCapitalAdapter::parse_publications(INDEX_FIXTURE).expect("parse index fixture");
    let known_revisions = BTreeMap::from([(
        "STATE_CAPITAL:vic-budget-capital-2026".to_string(),
        StateCapitalPublication::revision_for("2026-05-05", Some("2026-05-05")),
    )]);
    let jobs = StateCapitalAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].source_id.as_str(), "state_capital");
    assert_eq!(
        jobs[0].dataflow_id.as_str(),
        "state_capital.vic_major_projects"
    );
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(jobs[0].metadata["jurisdiction"], "VIC");
    assert_eq!(
        jobs[0].metadata["schema_drift_policy"],
        "validate-pdf-sidecar-json"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_honours_requested_dataflow_scope() {
    let Some(index_url) = serve_index_once(INDEX_FIXTURE).await else {
        return;
    };
    let adapter = StateCapitalAdapter::builder().index_url(&index_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap())
        .with_requested_dataflow_id(
            au_kpis_domain::DataflowId::new("state_capital.budget_capital_papers").unwrap(),
        );

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover state capital publications");

    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0].dataflow_id.as_str(),
        "state_capital.budget_capital_papers"
    );
}

#[test]
fn manifest_declares_state_capital_dataflows() {
    let adapter = StateCapitalAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "state_capital");
    assert_eq!(manifest.rate_limit.max_requests, 30);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest
            .dataflows
            .iter()
            .map(au_kpis_domain::DataflowId::as_str)
            .collect::<Vec<_>>(),
        vec![
            "state_capital.vic_major_projects",
            "state_capital.budget_capital_papers"
        ]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 2);
    assert_eq!(dataflows[0].id.as_str(), "state_capital.vic_major_projects");
    assert_eq!(
        dataflows[0]
            .dimensions
            .iter()
            .map(au_kpis_domain::DimensionId::as_str)
            .collect::<Vec<_>>(),
        vec!["jurisdiction", "project", "category", "metric"]
    );
    assert_eq!(
        dataflows[1].id.as_str(),
        "state_capital.budget_capital_papers"
    );
    assert_eq!(
        dataflows[1]
            .dimensions
            .iter()
            .map(au_kpis_domain::DimensionId::as_str)
            .collect::<Vec<_>>(),
        vec!["jurisdiction", "category", "metric"]
    );
}
