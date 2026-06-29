use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter, UpstreamRevision};
use au_kpis_adapter_ai_readiness::AiReadinessAdapter;
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
  <h1>AI readiness publications</h1>
  <a href="https://oxfordinsights.com/ai-readiness/gari-australia-2026.csv" data-dataflow="oxford.gari" data-updated="2026-06-12">
    Oxford Government AI Readiness Australia 2026
  </a>
  <a href="https://www.industry.gov.au/data/naic-ai-adoption-tracker-2026.csv" data-dataflow="naic.ai_adoption_tracker" data-updated="2026-06-10">
    National AI Centre adoption tracker 2026
  </a>
  <a href="https://www.abs.gov.au/statistics/research-and-development/abs-ai-rd-2026.csv" data-dataflow="abs.ai_rd" data-updated="2026-06-08">
    ABS AI research and development 2026
  </a>
  <a href="https://immi.homeaffairs.gov.au/reports/home-affairs-skillselect-talent-proxy-2026.csv" data-dataflow="home_affairs.skillselect_talent_proxy" data-updated="2026-06-06">
    SkillSelect AI talent proxy 2026
  </a>
  <a href="/files/readme.pdf">Unrelated PDF ignored</a>
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
        assert!(request.starts_with("GET /ai HTTP/1.1"), "{request}");
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-ai-readiness/"),
            "{request}"
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

    Some(format!("http://{addr}/ai"))
}

#[test]
fn parse_index_discovers_ai_readiness_csv_artifacts() {
    let publications =
        AiReadinessAdapter::parse_publications(INDEX_FIXTURE).expect("parse index fixture");

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
        AiReadinessAdapter::parse_publications(INDEX_FIXTURE).expect("parse index fixture");
    let known_revisions = BTreeMap::from([(
        "AI_READINESS:naic-ai-adoption-tracker-2026".to_string(),
        UpstreamRevision::new("2026-06-10", Some("2026-06-10")),
    )]);
    let jobs = AiReadinessAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 3);
    assert_eq!(jobs[0].source_id.as_str(), "ai-readiness");
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(jobs[0].metadata["artifact_format"], "csv");
    assert_eq!(jobs[0].metadata["license"], "Source publication terms");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_honours_requested_dataflow_scope() {
    let Some(index_url) = serve_index_once(INDEX_FIXTURE).await else {
        return;
    };
    let adapter = AiReadinessAdapter::builder().index_url(&index_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap())
        .with_requested_dataflow_id(au_kpis_domain::DataflowId::new("abs.ai_rd").unwrap());

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover AI readiness publications");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].dataflow_id.as_str(), "abs.ai_rd");
}

#[test]
fn manifest_declares_ai_readiness_dataflows() {
    let adapter = AiReadinessAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "ai-readiness");
    assert_eq!(manifest.rate_limit.max_requests, 30);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest
            .dataflows
            .iter()
            .map(au_kpis_domain::DataflowId::as_str)
            .collect::<Vec<_>>(),
        vec![
            "oxford.gari",
            "naic.ai_adoption_tracker",
            "abs.ai_rd",
            "home_affairs.skillselect_talent_proxy"
        ]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 4);
    assert_eq!(dataflows[0].id.as_str(), "oxford.gari");
    assert_eq!(dataflows[1].id.as_str(), "naic.ai_adoption_tracker");
    assert_eq!(
        dataflows[1].source_url,
        "https://www.ai.gov.au/news-and-insights/reports/ai-adoption-tracker"
    );
    assert_eq!(dataflows[2].id.as_str(), "abs.ai_rd");
    assert_eq!(
        dataflows[3].id.as_str(),
        "home_affairs.skillselect_talent_proxy"
    );
    assert_eq!(
        dataflows[3]
            .dimensions
            .iter()
            .map(au_kpis_domain::DimensionId::as_str)
            .collect::<Vec<_>>(),
        vec!["country", "occupation_group", "metric"]
    );
}
