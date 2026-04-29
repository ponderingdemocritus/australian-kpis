use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_abs::{AbsAdapter, DataflowRevision};
use chrono::{TimeZone, Utc};
use proptest::prelude::*;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const DATAFLOW_FIXTURE: &str = r#"{
  "data": {
    "dataflows": [
      {
        "id": "CPI",
        "agencyID": "ABS",
        "version": "2.0.0",
        "name": "Consumer Price Index (CPI)",
        "updated": "2026-04-28T00:00:00Z",
        "links": [
          { "href": "https://example.test/metadata/CPI", "rel": "alternate" },
          { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0", "rel": "external" }
        ]
      },
      {
        "id": "WPI",
        "agencyID": "ABS",
        "version": "1.0.0",
        "name": "Wage Price Index",
        "updated": "2026-04-20T00:00:00Z",
        "links": [
          { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/WPI/1.0.0", "rel": "external" }
        ]
      }
    ]
  }
}"#;

async fn serve_once(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /rest/dataflow/ABS/CPI?detail=allstubs HTTP/1.1"));
        assert!(request.contains("application/vnd.sdmx.structure+json"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-abs/")
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

    format!("http://{addr}/rest")
}

#[test]
fn diff_cpi_dataflow_emits_only_new_or_changed_releases() {
    let current = AbsAdapter::parse_dataflow_listing(DATAFLOW_FIXTURE).expect("parse fixture");
    let unchanged = BTreeMap::from([(
        "CPI".to_string(),
        DataflowRevision::new("2.0.0", Some("2026-04-28T00:00:00Z")),
    )]);
    let changed = BTreeMap::from([(
        "CPI".to_string(),
        DataflowRevision::new("1.0.0", Some("2025-01-01T00:00:00Z")),
    )]);

    assert!(AbsAdapter::discoverable_jobs(&current, &unchanged).is_empty());

    let jobs = AbsAdapter::discoverable_jobs(&current, &changed);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, "abs:CPI:2.0.0:2026-04-28T00-00-00Z");
    assert_eq!(jobs[0].source_id.as_str(), "abs");
    assert_eq!(jobs[0].dataflow_id.as_str(), "abs.cpi");
    assert_eq!(
        jobs[0].source_url,
        "https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD"
    );
    assert_eq!(
        jobs[0].metadata["dataflow_url"],
        "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0"
    );
    assert_eq!(jobs[0].metadata["abs_dataflow_id"], "CPI");
    assert_eq!(jobs[0].metadata["version"], "2.0.0");
    assert_eq!(jobs[0].metadata["last_updated"], "2026-04-28T00:00:00Z");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_fetches_abs_dataflow_listing_over_http() {
    let base_url = serve_once(DATAFLOW_FIXTURE).await;
    let adapter = AbsAdapter::builder().base_url(base_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap());

    let jobs = adapter.discover(&ctx).await.expect("discover CPI");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, "abs:CPI:2.0.0:2026-04-28T00-00-00Z");
}

#[test]
fn changed_timestamp_emits_distinct_job_id_for_same_version() {
    let current = AbsAdapter::parse_dataflow_listing(DATAFLOW_FIXTURE).expect("parse fixture");
    let known = BTreeMap::from([(
        "CPI".to_string(),
        DataflowRevision::new("2.0.0", Some("2026-04-27T00:00:00Z")),
    )]);

    let jobs = AbsAdapter::discoverable_jobs(&current, &known);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, "abs:CPI:2.0.0:2026-04-28T00-00-00Z");
}

#[test]
fn parse_dataflow_listing_rejects_missing_dataflows() {
    let err = AbsAdapter::parse_dataflow_listing(r#"{"data":{}}"#)
        .expect_err("missing dataflows should fail");

    assert!(err.to_string().contains("dataflows"));
}

#[test]
fn parse_dataflow_listing_rejects_missing_version() {
    let body = r#"{
      "data": {
        "dataflows": [
          {
            "id": "CPI",
            "agencyID": "ABS",
            "name": "Consumer Price Index",
            "links": [
              { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0" }
            ]
          }
        ]
      }
    }"#;

    let err = AbsAdapter::parse_dataflow_listing(body).expect_err("missing version should fail");

    assert!(err.to_string().contains("version"));
}

#[test]
fn parse_dataflow_listing_rejects_missing_links() {
    let body = r#"{
      "data": {
        "dataflows": [
          {
            "id": "CPI",
            "agencyID": "ABS",
            "version": "2.0.0",
            "name": "Consumer Price Index",
            "links": []
          }
        ]
      }
    }"#;

    let err = AbsAdapter::parse_dataflow_listing(body).expect_err("missing links should fail");

    assert!(err.to_string().contains("canonical link"));
}

#[test]
fn parse_dataflow_listing_selects_canonical_dataflow_link_by_href_and_rel() {
    let body = r#"{
      "data": {
        "dataflows": [
          {
            "id": "CPI",
            "agencyID": "ABS",
            "version": "2.0.0",
            "names": {"en": "Consumer Price Index"},
            "lastUpdated": "2026-04-28T00:00:00Z",
            "links": [
              { "href": "https://example.test/first", "rel": "external" },
              { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0", "rel": "self" }
            ]
          }
        ]
      }
    }"#;

    let current = AbsAdapter::parse_dataflow_listing(body).expect("parse canonical link fixture");

    assert_eq!(
        current[0].dataflow_url,
        "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0"
    );
    assert_eq!(
        current[0].source_url,
        "https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD"
    );
    assert_eq!(current[0].name, "Consumer Price Index");
}

#[test]
fn parse_dataflow_listing_snapshot() {
    let current = AbsAdapter::parse_dataflow_listing(DATAFLOW_FIXTURE).expect("parse fixture");
    let snapshot = current
        .iter()
        .map(|flow| {
            json!({
                "id": flow.id,
                "agency_id": flow.agency_id,
                "version": flow.version,
                "name": flow.name,
                "last_updated": flow.last_updated,
                "source_url": flow.source_url,
                "dataflow_url": flow.dataflow_url,
            })
        })
        .collect::<Vec<_>>();

    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn manifest_declares_abs_cpi_and_conservative_rate_limit() {
    let adapter = AbsAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(adapter.id(), "abs");
    assert_eq!(manifest.source_id.as_str(), "abs");
    assert_eq!(manifest.dataflows[0].as_str(), "abs.cpi");
    assert_eq!(manifest.rate_limit.max_requests, 60);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
}

proptest! {
    #[test]
    fn parse_dataflow_listing_uses_canonical_link_regardless_of_order(
        insert_at in 0usize..6,
        noise_count in 0usize..5,
    ) {
        let canonical = json!({
            "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0",
            "rel": "self",
        });
        let mut links = (0..noise_count)
            .map(|idx| {
                json!({
                    "href": format!("https://example.test/noise/{idx}"),
                    "rel": "external",
                })
            })
            .collect::<Vec<_>>();
        let position = insert_at.min(links.len());
        links.insert(position, canonical);

        let body = json!({
            "data": {
                "dataflows": [{
                    "id": "CPI",
                    "agencyID": "ABS",
                    "version": "2.0.0",
                    "name": "Consumer Price Index",
                    "updated": "2026-04-28T00:00:00Z",
                    "links": links,
                }]
            }
        })
        .to_string();

        let current = AbsAdapter::parse_dataflow_listing(&body).expect("parse generated dataflow");
        prop_assert_eq!(
            current[0].dataflow_url.as_str(),
            "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0"
        );
        prop_assert_eq!(
            current[0].source_url.as_str(),
            "https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD"
        );
    }
}
