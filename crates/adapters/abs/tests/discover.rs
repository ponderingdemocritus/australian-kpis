use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterError, AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_abs::{AbsAdapter, DataflowRevision};
use au_kpis_error::{Classify, ErrorClass};
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

const GZIP_DATAFLOW_FIXTURE: &[u8] = &[
    0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xa5, 0x51, 0x4d, 0x6b, 0xc3, 0x30,
    0x0c, 0xbd, 0xe7, 0x57, 0x18, 0x9f, 0x36, 0x68, 0x6d, 0x37, 0x8c, 0x31, 0x72, 0xdb, 0xc7, 0x25,
    0xb7, 0xc2, 0x06, 0x85, 0x8d, 0x1d, 0xb4, 0x46, 0x6d, 0xcd, 0x1c, 0x27, 0xd8, 0x4e, 0x97, 0x51,
    0xf2, 0xdf, 0x67, 0x9b, 0x34, 0x33, 0x8c, 0x7d, 0x83, 0x0f, 0xb2, 0x9e, 0xf4, 0x9e, 0xf4, 0x74,
    0xc8, 0x08, 0xa1, 0x15, 0x38, 0xa0, 0x05, 0x39, 0xf8, 0x78, 0xfc, 0x6d, 0x54, 0xf3, 0x62, 0x7d,
    0xea, 0x21, 0xa6, 0xc8, 0x08, 0x45, 0x58, 0x56, 0x3e, 0x4f, 0xaf, 0x97, 0x25, 0x9d, 0xbd, 0x27,
    0x61, 0x8b, 0x7a, 0xfd, 0x5a, 0xde, 0x04, 0xe8, 0xf2, 0xea, 0x36, 0x85, 0xf6, 0x68, 0xac, 0x6c,
    0x74, 0x40, 0x72, 0x26, 0x98, 0x48, 0x31, 0x0d, 0x35, 0x46, 0xb6, 0x46, 0xdb, 0xae, 0x46, 0x43,
    0x96, 0x46, 0xae, 0x91, 0x94, 0xba, 0xc2, 0x9e, 0x9c, 0x78, 0x8d, 0xd3, 0xb4, 0xba, 0x6b, 0xfd,
    0x68, 0x18, 0xe5, 0x73, 0x91, 0x9f, 0xcf, 0xc5, 0xd9, 0x3c, 0xbf, 0xb8, 0x13, 0xa2, 0x88, 0xef,
    0x3e, 0x2d, 0x55, 0x52, 0x3f, 0xa7, 0xf3, 0xc7, 0x1d, 0x08, 0xdd, 0x19, 0xdc, 0x84, 0xf6, 0x9d,
    0x73, 0xad, 0x2d, 0x38, 0xc7, 0x1e, 0xea, 0x56, 0x21, 0x73, 0x68, 0x1d, 0xaf, 0xd1, 0x41, 0xd8,
    0x9d, 0xc7, 0xdd, 0x08, 0x35, 0xa8, 0x42, 0x2d, 0x28, 0x87, 0x46, 0x7b, 0x61, 0x4a, 0x86, 0xd9,
    0xd7, 0x74, 0xa1, 0x9b, 0x41, 0x2b, 0x19, 0x3c, 0x59, 0xb6, 0x6d, 0xf6, 0x0c, 0x3a, 0x6e, 0x02,
    0xf5, 0xd1, 0x52, 0xee, 0xbd, 0x09, 0xf4, 0x7c, 0x74, 0x62, 0x12, 0xc1, 0x3e, 0x8a, 0x28, 0xaf,
    0x31, 0x49, 0x3c, 0x8e, 0xd1, 0xa4, 0xfa, 0xe1, 0x08, 0xab, 0xbf, 0x1c, 0x61, 0xf1, 0xd9, 0x11,
    0x56, 0xbe, 0x3f, 0x3d, 0xc0, 0x77, 0xd6, 0x8b, 0x7f, 0x59, 0xff, 0x33, 0xaf, 0xfc, 0x86, 0x7c,
    0xf1, 0x1b, 0xaf, 0xb2, 0xe3, 0x6f, 0xc8, 0x86, 0x37, 0xb0, 0xc8, 0x58, 0x13, 0xdc, 0x02, 0x00,
    0x00,
];

const MULTI_VERSION_CPI_FIXTURE: &str = r#"{
  "data": {
    "dataflows": [
      {
        "id": "CPI",
        "agencyID": "ABS",
        "version": "1.0.0",
        "name": "Consumer Price Index (old)",
        "updated": "2025-01-01T00:00:00Z",
        "links": [
          { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/1.0.0", "rel": "self" }
        ]
      },
      {
        "id": "CPI",
        "agencyID": "ABS",
        "version": "2.0.0",
        "name": "Consumer Price Index",
        "updated": "2026-04-28T00:00:00Z",
        "links": [
          { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0", "rel": "self" }
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

async fn serve_gzip_once(body: &'static [u8]) -> String {
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

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-encoding: gzip\r\ncontent-length: {}\r\n\r\n",
            body.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write gzip headers");
        stream.write_all(body).await.expect("write gzip body");
    });

    format!("http://{addr}/rest")
}

#[test]
fn diff_cpi_dataflow_emits_only_new_or_changed_releases() {
    let current = AbsAdapter::parse_dataflow_listing(DATAFLOW_FIXTURE).expect("parse fixture");
    let unchanged = BTreeMap::from([(
        "ABS:CPI".to_string(),
        DataflowRevision::new("2.0.0", Some("2026-04-28T00:00:00Z")),
    )]);
    let changed = BTreeMap::from([(
        "ABS:CPI".to_string(),
        DataflowRevision::new("2.0.0", Some("2025-01-01T00:00:00Z")),
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
    assert_eq!(jobs[0].metadata["revision_key"], "ABS:CPI");
    assert_eq!(jobs[0].metadata["last_updated"], "2026-04-28T00:00:00Z");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_fetches_abs_dataflow_listing_over_http() {
    let base_url = serve_once(DATAFLOW_FIXTURE).await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap());

    let jobs = adapter.discover(&ctx).await.expect("discover CPI");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, "abs:CPI:2.0.0:2026-04-28T00-00-00Z");
    assert_eq!(
        jobs[0].source_url,
        format!("{base_url}/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_decodes_compressed_abs_dataflow_listing() {
    let base_url = serve_gzip_once(GZIP_DATAFLOW_FIXTURE).await;
    let adapter = AbsAdapter::builder().base_url(&base_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap());

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover compressed CPI listing");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, "abs:CPI:2.0.0:2026-04-28T00-00-00Z");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_applies_known_revisions_from_context() {
    let base_url = serve_once(DATAFLOW_FIXTURE).await;
    let adapter = AbsAdapter::builder().base_url(base_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap())
        .with_known_revision(
            "ABS:CPI",
            DataflowRevision::new("2.0.0", Some("2026-04-28T00:00:00Z")),
        );

    let jobs = adapter.discover(&ctx).await.expect("discover CPI");

    assert!(jobs.is_empty());
}

#[test]
fn changed_timestamp_emits_distinct_job_id_for_same_version() {
    let current = AbsAdapter::parse_dataflow_listing(DATAFLOW_FIXTURE).expect("parse fixture");
    let known = BTreeMap::from([(
        "ABS:CPI".to_string(),
        DataflowRevision::new("2.0.0", Some("2026-04-27T00:00:00Z")),
    )]);

    let jobs = AbsAdapter::discoverable_jobs(&current, &known);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, "abs:CPI:2.0.0:2026-04-28T00-00-00Z");
}

#[test]
fn diff_keys_cpi_revisions_by_dataflow_and_selects_latest_version() {
    let current =
        AbsAdapter::parse_dataflow_listing(MULTI_VERSION_CPI_FIXTURE).expect("parse fixture");
    let unchanged = BTreeMap::from([(
        "ABS:CPI".to_string(),
        DataflowRevision::new("2.0.0", Some("2026-04-28T00:00:00Z")),
    )]);
    assert!(AbsAdapter::discoverable_jobs(&current, &unchanged).is_empty());

    let changed_latest = BTreeMap::from([(
        "ABS:CPI".to_string(),
        DataflowRevision::new("1.0.0", Some("2025-01-01T00:00:00Z")),
    )]);
    let jobs = AbsAdapter::discoverable_jobs(&current, &changed_latest);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].metadata["revision_key"], "ABS:CPI");
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

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(err.to_string().contains("version"));
}

#[test]
fn parse_dataflow_listing_rejects_missing_id_rows() {
    let body = r#"{
      "data": {
        "dataflows": [
          {
            "agencyID": "ABS",
            "version": "2.0.0",
            "name": "Consumer Price Index",
            "updated": "2026-04-28T00:00:00Z",
            "links": [
              { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0", "rel": "self" }
            ]
          }
        ]
      }
    }"#;

    let err = AbsAdapter::parse_dataflow_listing(body).expect_err("missing id should fail");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(err.to_string().contains("missing id"));
}

#[test]
fn parse_dataflow_listing_rejects_malformed_non_cpi_rows() {
    let body = r#"{
      "data": {
        "dataflows": [
          {
            "id": "WPI",
            "agencyID": "ABS",
            "name": "Wage Price Index"
          },
          {
            "id": "CPI",
            "agencyID": "ABS",
            "version": "2.0.0",
            "name": "Consumer Price Index",
            "updated": "2026-04-28T00:00:00Z",
            "links": [
              { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0", "rel": "self" }
            ]
          }
        ]
      }
    }"#;

    let err = AbsAdapter::parse_dataflow_listing(body).expect_err("malformed row should fail");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
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

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_derives_fetch_url_from_configured_abs_base() {
    let body = r#"{
      "data": {
        "dataflows": [
          {
            "id": "CPI",
            "agencyID": "ABS",
            "version": "2.0.0",
            "name": "Consumer Price Index",
            "updated": "2026-04-28T00:00:00Z",
            "links": [
              { "href": "https://metadata.example.test/rest/dataflow/ABS/CPI/2.0.0", "rel": "external" }
            ]
          }
        ]
      }
    }"#;
    let base_url = serve_once(body).await;
    let adapter = AbsAdapter::builder().base_url(base_url.clone()).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap());

    let jobs = adapter.discover(&ctx).await.expect("discover CPI");

    assert_eq!(
        jobs[0].metadata["dataflow_url"],
        "https://metadata.example.test/rest/dataflow/ABS/CPI/2.0.0"
    );
    assert_eq!(
        jobs[0].source_url,
        format!("{base_url}/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD")
    );
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
                "revision_key": format!("{}:{}", flow.agency_id, flow.id),
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
