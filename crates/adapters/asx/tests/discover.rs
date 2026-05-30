use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_asx::{AsxAdapter, AsxEodFile, AsxRevision};
use chrono::{NaiveDate, TimeZone, Utc};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

const ANNOUNCEMENTS_RSS: &str = r#"
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:asx="https://www.asx.com.au/rss">
  <channel>
    <title>ASX Market Announcements</title>
    <item>
      <title>BHP: Quarterly activities report</title>
      <link>https://www.asx.com.au/asxpdf/20260529/pdf/06abc123.pdf</link>
      <guid>06abc123</guid>
      <pubDate>Fri, 29 May 2026 01:15:00 GMT</pubDate>
      <category>Periodic Reports</category>
      <asx:code>BHP</asx:code>
      <asx:marketSensitive>true</asx:marketSensitive>
      <description>Quarterly operating update.</description>
    </item>
    <item>
      <title>CBA: Trading halt</title>
      <link>https://www.asx.com.au/asxpdf/20260529/pdf/06def456.pdf</link>
      <guid>06def456</guid>
      <pubDate>Fri, 29 May 2026 00:45:00 GMT</pubDate>
      <category>Trading Halt</category>
      <description>Trading halt request.</description>
    </item>
  </channel>
</rss>
"#;

const ANNOUNCEMENTS_JSON: &str = r#"
{
  "data": {
    "items": [
      {
        "announcementTypes": ["End of Day"],
        "date": "2026-05-29T09:30:19.000Z",
        "documentKey": "2924-03095499-6A1327670",
        "headline": "End of Day",
        "isPriceSensitive": false,
        "symbol": "ZOR"
      },
      {
        "announcementTypes": ["Final Director's Interest Notice"],
        "date": "2026-05-29T09:27:04.000Z",
        "documentKey": "2924-03095498-6A1327673",
        "headline": "Final Director's Interest Notice",
        "isPriceSensitive": false,
        "symbol": "MTL"
      }
    ]
  }
}
"#;

async fn serve_announcements_once(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /announcements.xml HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-asx/")
        );

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/rss+xml\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });

    format!("http://{addr}/announcements.xml")
}

#[test]
fn parse_announcements_rss_resolves_tickers_and_market_metadata() {
    let announcements =
        AsxAdapter::parse_announcements_feed(ANNOUNCEMENTS_RSS).expect("parse ASX RSS");

    let snapshot = announcements
        .iter()
        .map(|announcement| {
            json!({
                "announcement_id": announcement.announcement_id,
                "ticker": announcement.ticker,
                "title": announcement.title,
                "category": announcement.category,
                "published_at": announcement.published_at.to_rfc3339(),
                "market_sensitive": announcement.market_sensitive,
                "source_url": announcement.source_url,
            })
        })
        .collect::<Vec<_>>();

    assert_eq!(announcements.len(), 2);
    assert_eq!(announcements[0].ticker, "BHP");
    assert_eq!(announcements[1].ticker, "CBA");
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn parse_announcements_json_feed_resolves_tickers_and_document_urls() {
    let announcements =
        AsxAdapter::parse_announcements_feed(ANNOUNCEMENTS_JSON).expect("parse ASX JSON feed");

    assert_eq!(announcements.len(), 2);
    assert_eq!(announcements[0].ticker, "ZOR");
    assert_eq!(announcements[0].announcement_id, "2924-03095499-6A1327670");
    assert_eq!(
        announcements[0].source_url,
        "https://asx.api.markitdigital.com/asx-research/1.0/file/2924-03095499-6A1327670"
    );
    assert_eq!(announcements[1].ticker, "MTL");
    assert_eq!(
        announcements[1].category.as_deref(),
        Some("Final Director's Interest Notice")
    );
}

#[test]
fn discoverable_jobs_skip_known_feed_revision_and_emit_eod_daily_job() {
    let announcements =
        AsxAdapter::parse_announcements_feed(ANNOUNCEMENTS_RSS).expect("parse ASX RSS");
    let eod_file = AsxEodFile::new(
        "https://data.example.invalid/asx/eod/20260529.csv",
        NaiveDate::from_ymd_opt(2026, 5, 29).unwrap(),
        Utc.with_ymd_and_hms(2026, 5, 29, 8, 0, 0).unwrap(),
    );
    let known_revisions = BTreeMap::from([(
        "ASX:ANNOUNCEMENTS:FEED".to_string(),
        AsxRevision::new(
            "2026-05-29T01:15:00Z:06abc123",
            Some("2026-05-29T01:15:00Z"),
        ),
    )]);

    let jobs = AsxAdapter::discoverable_jobs_with_started_at(
        &announcements,
        &[eod_file],
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 5, 29, 8, 5, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].source_id.as_str(), "asx");
    assert_eq!(jobs[0].dataflow_id.as_str(), "asx.eod");
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(jobs[0].metadata["artifact_kind"], "eod_csv");
    assert_eq!(jobs[0].metadata["revision_key"], "ASX:EOD:2026-05-29");
    assert_eq!(jobs[0].metadata["trading_date"], "2026-05-29");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_scrapes_announcements_feed_and_schedules_eod_file() {
    let announcements_feed_url = serve_announcements_once(ANNOUNCEMENTS_RSS).await;
    let adapter = AsxAdapter::builder()
        .announcements_feed_url(&announcements_feed_url)
        .eod_file_url("https://data.example.invalid/asx/eod/latest.csv")
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 29, 8, 5, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter.discover(&ctx).await.expect("discover ASX feeds");

    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].dataflow_id.as_str(), "asx.announcements");
    assert_eq!(jobs[0].metadata["artifact_kind"], "announcements_rss");
    assert_eq!(jobs[0].metadata["latest_ticker"], "BHP");
    assert_eq!(jobs[1].dataflow_id.as_str(), "asx.eod");
    assert_eq!(jobs[1].metadata["artifact_kind"], "eod_csv");
}

#[test]
fn manifest_declares_asx_rate_limit_and_dataflow_metadata() {
    let adapter = AsxAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "asx");
    assert_eq!(manifest.rate_limit.max_requests, 30);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest
            .dataflows
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        vec!["asx.announcements", "asx.eod"]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 2);
    assert_eq!(dataflows[0].id.as_str(), "asx.announcements");
    assert_eq!(dataflows[1].id.as_str(), "asx.eod");
    assert!(
        dataflows
            .iter()
            .all(|dataflow| dataflow.attribution == "Source: ASX")
    );
}
