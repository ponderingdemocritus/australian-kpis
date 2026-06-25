use std::time::Duration;

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_asx::AsxAdapter;
use chrono::{TimeZone, Utc};

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

#[test]
fn current_jobs_emit_open_market_statistics_page_with_source_metadata() {
    let jobs = AsxAdapter::current_jobs_with_started_at(
        Utc.with_ymd_and_hms(2026, 6, 19, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, "asx:market-statistics:2026-06");
    assert_eq!(jobs[0].source_id.as_str(), "asx");
    assert_eq!(jobs[0].dataflow_id.as_str(), "asx.market_statistics");
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(jobs[0].metadata["revision_key"], "ASX:market-statistics");
    assert_eq!(jobs[0].metadata["revision_version"], "2026-06");
    assert_eq!(jobs[0].metadata["attribution"], "Source: ASX");
}

#[tokio::test]
async fn discover_emits_configured_announcements_and_eod_jobs() {
    let adapter = AsxAdapter::builder()
        .announcements_rss_url("https://feeds.example.test/asx/announcements.xml")
        .eod_csv_url("https://feeds.example.test/asx/eod.csv")
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 6, 19, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT.to_string());

    let jobs = adapter.discover(&ctx).await.expect("discover ASX jobs");

    assert_eq!(jobs.len(), 3);
    assert_eq!(jobs[1].id, "asx:announcements:2026-06-19");
    assert_eq!(jobs[1].dataflow_id.as_str(), "asx.announcements");
    assert_eq!(jobs[1].metadata["artifact_format"], "rss");
    assert_eq!(jobs[1].metadata["cadence"], "daily");

    assert_eq!(jobs[2].id, "asx:eod:2026-06-19");
    assert_eq!(jobs[2].dataflow_id.as_str(), "asx.eod");
    assert_eq!(jobs[2].metadata["artifact_format"], "csv");
    assert_eq!(jobs[2].metadata["cadence"], "daily");
}

#[test]
fn manifest_declares_asx_market_statistics_metadata() {
    let adapter = AsxAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "asx");
    assert_eq!(manifest.rate_limit.max_requests, 30);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![
            au_kpis_domain::DataflowId::new("asx.market_statistics").unwrap(),
            au_kpis_domain::DataflowId::new("asx.announcements").unwrap(),
            au_kpis_domain::DataflowId::new("asx.eod").unwrap(),
        ]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 3);
    assert_eq!(dataflows[0].id.as_str(), "asx.market_statistics");
    assert_eq!(dataflows[1].id.as_str(), "asx.announcements");
    assert_eq!(dataflows[2].id.as_str(), "asx.eod");
    assert_eq!(dataflows[0].attribution, "Source: ASX");
    assert_eq!(
        dataflows[0].source_url,
        "https://www.asx.com.au/about/market-statistics/historical-market-statistics"
    );
    assert_eq!(
        dataflows[1].source_url,
        "https://www.asx.com.au/connectivity-and-data/information-services/company-news"
    );
    assert_eq!(
        dataflows[2].source_url,
        "https://www.asx.com.au/connectivity-and-data/information-services/reference-data"
    );
}

#[test]
fn dataflow_metadata_keeps_official_source_url_when_page_url_is_overridden() {
    let adapter = AsxAdapter::builder()
        .market_statistics_url("http://127.0.0.1:8766/market-statistics.html")
        .build();

    assert_eq!(
        adapter.dataflow_metadata()[0].source_url,
        "https://www.asx.com.au/about/market-statistics/historical-market-statistics"
    );
}
