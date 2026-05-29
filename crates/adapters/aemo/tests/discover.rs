use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterError, AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_aemo::{AemoAdapter, AemoDispatchFileRevision, FRESHNESS_SLO_SECONDS};
use chrono::{TimeZone, Utc};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

const DISPATCH_LISTING: &str = r#"
<html>
  <body>
    <pre><A HREF="/Reports/CURRENT">[To Parent Directory]</A><br><br>
       Friday, May 8, 2026 05:16 AM        &lt;dir&gt; <A HREF="/Reports/CURRENT/DispatchIS_Reports/DUPLICATE">DUPLICATE</A><br>
      Friday, May 29, 2026 10:51 AM        20468 <A HREF="/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605291055_0000000519884398.zip">PUBLIC_DISPATCHIS_202605291055_0000000519884398.zip</A><br>
      Friday, May 29, 2026 10:57 AM        20660 <A HREF="/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605291100_0000000519884840.zip">PUBLIC_DISPATCHIS_202605291100_0000000519884840.zip</A><br>
      Friday, May 29, 2026 11:02 AM        20628 <A HREF="/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605291105_0000000519885422.zip">PUBLIC_DISPATCHIS_202605291105_0000000519885422.zip</A><br>
      Friday, May 29, 2026 11:06 AM        20654 <A HREF="/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip">PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip</A><br>
      Friday, May 29, 2026 11:07 AM         1024 <A HREF="/Reports/CURRENT/DispatchIS_Reports/README.txt">README.txt</A><br>
    </pre>
  </body>
</html>
"#;

async fn serve_once(
    status: &'static str,
    headers: &'static [(&'static str, &'static str)],
    body: &'static str,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /Reports/CURRENT/DispatchIS_Reports/ HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-aemo/")
        );

        let mut response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: text/html\r\ncontent-length: {}\r\n",
            body.len(),
        );
        for (name, value) in headers {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        response.push_str(body);
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });

    format!("http://{addr}/Reports/CURRENT/DispatchIS_Reports/")
}

#[test]
fn parse_dispatch_listing_discovers_five_minute_zip_files() {
    let files =
        AemoAdapter::parse_dispatch_listing(DISPATCH_LISTING).expect("parse NEMWeb listing");

    let snapshot = files
        .iter()
        .map(|file| {
            json!({
                "file_name": file.file_name,
                "dispatch_interval": file.dispatch_interval.to_rfc3339(),
                "published_at": file.published_at.to_rfc3339(),
                "size_bytes": file.size_bytes,
                "sequence": file.sequence,
                "freshness_lag_seconds_at_1110": file.freshness_lag_seconds(
                    Utc.with_ymd_and_hms(2026, 5, 29, 1, 10, 0).unwrap()
                ),
                "source_url": file.source_url,
            })
        })
        .collect::<Vec<_>>();

    assert_eq!(files.len(), 4);
    assert!(
        files[2].freshness_lag_seconds(Utc.with_ymd_and_hms(2026, 5, 29, 1, 10, 0).unwrap())
            <= FRESHNESS_SLO_SECONDS
    );
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn discoverable_jobs_skip_known_revisions_and_carry_freshness_metadata() {
    let files =
        AemoAdapter::parse_dispatch_listing(DISPATCH_LISTING).expect("parse NEMWeb listing");
    let known_revisions = BTreeMap::from([(
        "AEMO:DISPATCHIS:202605291055".to_string(),
        AemoDispatchFileRevision::new("0000000519884398", Some("2026-05-29T00:51:00Z")),
    )]);

    let jobs = AemoAdapter::discoverable_jobs_with_started_at(
        &files,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 5, 29, 1, 10, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 3);
    assert!(jobs.iter().all(|job| job.source_id.as_str() == "aemo"));
    assert!(
        jobs.iter()
            .all(|job| job.dataflow_id.as_str() == "aemo.dispatch")
    );
    assert!(
        jobs.iter()
            .all(|job| job.trace_parent.as_deref() == Some(TRACE_PARENT))
    );
    assert!(jobs.iter().all(|job| job.metadata["cadence"] == "5min"));
    assert!(
        jobs.iter()
            .all(|job| job.metadata["poll_interval_seconds"] == "300")
    );
    assert!(
        jobs.iter()
            .all(|job| job.metadata["freshness_slo_seconds"] == "900")
    );
    assert_eq!(
        jobs[0].metadata["revision_key"],
        "AEMO:DISPATCHIS:202605291100"
    );
    assert_eq!(jobs[0].metadata["revision_version"], "0000000519884840");
    assert_eq!(jobs[0].metadata["published_at"], "2026-05-29T00:57:00Z");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_scrapes_nemweb_listing_over_http() {
    let listing_url = serve_once("200 OK", &[], DISPATCH_LISTING).await;
    let adapter = AemoAdapter::builder()
        .dispatch_listing_url(&listing_url)
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 29, 1, 10, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter.discover(&ctx).await.expect("discover AEMO files");

    assert_eq!(jobs.len(), 4);
    assert_eq!(jobs[0].metadata["artifact_format"], "zip");
    assert_eq!(jobs[0].metadata["csv_payload"], "aemo-csv-cid");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_returns_retry_after_when_nemweb_rate_limits() {
    let listing_url = serve_once("429 Too Many Requests", &[("Retry-After", "7")], "").await;
    let adapter = AemoAdapter::builder()
        .dispatch_listing_url(&listing_url)
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 29, 1, 10, 0).unwrap());

    let err = adapter
        .discover(&ctx)
        .await
        .expect_err("rate limit should be surfaced as retryable upstream status");

    match err {
        AdapterError::UpstreamStatus {
            status,
            retry_after,
            ..
        } => {
            assert_eq!(status, reqwest::StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(retry_after, Some(Duration::from_secs(7)));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn manifest_declares_aemo_rate_limit_and_dispatch_dataflow_metadata() {
    let adapter = AemoAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "aemo");
    assert_eq!(manifest.rate_limit.max_requests, 12);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![au_kpis_domain::DataflowId::new("aemo.dispatch").unwrap()]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 1);
    assert_eq!(dataflows[0].id.as_str(), "aemo.dispatch");
    assert_eq!(dataflows[0].frequency, au_kpis_domain::Frequency::Irregular);
    assert_eq!(
        dataflows[0].attribution,
        "Source: Australian Energy Market Operator"
    );
    assert_eq!(
        dataflows[0].source_url,
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    );
}
