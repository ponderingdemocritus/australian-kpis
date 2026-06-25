use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{SourceAdapter, UpstreamRevision};
use au_kpis_adapter_aemo::AemoAdapter;
use au_kpis_domain::DataflowId;
use chrono::{TimeZone, Utc};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

const LISTING_FIXTURE: &str = r#"
<html>
  <body>
    <A HREF="/Reports/CURRENT/DispatchIS_Reports/DUPLICATE/">DUPLICATE</A>
    Friday, June 19, 2026 04:56 PM 18879
    <A HREF="/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202606191700_0000000523261454.zip">PUBLIC_DISPATCHIS_202606191700_0000000523261454.zip</A>
    Friday, June 19, 2026 05:01 PM 18879
    <A HREF="/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202606191705_0000000523261987.zip">PUBLIC_DISPATCHIS_202606191705_0000000523261987.zip</A>
    <A HREF="/Reports/CURRENT/DispatchIS_Reports/README.txt">README.txt</A>
  </body>
</html>
"#;

const GENERATION_MIX_LISTING_FIXTURE: &str = r#"
<html>
  <body>
    Friday, June 19, 2026 05:06 PM 2048
    <A HREF="/Reports/CURRENT/FuelMix/PUBLIC_FUEL_MIX_202606191705_0000000523261987.zip">PUBLIC_FUEL_MIX_202606191705_0000000523261987.zip</A>
    <A HREF="/Reports/CURRENT/FuelMix/README.txt">README.txt</A>
  </body>
</html>
"#;

const GENERATION_MIX_ROOT_LISTING_FIXTURE: &str = r#"
<html>
  <body>
    <A HREF="/Reports/CURRENT/Next_Day_Actual_Gen/">Next_Day_Actual_Gen</A>
    <A HREF="/Reports/CURRENT/ROOFTOP_PV/">ROOFTOP_PV</A>
  </body>
</html>
"#;

const NEXT_DAY_ACTUAL_GEN_LISTING_FIXTURE: &str = r#"
<html>
  <body>
    Wednesday, June 24, 2026 12:08 AM 4096
    <A HREF="/Reports/CURRENT/Next_Day_Actual_Gen/PUBLIC_NEXT_DAY_ACTUAL_GEN_20260624_0000000524147230.zip">PUBLIC_NEXT_DAY_ACTUAL_GEN_20260624_0000000524147230.zip</A>
    <A HREF="/Reports/CURRENT/Next_Day_Actual_Gen/README.txt">README.txt</A>
  </body>
</html>
"#;

const DISPATCHABILITY_CAPACITY_LISTING_FIXTURE: &str = r#"
<html>
  <body>
    Friday, June 19, 2026 05:06 PM 4096
    <A HREF="/Reports/CURRENT/DispatchCapacity/PUBLIC_DISPATCHCAPACITY_202606191705_0000000523261987.zip">PUBLIC_DISPATCHCAPACITY_202606191705_0000000523261987.zip</A>
    <A HREF="/Reports/CURRENT/DispatchCapacity/README.txt">README.txt</A>
  </body>
</html>
"#;

async fn serve_dispatch_listing_once(body: &'static str) -> Option<String> {
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
        assert!(request.starts_with("GET /dispatch HTTP/1.1"), "{request}");
        assert!(
            request
                .to_ascii_lowercase()
                .contains("user-agent: au-kpis-adapter-aemo/"),
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

    Some(format!("http://{addr}/dispatch"))
}

async fn serve_generation_mix_root_and_next_day_once() -> Option<String> {
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
        for (expected_path, body) in [
            ("/generation-root", GENERATION_MIX_ROOT_LISTING_FIXTURE),
            (
                "/Reports/CURRENT/Next_Day_Actual_Gen/",
                NEXT_DAY_ACTUAL_GEN_LISTING_FIXTURE,
            ),
        ] {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let read = stream.read(&mut request).await.expect("read request");
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(
                request.starts_with(&format!("GET {expected_path} HTTP/1.1")),
                "{request}"
            );
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("user-agent: au-kpis-adapter-aemo/"),
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
        }
    });

    Some(format!("http://{addr}/generation-root"))
}

#[test]
fn parse_dispatch_listing_discovers_zip_artifacts() {
    let artifacts =
        AemoAdapter::parse_dispatch_listing(LISTING_FIXTURE).expect("parse AEMO listing");

    let snapshot = artifacts
        .iter()
        .map(|artifact| {
            json!({
                "file_name": artifact.file_name,
                "dispatch_interval": artifact.dispatch_interval,
                "source_url": artifact.source_url,
                "revision_key": artifact.revision_key(),
            })
        })
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn parse_generation_mix_listing_discovers_zip_artifacts() {
    let artifacts = AemoAdapter::parse_generation_mix_listing(GENERATION_MIX_LISTING_FIXTURE)
        .expect("parse AEMO generation mix listing");

    let snapshot = artifacts
        .iter()
        .map(|artifact| {
            json!({
                "file_name": artifact.file_name,
                "interval": artifact.interval,
                "source_url": artifact.source_url,
                "revision_key": artifact.revision_key(),
            })
        })
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn parse_dispatchability_capacity_listing_discovers_zip_artifacts() {
    let artifacts = AemoAdapter::parse_dispatchability_capacity_listing(
        DISPATCHABILITY_CAPACITY_LISTING_FIXTURE,
    )
    .expect("parse AEMO dispatchability capacity listing");

    let snapshot = artifacts
        .iter()
        .map(|artifact| {
            json!({
                "file_name": artifact.file_name,
                "interval": artifact.interval,
                "source_url": artifact.source_url,
                "revision_key": artifact.revision_key(),
            })
        })
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(snapshot);
}

#[test]
fn discoverable_jobs_apply_file_revision_and_source_metadata() {
    let current = AemoAdapter::parse_dispatch_listing(LISTING_FIXTURE).expect("parse AEMO listing");
    let known_revisions = BTreeMap::from([(
        current[0].revision_key(),
        UpstreamRevision::new(
            current[0].revision_version(),
            Some(current[0].file_name.clone()),
        ),
    )]);

    let jobs = AemoAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 15, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].source_id.as_str(), "aemo");
    assert_eq!(jobs[0].dataflow_id.as_str(), "aemo.dispatch");
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(
        jobs[0].metadata["dispatch_interval"],
        "2026-06-19T17:05:00Z"
    );
    assert_eq!(
        jobs[0].metadata["attribution"],
        "Source: Australian Energy Market Operator"
    );
    assert_eq!(
        jobs[0].metadata["license"],
        "AEMO Copyright and Disclaimer Notice"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_honours_requested_dispatch_scope() {
    let Some(dispatch_url) = serve_dispatch_listing_once(LISTING_FIXTURE).await else {
        return;
    };
    let adapter = AemoAdapter::builder()
        .dispatch_listing_url(&dispatch_url)
        .generation_mix_listing_url("http://127.0.0.1:9/broken-generation-mix")
        .dispatchability_capacity_listing_url("http://127.0.0.1:9/broken-capacity")
        .build();
    let http = au_kpis_adapter::AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = au_kpis_adapter::DiscoveryCtx::new(
        http,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 15, 0).unwrap(),
    )
    .with_requested_dataflow_id(DataflowId::new("aemo.dispatch").unwrap())
    .with_trace_parent(TRACE_PARENT);

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover dispatch only");

    assert_eq!(jobs.len(), 2);
    assert!(
        jobs.iter()
            .all(|job| job.dataflow_id.as_str() == "aemo.dispatch")
    );
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_uses_next_day_actual_gen_for_generation_mix_scope() {
    let Some(generation_mix_url) = serve_generation_mix_root_and_next_day_once().await else {
        return;
    };
    let adapter = AemoAdapter::builder()
        .dispatch_listing_url("http://127.0.0.1:9/broken-dispatch")
        .generation_mix_listing_url(&generation_mix_url)
        .dispatchability_capacity_listing_url("http://127.0.0.1:9/broken-capacity")
        .build();
    let http = au_kpis_adapter::AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = au_kpis_adapter::DiscoveryCtx::new(
        http,
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 10, 0).unwrap(),
    )
    .with_requested_dataflow_id(DataflowId::new("aemo.generation_mix").unwrap())
    .with_trace_parent(TRACE_PARENT);

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover generation mix proxy only");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].dataflow_id.as_str(), "aemo.generation_mix");
    assert!(jobs[0].source_url.contains("Next_Day_Actual_Gen"));
    assert!(jobs[0].source_url.contains("PUBLIC_NEXT_DAY_ACTUAL_GEN_"));
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(
        jobs[0].metadata["revision_key"],
        "AEMO:generation_mix:PUBLIC_NEXT_DAY_ACTUAL_GEN_20260624_0000000524147230.zip"
    );
    assert_eq!(
        jobs[0].metadata["proxy_source_family"],
        "Next_Day_Actual_Gen"
    );
    assert_eq!(jobs[0].metadata["aemo_table"], "METER_DATA.GEN_DUID");
    assert_eq!(jobs[0].metadata["aemo_field"], "MWH_READING");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_uses_dispatchis_proxy_for_capacity_scope() {
    let Some(dispatch_url) = serve_dispatch_listing_once(LISTING_FIXTURE).await else {
        return;
    };
    let adapter = AemoAdapter::builder()
        .dispatch_listing_url("http://127.0.0.1:9/broken-dispatch")
        .generation_mix_listing_url("http://127.0.0.1:9/broken-generation-mix")
        .dispatchability_capacity_listing_url(&dispatch_url)
        .build();
    let http = au_kpis_adapter::AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = au_kpis_adapter::DiscoveryCtx::new(
        http,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 15, 0).unwrap(),
    )
    .with_requested_dataflow_id(DataflowId::new("aemo.dispatchability_capacity").unwrap())
    .with_trace_parent(TRACE_PARENT);

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover capacity proxy only");

    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| {
        job.dataflow_id.as_str() == "aemo.dispatchability_capacity"
            && job.source_url.contains("PUBLIC_DISPATCHIS_")
    }));
    assert_eq!(jobs[0].trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(jobs[0].metadata["proxy_source_dataflow"], "aemo.dispatch");
    assert_eq!(jobs[0].metadata["aemo_table"], "DISPATCH.REGIONSUM");
    assert_eq!(jobs[0].metadata["aemo_field"], "AVAILABLEGENERATION");
    assert!(
        jobs[0].metadata["revision_key"]
            .starts_with("AEMO:dispatchability_capacity:PUBLIC_DISPATCHIS_")
    );
}

#[test]
fn manifest_declares_aemo_dispatch_metadata() {
    let adapter = AemoAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "aemo");
    assert_eq!(manifest.rate_limit.max_requests, 120);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![
            au_kpis_domain::DataflowId::new("aemo.dispatch").unwrap(),
            au_kpis_domain::DataflowId::new("aemo.generation_mix").unwrap(),
            au_kpis_domain::DataflowId::new("aemo.dispatchability_capacity").unwrap(),
        ]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 3);
    assert_eq!(dataflows[0].id.as_str(), "aemo.dispatch");
    assert_eq!(dataflows[1].id.as_str(), "aemo.generation_mix");
    assert_eq!(dataflows[1].measures[0].as_str(), "generation_mw");
    assert_eq!(dataflows[2].id.as_str(), "aemo.dispatchability_capacity");
    assert_eq!(dataflows[2].measures[0].as_str(), "value");
    assert_eq!(
        dataflows[0].attribution,
        "Source: Australian Energy Market Operator"
    );
    assert_eq!(
        dataflows[0].source_url,
        "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    );
}

#[test]
fn dataflow_metadata_keeps_official_source_url_when_listing_url_is_overridden() {
    let adapter = AemoAdapter::builder()
        .dispatch_listing_url("http://127.0.0.1:8765/listing.html")
        .build();

    assert_eq!(
        adapter.dataflow_metadata()[0].source_url,
        "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    );
}
