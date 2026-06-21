use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{SourceAdapter, UpstreamRevision};
use au_kpis_adapter_aemo::AemoAdapter;
use chrono::{TimeZone, Utc};
use serde_json::json;

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

#[test]
fn manifest_declares_aemo_dispatch_metadata() {
    let adapter = AemoAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "aemo");
    assert_eq!(manifest.rate_limit.max_requests, 120);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![au_kpis_domain::DataflowId::new("aemo.dispatch").unwrap()]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 1);
    assert_eq!(dataflows[0].id.as_str(), "aemo.dispatch");
    assert_eq!(
        dataflows[0].attribution,
        "Source: Australian Energy Market Operator"
    );
    assert_eq!(
        dataflows[0].source_url,
        "https://www.nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/"
    );
}

#[test]
fn dataflow_metadata_keeps_official_source_url_when_listing_url_is_overridden() {
    let adapter = AemoAdapter::builder()
        .dispatch_listing_url("http://127.0.0.1:8765/listing.html")
        .build();

    assert_eq!(
        adapter.dataflow_metadata()[0].source_url,
        "https://www.nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/"
    );
}
