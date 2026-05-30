use std::collections::{BTreeMap, BTreeSet};

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_asx::AsxAdapter;
use au_kpis_domain::{ArtifactId, SourceId, TimePrecision};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;

const ANNOUNCEMENTS_RSS: &[u8] = br#"
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:asx="https://www.asx.com.au/rss">
  <channel>
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

const EOD_CSV: &[u8] = br#"ticker,date,open,high,low,close,volume,company_name
BHP,2026-05-29,42.10,42.80,41.95,42.55,18234567,BHP Group Limited
CBA,2026-05-29,169.25,170.10,168.40,169.80,3250123,Commonwealth Bank of Australia
"#;

#[derive(Debug, Serialize)]
struct FixtureSnapshot {
    observation_count: usize,
    series_count: usize,
    first_rows: Vec<SnapshotRow>,
}

#[derive(Debug, Serialize)]
struct SnapshotRow {
    dataflow_id: String,
    measure_id: String,
    dimensions: BTreeMap<String, String>,
    unit: String,
    time: String,
    time_precision: String,
    value: Option<f64>,
    status: String,
    attributes: BTreeMap<String, String>,
    source_artifact_id: String,
}

async fn artifact_for(
    blob_store: &BlobStore,
    bytes: &'static [u8],
    source_url: &str,
    content_type: &str,
) -> ArtifactRef {
    let id = blob_store
        .put_artifact(Bytes::from_static(bytes))
        .await
        .expect("store fixture artifact");
    ArtifactRef {
        id,
        source_id: SourceId::new("asx").unwrap(),
        source_url: source_url.into(),
        content_type: content_type.into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 29, 8, 0, 0).unwrap(),
    }
}

async fn snapshot_fixture(
    artifact: ArtifactRef,
    blob_store: BlobStore,
    dataflow: &str,
) -> FixtureSnapshot {
    let adapter = AsxAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 29, 8, 5, 0).unwrap(),
    )
    .with_expected_dataflow(
        au_kpis_domain::DataflowId::new(dataflow).unwrap(),
        BTreeMap::from([("test_dataflow".into(), dataflow.into())]),
    );
    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse ASX fixture");

    assert!(
        rows.iter()
            .all(|(_, observation)| observation.time_precision == TimePrecision::Day)
    );
    let series_count = rows
        .iter()
        .map(|(series, _)| series.series_key)
        .collect::<BTreeSet<_>>()
        .len();
    let first_rows = rows
        .iter()
        .take(12)
        .map(|(series, observation)| SnapshotRow {
            dataflow_id: series.dataflow_id.as_str().to_string(),
            measure_id: series.measure_id.as_str().to_string(),
            dimensions: series
                .dimensions
                .iter()
                .map(|(key, value)| (key.as_str().to_string(), value.as_str().to_string()))
                .collect(),
            unit: series.unit.clone(),
            time: observation.time.to_rfc3339(),
            time_precision: format!("{:?}", observation.time_precision),
            value: observation.value,
            status: format!("{:?}", observation.status),
            attributes: observation.attributes.clone(),
            source_artifact_id: observation.source_artifact_id.to_hex(),
        })
        .collect();

    FixtureSnapshot {
        observation_count: rows.len(),
        series_count,
        first_rows,
    }
}

#[tokio::test]
async fn parses_asx_announcements_and_eod_fixtures() {
    let blob_store = BlobStore::new(InMemory::new());
    let announcement_artifact = artifact_for(
        &blob_store,
        ANNOUNCEMENTS_RSS,
        "https://www.asx.com.au/announcements.xml",
        "application/rss+xml",
    )
    .await;
    let eod_artifact = artifact_for(
        &blob_store,
        EOD_CSV,
        "https://data.example.invalid/asx/eod/latest.csv",
        "text/csv",
    )
    .await;

    let announcements = snapshot_fixture(
        announcement_artifact,
        blob_store.clone(),
        "asx.announcements",
    )
    .await;
    let eod = snapshot_fixture(eod_artifact, blob_store.clone(), "asx.eod").await;

    assert_eq!(announcements.observation_count, 2);
    assert_eq!(eod.observation_count, 10);
    insta::assert_json_snapshot!("announcements_rss", announcements);
    insta::assert_json_snapshot!("eod_csv", eod);
}

#[tokio::test]
async fn parse_rejects_ambiguous_asx_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let mut artifact = artifact_for(
        &blob_store,
        ANNOUNCEMENTS_RSS,
        "https://mirror.example.invalid/announcements.xml",
        "application/rss+xml",
    )
    .await;
    artifact.source_id = SourceId::new("rba").unwrap();

    let adapter = AsxAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 29, 8, 5, 0).unwrap(),
    );
    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("invalid provenance should fail");

    assert!(
        err.to_string()
            .contains("ASX parse received artifact for source")
    );
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact(Bytes::from_static(EOD_CSV))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different ASX artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        source_id: SourceId::new("asx").unwrap(),
        source_url: "https://data.example.invalid/asx/eod/latest.csv".into(),
        content_type: "text/csv".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: EOD_CSV.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 29, 8, 0, 0).unwrap(),
    };
    let adapter = AsxAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 29, 8, 5, 0).unwrap(),
    )
    .with_expected_dataflow(
        au_kpis_domain::DataflowId::new("asx.eod").unwrap(),
        BTreeMap::new(),
    );

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("mismatched storage key should fail");

    assert!(
        err.to_string().contains("does not match artifact id"),
        "{err}"
    );
}
