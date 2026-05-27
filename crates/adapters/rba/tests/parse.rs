use std::collections::BTreeMap;

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_rba::RbaAdapter;
use au_kpis_domain::{ArtifactId, SourceId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;

const F1_CSV: &[u8] = include_bytes!("fixtures/f1_money_market_daily.csv");
const G1_CSV: &[u8] = include_bytes!("fixtures/g1_consumer_price_inflation.csv");
const A1_XLSX: &[u8] = include_bytes!("fixtures/a1_balance_sheet_weekly.xlsx");

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
        source_id: SourceId::new("rba").unwrap(),
        source_url: source_url.into(),
        content_type: content_type.into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
    }
}

async fn snapshot_rows(artifact: ArtifactRef, blob_store: BlobStore) -> Vec<SnapshotRow> {
    let adapter = RbaAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    );
    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse RBA fixture");

    rows.into_iter()
        .map(|(series, observation)| SnapshotRow {
            dataflow_id: series.dataflow_id.as_str().to_string(),
            measure_id: series.measure_id.as_str().to_string(),
            dimensions: series
                .dimensions
                .into_iter()
                .map(|(key, value)| (key.as_str().to_string(), value.as_str().to_string()))
                .collect(),
            unit: series.unit,
            time: observation.time.to_rfc3339(),
            time_precision: format!("{:?}", observation.time_precision),
            value: observation.value,
            status: format!("{:?}", observation.status),
            attributes: observation.attributes,
            source_artifact_id: observation.source_artifact_id.to_hex(),
        })
        .collect()
}

#[tokio::test]
async fn parses_rba_csv_and_xls_statistical_table_fixtures() {
    let blob_store = BlobStore::new(InMemory::new());
    let fixtures = [
        (
            "f1_money_market_daily_csv",
            F1_CSV,
            "https://www.rba.gov.au/statistics/tables/csv/f01d.csv",
            "text/csv",
        ),
        (
            "g1_consumer_price_inflation_csv",
            G1_CSV,
            "https://www.rba.gov.au/statistics/tables/csv/g01.csv",
            "text/csv",
        ),
        (
            "a1_balance_sheet_weekly_xlsx",
            A1_XLSX,
            "https://www.rba.gov.au/statistics/tables/xls/a01hist.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
    ];

    for (name, bytes, source_url, content_type) in fixtures {
        let artifact = artifact_for(&blob_store, bytes, source_url, content_type).await;
        let rows = snapshot_rows(artifact, blob_store.clone()).await;
        insta::assert_json_snapshot!(name, rows);
    }
}

#[tokio::test]
async fn parse_rejects_ambiguous_rba_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let mut artifact = artifact_for(
        &blob_store,
        F1_CSV,
        "https://mirror.example.invalid/f01d.csv",
        "text/csv",
    )
    .await;
    artifact.source_id = SourceId::new("abs").unwrap();

    let adapter = RbaAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    );
    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("invalid provenance should fail");

    assert!(
        err.to_string()
            .contains("RBA parse received artifact for source")
    );
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact(Bytes::from_static(F1_CSV))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different RBA artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        source_id: SourceId::new("rba").unwrap(),
        source_url: "https://www.rba.gov.au/statistics/tables/csv/f01d.csv".into(),
        content_type: "text/csv".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: F1_CSV.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
    };
    let adapter = RbaAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
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
