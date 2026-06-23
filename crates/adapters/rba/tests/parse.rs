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
const CURRENT_A1_CSV: &[u8] = b"\xEF\xBB\xBFA1 RESERVE BANK OF AUSTRALIA - BALANCE SHEET,,
Title,Notes on issue,Exchange settlement balances
Frequency,Weekly,Weekly
Units,$ million,$ million
Source,RBA,RBA
Publication date,19-Jun-2026,19-Jun-2026
Series ID,ARBALNOIW,ARBALESBW
03-Jul-2013,56986,1208
";
const CURRENT_A2_RANGE_CSV: &[u8] =
    b"\xEF\xBB\xBFA2 RESERVE BANK OF AUSTRALIA - CHANGES IN MONETARY POLICY AND ADMINISTERED RATES
Title,Change in Cash Rate Target,New Cash Rate Target
Frequency,As announced,As announced
Units,Per cent,Per cent
Source,RBA,RBA
Series ID,ARBAMPCCCR,ARBAMPCNCRT
23-Jan-1990,-0.50 to -1.00,17.00 to 17.50
";
const CURRENT_A3_CP1252_CSV: &[u8] =
    b"A3 MONETARY POLICY OPERATIONS \x96 REGULAR OPEN MARKET LIQUIDITY OPERATIONS,,,,,
Title,Number of OMO repos,Value of OMO repos,,,,
Frequency,Daily,Daily,,,,
Units,Number,$ million,,,,
Source,RBA,RBA,,,,
Series ID,ARBAMPOON,ARBAMPOOV,,,,
03-Jul-2013,1,100,,,,
";
const CURRENT_A3_LONG_DATED_CSV: &[u8] =
    b"A3 MONETARY POLICY OPERATIONS - LONG-DATED OPEN MARKET OPERATIONS
Title,Bond Issuer,Coupon Rate,Maturity,Face Value,Average Purchase Rate,Cut-off Rate,Value Date
Frequency,per operation,per operation,per operation,per operation,per operation,per operation,per operation
Units,Index,Per cent,Date,$m,Per cent,Per cent,Date
Source,RBA,RBA,RBA,RBA,RBA,RBA,RBA
Publication date,19-Jun-2026,19-Jun-2026,19-Jun-2026,19-Jun-2026,19-Jun-2026,19-Jun-2026,19-Jun-2026
Series ID,ALDOMOISS,ALDOMOCP,ALDOMOMD,ALDOMOFVD,AOMOLDWAR,ALDOMOCOR,ALDOMOSD
10-Apr-2014,NSWTC,4.00,20-Feb-2017,23,3.1900,3.1900,15-Apr-2014
";

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
        fetch_id: None,
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
async fn parses_current_rba_csv_shape_without_date_header() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        CURRENT_A1_CSV,
        "https://www.rba.gov.au/statistics/tables/csv/a1-data.csv",
        "text/csv",
    )
    .await;
    let rows = snapshot_rows(artifact, blob_store).await;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].time, "2013-07-03T00:00:00+00:00");
    assert_eq!(rows[0].time_precision, "Day");
    assert_eq!(rows[0].value, Some(56_986.0));
    assert_eq!(rows[0].unit, "$ million");
    assert_eq!(
        rows[0].dimensions.get("series_id").map(String::as_str),
        Some("ARBALNOIW")
    );
    assert_eq!(
        rows[0].dimensions.get("series_name").map(String::as_str),
        Some("Notes on issue")
    );
    assert_eq!(
        rows[0].attributes.get("frequency").map(String::as_str),
        Some("Weekly")
    );
}

#[tokio::test]
async fn parses_current_rba_range_cells_as_missing_values() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        CURRENT_A2_RANGE_CSV,
        "https://www.rba.gov.au/statistics/tables/csv/a2-data.csv",
        "text/csv",
    )
    .await;
    let rows = snapshot_rows(artifact, blob_store).await;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].time, "1990-01-23T00:00:00+00:00");
    assert_eq!(rows[0].value, None);
    assert_eq!(rows[0].status, "Missing");
    assert_eq!(
        rows[0].dimensions.get("series_id").map(String::as_str),
        Some("ARBAMPCCCR")
    );
}

#[tokio::test]
async fn parses_current_rba_windows_1252_csv_shape() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        CURRENT_A3_CP1252_CSV,
        "https://www.rba.gov.au/statistics/tables/csv/a3-daily-open-market-operations.csv",
        "text/csv",
    )
    .await;
    let rows = snapshot_rows(artifact, blob_store).await;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].time, "2013-07-03T00:00:00+00:00");
    assert_eq!(rows[0].value, Some(1.0));
    assert_eq!(
        rows[0].attributes.get("table_title").map(String::as_str),
        Some("A3 MONETARY POLICY OPERATIONS – REGULAR OPEN MARKET LIQUIDITY OPERATIONS")
    );
}

#[tokio::test]
async fn parses_current_rba_mixed_operation_rows_as_numeric_observations_with_context() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        CURRENT_A3_LONG_DATED_CSV,
        "https://www.rba.gov.au/statistics/tables/csv/a3-long-dated-open-mkt-operations.csv",
        "text/csv",
    )
    .await;
    let rows = snapshot_rows(artifact, blob_store).await;

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].time, "2014-04-10T00:00:00+00:00");
    assert_eq!(
        rows[0].dimensions.get("series_id").map(String::as_str),
        Some("ALDOMOCP")
    );
    assert_eq!(
        rows[0].dimensions.get("series_name").map(String::as_str),
        Some("Coupon Rate")
    );
    assert_eq!(rows[0].value, Some(4.0));
    assert_eq!(
        rows[0]
            .attributes
            .get("rba_context_bond_issuer")
            .map(String::as_str),
        Some("NSWTC")
    );
    assert_eq!(
        rows[0]
            .attributes
            .get("rba_context_maturity")
            .map(String::as_str),
        Some("20-Feb-2017")
    );
    assert_eq!(
        rows[0]
            .attributes
            .get("rba_context_value_date")
            .map(String::as_str),
        Some("15-Apr-2014")
    );
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
        fetch_id: None,
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
