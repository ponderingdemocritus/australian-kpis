use std::collections::BTreeMap;

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_nhsac::NhsacAdapter;
use au_kpis_domain::{ArtifactId, DataflowId, SourceId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;

const HOUSING_ACCORD_CSV: &[u8] = include_bytes!("fixtures/housing_accord_progress.csv");

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
) -> ArtifactRef {
    let id = blob_store
        .put_artifact(Bytes::from_static(bytes))
        .await
        .expect("store fixture artifact");
    ArtifactRef {
        id,
        fetch_id: None,
        source_id: SourceId::new("nhsac").unwrap(),
        source_url: source_url.into(),
        content_type: "text/csv".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
    }
}

async fn snapshot_rows(artifact: ArtifactRef, blob_store: BlobStore) -> Vec<SnapshotRow> {
    let adapter = NhsacAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("nhsac.housing_accord_progress").unwrap(),
        BTreeMap::new(),
    );
    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse NHSAC fixture");

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
async fn parses_housing_accord_progress_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        HOUSING_ACCORD_CSV,
        "https://nhsac.gov.au/publications/housing-accord-progress-2026.csv",
    )
    .await;

    let rows = snapshot_rows(artifact, blob_store).await;

    assert_eq!(rows.len(), 4);
    assert!(
        rows.iter()
            .any(|row| row.measure_id == "progress_to_target_pct"
                && row.dimensions.get("region").map(String::as_str) == Some("AUS")
                && row.time == "2024-01-01T00:00:00+00:00"
                && row.value == Some(100.0))
    );
    assert!(
        rows.iter()
            .any(|row| row.measure_id == "homes_completed" && row.value == Some(240000.0))
    );
    insta::assert_json_snapshot!(rows);
}

#[tokio::test]
async fn parse_rejects_mismatched_nhsac_source_and_dataflow() {
    let blob_store = BlobStore::new(InMemory::new());
    let mut artifact = artifact_for(
        &blob_store,
        HOUSING_ACCORD_CSV,
        "https://nhsac.gov.au/publications/housing-accord-progress-2026.csv",
    )
    .await;
    artifact.source_id = SourceId::new("abs").unwrap();

    let adapter = NhsacAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("nhsac.housing_accord_progress").unwrap(),
        BTreeMap::new(),
    );
    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("invalid source should fail");

    assert!(
        err.to_string()
            .contains("NHSAC parse received artifact for source")
    );
}

#[tokio::test]
async fn parse_rejects_expected_dataflow_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        HOUSING_ACCORD_CSV,
        "https://nhsac.gov.au/publications/housing-accord-progress-2026.csv",
    )
    .await;
    let adapter = NhsacAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(DataflowId::new("nhsac.other").unwrap(), BTreeMap::new());

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("mismatched expected dataflow should fail");

    assert!(err.to_string().contains("expected dataflow"));
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact(Bytes::from_static(HOUSING_ACCORD_CSV))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different NHSAC housing artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        fetch_id: None,
        source_id: SourceId::new("nhsac").unwrap(),
        source_url: "https://nhsac.gov.au/publications/housing-accord-progress-2026.csv".into(),
        content_type: "text/csv".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: HOUSING_ACCORD_CSV.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
    };
    let adapter = NhsacAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("nhsac.housing_accord_progress").unwrap(),
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
