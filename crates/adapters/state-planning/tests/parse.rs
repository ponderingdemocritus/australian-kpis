use std::collections::BTreeMap;

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_state_planning::StatePlanningAdapter;
use au_kpis_domain::{ArtifactId, DataflowId, SourceId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;

const NSW_DA_PROCESSING: &[u8] = include_bytes!("fixtures/nsw_da_processing.csv");
const VIC_PERMIT_ACTIVITY: &[u8] = include_bytes!("fixtures/vic_permit_activity.csv");

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
        source_id: SourceId::new("state-planning").unwrap(),
        source_url: source_url.into(),
        content_type: "text/csv".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
    }
}

async fn snapshot_rows(
    artifact: ArtifactRef,
    blob_store: BlobStore,
    dataflow_id: &str,
) -> Vec<SnapshotRow> {
    let adapter = StatePlanningAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(DataflowId::new(dataflow_id).unwrap(), BTreeMap::new());
    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse state planning fixture");

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
async fn parses_nsw_da_processing_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        NSW_DA_PROCESSING,
        "https://www.planning.nsw.gov.au/data/nsw-da-processing-2026.csv",
    )
    .await;

    let rows = snapshot_rows(artifact, blob_store, "state_planning.nsw_da_processing").await;

    assert_eq!(rows.len(), 3);
    assert!(rows.iter().any(|row| {
        row.dimensions.get("development_type").map(String::as_str) == Some("all")
            && row.dimensions.get("metric").map(String::as_str) == Some("median_assessment_days")
            && row.value == Some(54.0)
    }));
    insta::assert_json_snapshot!(rows);
}

#[tokio::test]
async fn parses_vic_permit_activity_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        VIC_PERMIT_ACTIVITY,
        "https://www.planning.vic.gov.au/data/vic-permit-activity-2026.csv",
    )
    .await;

    let rows = snapshot_rows(artifact, blob_store, "state_planning.vic_permit_activity").await;

    assert_eq!(rows.len(), 3);
    assert!(rows.iter().any(|row| {
        row.dimensions.get("permit_type").map(String::as_str) == Some("all")
            && row.dimensions.get("metric").map(String::as_str) == Some("median_decision_days")
            && row.value == Some(60.0)
    }));
    insta::assert_json_snapshot!(rows);
}

#[tokio::test]
async fn parse_rejects_wrong_jurisdiction_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        NSW_DA_PROCESSING,
        "https://mirror.example.invalid/nsw-da-processing-2026.csv",
    )
    .await;
    let adapter = StatePlanningAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_planning.nsw_da_processing").unwrap(),
        BTreeMap::new(),
    );

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("ambiguous provenance should fail");

    assert!(
        err.to_string()
            .contains("missing state planning publication provenance"),
        "{err}"
    );
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact(Bytes::from_static(NSW_DA_PROCESSING))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different state planning artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        fetch_id: None,
        source_id: SourceId::new("state-planning").unwrap(),
        source_url: "https://www.planning.nsw.gov.au/data/nsw-da-processing-2026.csv".into(),
        content_type: "text/csv".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: NSW_DA_PROCESSING.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
    };
    let adapter = StatePlanningAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_planning.nsw_da_processing").unwrap(),
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
