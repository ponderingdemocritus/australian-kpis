use std::collections::BTreeMap;

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_state_capital::StateCapitalAdapter;
use au_kpis_domain::{ArtifactId, DataflowId, SourceId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;

const VAGO_MAJOR_PROJECTS: &[u8] = include_bytes!("fixtures/vic_major_projects_sidecar.json");
const BUDGET_CAPITAL_PAPERS: &[u8] = include_bytes!("fixtures/budget_capital_papers_sidecar.json");

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
        source_id: SourceId::new("state_capital").unwrap(),
        source_url: source_url.into(),
        content_type: "application/json".into(),
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
    let adapter = StateCapitalAdapter::default();
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
        .expect("parse state capital fixture");

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
async fn parses_vago_major_projects_sidecar_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        VAGO_MAJOR_PROJECTS,
        "https://www.audit.vic.gov.au/report/major-projects-performance-2026.json",
    )
    .await;

    let rows = snapshot_rows(artifact, blob_store, "state_capital.vic_major_projects").await;

    assert_eq!(rows.len(), 4);
    assert!(rows.iter().any(|row| {
        row.dimensions.get("project").map(String::as_str) == Some("metro_tunnel")
            && row.dimensions.get("metric").map(String::as_str) == Some("cost_overrun_pct")
            && row.value == Some(13.6725)
    }));
    assert!(
        rows.iter()
            .all(|row| row.attributes.contains_key("sidecar_reviewer"))
    );
    insta::assert_json_snapshot!("vic_major_projects", rows);
}

#[tokio::test]
async fn parses_budget_capital_papers_sidecar_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        BUDGET_CAPITAL_PAPERS,
        "https://www.budget.vic.gov.au/budget-capital-program-2026.json",
    )
    .await;

    let rows = snapshot_rows(artifact, blob_store, "state_capital.budget_capital_papers").await;

    assert_eq!(rows.len(), 4);
    assert!(rows.iter().any(|row| {
        row.dimensions.get("category").map(String::as_str) == Some("transport")
            && row.dimensions.get("metric").map(String::as_str) == Some("capital_delivery_pct")
            && row.value == Some(94.2857)
    }));
    insta::assert_json_snapshot!("budget_capital_papers", rows);
}

#[tokio::test]
async fn parse_rejects_ambiguous_project_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        VAGO_MAJOR_PROJECTS,
        "https://mirror.example.invalid/major-projects-performance-2026.json",
    )
    .await;
    let adapter = StateCapitalAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_capital.vic_major_projects").unwrap(),
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
            .contains("missing state capital publication provenance"),
        "{err}"
    );
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact(Bytes::from_static(VAGO_MAJOR_PROJECTS))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different state capital artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        source_id: SourceId::new("state_capital").unwrap(),
        source_url: "https://www.audit.vic.gov.au/report/major-projects-performance-2026.json"
            .into(),
        content_type: "application/json".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: VAGO_MAJOR_PROJECTS.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
    };
    let adapter = StateCapitalAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_capital.vic_major_projects").unwrap(),
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
