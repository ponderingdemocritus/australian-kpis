use std::collections::BTreeMap;

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_ai_readiness::AiReadinessAdapter;
use au_kpis_domain::{ArtifactId, DataflowId, SourceId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;

const OXFORD_GARI: &[u8] = include_bytes!("fixtures/oxford_gari.csv");
const NAIC_ADOPTION: &[u8] = include_bytes!("fixtures/naic_ai_adoption_tracker.csv");
const ABS_AI_RD: &[u8] = include_bytes!("fixtures/abs_ai_rd.csv");
const HOME_AFFAIRS_TALENT: &[u8] =
    include_bytes!("fixtures/home_affairs_skillselect_talent_proxy.csv");

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
        source_id: SourceId::new("ai-readiness").unwrap(),
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
    let adapter = AiReadinessAdapter::default();
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
        .expect("parse AI readiness fixture");

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
async fn parses_oxford_gari_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        OXFORD_GARI,
        "https://oxfordinsights.com/ai-readiness/gari-australia-2026.csv",
    )
    .await;

    let rows = snapshot_rows(artifact, blob_store, "oxford.gari").await;

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| {
        row.dimensions.get("country").map(String::as_str) == Some("AUS")
            && row.measure_id == "ai_readiness_score"
            && row.value == Some(73.5)
    }));
    insta::assert_json_snapshot!(rows);
}

#[tokio::test]
async fn parses_naic_ai_adoption_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        NAIC_ADOPTION,
        "https://www.industry.gov.au/data/naic-ai-adoption-tracker-2026.csv",
    )
    .await;

    let rows = snapshot_rows(artifact, blob_store, "naic.ai_adoption_tracker").await;

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| {
        row.dimensions.get("segment").map(String::as_str) == Some("all")
            && row.measure_id == "adoption_rate_pct"
            && row.value == Some(42.0)
    }));
    insta::assert_json_snapshot!(rows);
}

#[tokio::test]
async fn parses_abs_ai_rd_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        ABS_AI_RD,
        "https://www.abs.gov.au/statistics/research-and-development/abs-ai-rd-2026.csv",
    )
    .await;

    let rows = snapshot_rows(artifact, blob_store, "abs.ai_rd").await;

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| {
        row.dimensions.get("sector").map(String::as_str) == Some("all")
            && row.dimensions.get("metric").map(String::as_str) == Some("ai_rd_spend_m")
            && row.value == Some(1850.0)
    }));
    insta::assert_json_snapshot!(rows);
}

#[tokio::test]
async fn parses_home_affairs_skillselect_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        HOME_AFFAIRS_TALENT,
        "https://immi.homeaffairs.gov.au/reports/home-affairs-skillselect-talent-proxy-2026.csv",
    )
    .await;

    let rows = snapshot_rows(
        artifact,
        blob_store,
        "home_affairs.skillselect_talent_proxy",
    )
    .await;

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| {
        row.dimensions.get("occupation_group").map(String::as_str) == Some("ai_related")
            && row.dimensions.get("metric").map(String::as_str) == Some("invitations_issued")
            && row.value == Some(6400.0)
    }));
    insta::assert_json_snapshot!(rows);
}

#[tokio::test]
async fn parse_rejects_ambiguous_source_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        OXFORD_GARI,
        "https://mirror.example.invalid/gari-australia-2026.csv",
    )
    .await;
    let adapter = AiReadinessAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(DataflowId::new("oxford.gari").unwrap(), BTreeMap::new());

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("ambiguous provenance should fail");

    assert!(
        err.to_string()
            .contains("missing AI readiness publication provenance"),
        "{err}"
    );
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact(Bytes::from_static(OXFORD_GARI))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different AI readiness artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        fetch_id: None,
        source_id: SourceId::new("ai-readiness").unwrap(),
        source_url: "https://oxfordinsights.com/ai-readiness/gari-australia-2026.csv".into(),
        content_type: "text/csv".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: OXFORD_GARI.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 6, 22, 0, 0, 0).unwrap(),
    };
    let adapter = AiReadinessAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 22, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(DataflowId::new("oxford.gari").unwrap(), BTreeMap::new());

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
