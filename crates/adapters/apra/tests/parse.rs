use std::collections::{BTreeMap, BTreeSet};

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_apra::ApraAdapter;
use au_kpis_domain::{ArtifactId, DataflowId, SourceId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;

const PERFORMANCE_XLSX: &[u8] = include_bytes!("fixtures/performance.xlsx");
const CENTRALISED_XLSX: &[u8] = include_bytes!("fixtures/centralised.xlsx");
const PROPERTY_XLSX: &[u8] = include_bytes!("fixtures/property.xlsx");
const SUPER_ASSET_ALLOCATION_XLSX: &[u8] = include_bytes!("fixtures/super_asset_allocation.xlsx");

#[derive(Debug, Serialize)]
struct FixtureSnapshot {
    observation_count: usize,
    series_count: usize,
    schema_hashes: BTreeSet<String>,
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
) -> ArtifactRef {
    let id = blob_store
        .put_artifact(Bytes::from_static(bytes))
        .await
        .expect("store fixture artifact");
    ArtifactRef {
        id,
        fetch_id: None,
        source_id: SourceId::new("apra").unwrap(),
        source_url: source_url.into(),
        content_type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
    }
}

async fn snapshot_fixture(artifact: ArtifactRef, blob_store: BlobStore) -> FixtureSnapshot {
    let adapter = ApraAdapter::default();
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
        .expect("parse APRA fixture");

    let schema_hashes = rows
        .iter()
        .map(|(_, observation)| {
            observation
                .attributes
                .get("schema_hash")
                .expect("schema hash attribute")
                .clone()
        })
        .collect::<BTreeSet<_>>();
    let series_count = rows
        .iter()
        .map(|(series, _)| series.series_key)
        .collect::<BTreeSet<_>>()
        .len();
    let first_rows = rows
        .iter()
        .take(8)
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
        schema_hashes,
        first_rows,
    }
}

async fn parse_super_fixture(
    artifact: ArtifactRef,
    blob_store: BlobStore,
) -> Vec<(
    au_kpis_domain::SeriesDescriptor,
    au_kpis_domain::Observation,
)> {
    let adapter = ApraAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 28, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("apra.super_asset_allocation").unwrap(),
        BTreeMap::new(),
    );

    adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse APRA super asset allocation fixture")
}

#[tokio::test]
async fn parses_apra_quarterly_xls_fixtures_with_schema_hashes() {
    let blob_store = BlobStore::new(InMemory::new());
    let fixtures = [
        (
            "performance_xlsx",
            PERFORMANCE_XLSX,
            "https://www.apra.gov.au/sites/default/files/2026-03/Quarterly%20authorised%20deposit-taking%20institution%20performance-September%202004%20to%20December%202025.xlsx",
        ),
        (
            "centralised_xlsx",
            CENTRALISED_XLSX,
            "https://www.apra.gov.au/sites/default/files/2026-03/Authorised%20deposit-taking%20institution%20centralised%20publication%20-%20March%202013%20to%20December%202025.xlsx",
        ),
        (
            "property_xlsx",
            PROPERTY_XLSX,
            "https://www.apra.gov.au/sites/default/files/2026-03/Quarterly%20authorised%20deposit-taking%20institution%20property%20exposures%20statistics%20December%202025.xlsx",
        ),
    ];

    for (name, bytes, source_url) in fixtures {
        let artifact = artifact_for(&blob_store, bytes, source_url).await;
        let snapshot = snapshot_fixture(artifact, blob_store.clone()).await;
        assert!(snapshot.observation_count > 0);
        assert!(!snapshot.schema_hashes.is_empty());
        insta::assert_json_snapshot!(name, snapshot);
    }
}

#[tokio::test]
async fn parse_rejects_ambiguous_apra_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let mut artifact = artifact_for(
        &blob_store,
        PERFORMANCE_XLSX,
        "https://mirror.example.invalid/performance.xlsx",
    )
    .await;
    artifact.source_id = SourceId::new("rba").unwrap();

    let adapter = ApraAdapter::default();
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
            .contains("APRA parse received artifact for source")
    );
}

#[tokio::test]
async fn parse_rejects_super_asset_allocation_for_non_super_publication() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        PERFORMANCE_XLSX,
        "https://www.apra.gov.au/sites/default/files/2026-03/Quarterly%20authorised%20deposit-taking%20institution%20performance-September%202004%20to%20December%202025.xlsx",
    )
    .await;

    let adapter = ApraAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("apra.super_asset_allocation").unwrap(),
        BTreeMap::new(),
    );

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("super dataflow should reject ADI publication");

    assert!(
        err.to_string()
            .contains("super asset-allocation parse received publication"),
        "{err}"
    );
}

#[tokio::test]
async fn parses_super_asset_allocation_fixture_with_reviewed_category_mapping() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        SUPER_ASSET_ALLOCATION_XLSX,
        "https://www.apra.gov.au/sites/default/files/2026-05/Quarterly%20superannuation%20performance%20statistics%20-%20December%202004%20to%20March%202026.xlsx",
    )
    .await;

    let rows = parse_super_fixture(artifact, blob_store).await;
    let snapshot = rows
        .iter()
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
        .collect::<Vec<_>>();

    assert_eq!(snapshot.len(), 10);
    assert!(
        snapshot
            .iter()
            .all(|row| row.dataflow_id == "apra.super_asset_allocation")
    );
    assert!(snapshot.iter().all(|row| row.measure_id == "value"));
    assert!(
        snapshot
            .iter()
            .all(|row| row.attributes.contains_key("apra_mapping_review"))
    );
    assert!(snapshot.iter().any(|row| {
        row.dimensions.get("asset_category").map(String::as_str) == Some("total")
            && row.dimensions.get("mapping").map(String::as_str)
                == Some("productive_infrastructure_onshore")
            && row.time == "2026-04-01T00:00:00+00:00"
            && row.value == Some(12600.0)
    }));
    insta::assert_json_snapshot!("super_asset_allocation_mapping", snapshot);
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact(Bytes::from_static(PERFORMANCE_XLSX))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different APRA artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        fetch_id: None,
        source_id: SourceId::new("apra").unwrap(),
        source_url: "https://www.apra.gov.au/sites/default/files/2026-03/Quarterly%20authorised%20deposit-taking%20institution%20performance-September%202004%20to%20December%202025.xlsx".into(),
        content_type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: PERFORMANCE_XLSX.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
    };
    let adapter = ApraAdapter::default();
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
