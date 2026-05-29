use std::collections::{BTreeMap, BTreeSet};

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_aemo::AemoAdapter;
use au_kpis_domain::{ArtifactId, SourceId, TimePrecision};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;

const DISPATCH_CSV: &[u8] = br#"C,NEMP.WORLD,DISPATCHIS,AEMO,PUBLIC,2026/05/29,11:05:12,0000000519886550,DISPATCHIS,0000000519886549
I,DISPATCH,PRICE,5,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,RRP,EEP,ROP,APCFLAG,MARKETSUSPENDEDFLAG,LASTCHANGED,PRICE_STATUS
D,DISPATCH,PRICE,5,"2026/05/29 11:10:00",1,NSW1,20260529086,0,91.89,0,91.89,0,0,"2026/05/29 11:05:08",FIRM
D,DISPATCH,PRICE,5,"2026/05/29 11:10:00",1,QLD1,20260529086,0,40.02,0,40.02,0,0,"2026/05/29 11:05:08",FIRM
I,DISPATCH,REGIONSUM,9,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,TOTALDEMAND,AVAILABLEGENERATION,AVAILABLELOAD,DEMANDFORECAST,DISPATCHABLEGENERATION,DISPATCHABLELOAD,NETINTERCHANGE,LASTCHANGED
D,DISPATCH,REGIONSUM,9,"2026/05/29 11:10:00",1,NSW1,20260529086,0,7989.37,13499.68147,1746,-29,7537.17,492.58,-944.77,"2026/05/29 11:05:08"
D,DISPATCH,REGIONSUM,9,"2026/05/29 11:10:00",1,QLD1,20260529086,0,4750.84,11744.2721,2484,-17,6683.77,1158.67,774.26,"2026/05/29 11:05:08"
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

fn zip_fixture() -> Vec<u8> {
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file(
            "PUBLIC_DISPATCHIS_202605291110_0000000519886550.CSV",
            zip::write::SimpleFileOptions::default(),
        )
        .expect("start zip file");
    std::io::Write::write_all(&mut writer, DISPATCH_CSV).expect("write zip csv");
    writer.finish().expect("finish zip").into_inner()
}

async fn artifact_for(blob_store: &BlobStore, bytes: Vec<u8>, source_url: &str) -> ArtifactRef {
    let size_bytes = bytes.len() as u64;
    let id = blob_store
        .put_artifact(Bytes::from(bytes))
        .await
        .expect("store fixture artifact");
    ArtifactRef {
        id,
        source_id: SourceId::new("aemo").unwrap(),
        source_url: source_url.into(),
        content_type: "application/zip".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 29, 1, 6, 0).unwrap(),
    }
}

async fn snapshot_fixture(artifact: ArtifactRef, blob_store: BlobStore) -> FixtureSnapshot {
    let adapter = AemoAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 29, 1, 12, 0).unwrap(),
    )
    .with_expected_dataflow(
        au_kpis_domain::DataflowId::new("aemo.dispatch").unwrap(),
        BTreeMap::from([
            ("dispatch_interval".into(), "2026-05-29T01:10:00Z".into()),
            ("published_at".into(), "2026-05-29T01:05:12Z".into()),
            ("freshness_slo_seconds".into(), "900".into()),
        ]),
    );
    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse AEMO dispatch fixture");

    assert!(
        rows.iter()
            .all(|(_, observation)| observation.time_precision == TimePrecision::Minute)
    );
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
        first_rows,
    }
}

#[tokio::test]
async fn parses_aemo_dispatch_zip_fixture_with_region_price_and_summary_rows() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        zip_fixture(),
        "https://www.nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip",
    )
    .await;

    let snapshot = snapshot_fixture(artifact, blob_store.clone()).await;

    assert_eq!(snapshot.observation_count, 8);
    assert_eq!(snapshot.series_count, 8);
    insta::assert_json_snapshot!("dispatchis_region_price_and_summary", snapshot);
}

#[tokio::test]
async fn parse_rejects_ambiguous_aemo_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let mut artifact = artifact_for(
        &blob_store,
        zip_fixture(),
        "https://mirror.example.invalid/PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip",
    )
    .await;
    artifact.source_id = SourceId::new("rba").unwrap();

    let adapter = AemoAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 29, 1, 12, 0).unwrap(),
    );
    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("invalid provenance should fail");

    assert!(
        err.to_string()
            .contains("AEMO parse received artifact for source")
    );
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let bytes = zip_fixture();
    let actual_id = blob_store
        .put_artifact(Bytes::from(bytes.clone()))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different AEMO artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        source_id: SourceId::new("aemo").unwrap(),
        source_url: "https://www.nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip".into(),
        content_type: "application/zip".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 29, 1, 6, 0).unwrap(),
    };
    let adapter = AemoAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 29, 1, 12, 0).unwrap(),
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
