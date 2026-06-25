use std::{
    collections::BTreeMap,
    io::{Cursor, Write},
};

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_aemo::AemoAdapter;
use au_kpis_domain::{ArtifactId, DataflowId, SourceId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

const DISPATCH_CSV: &str = r#"C,NEMP.WORLD,DISPATCHIS,AEMO,PUBLIC,2026/06/19,17:00:11,0000000523261987,DISPATCHIS,0000000523261986
I,DISPATCH,PRICE,5,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,RRP,EEP,ROP,APCFLAG,MARKETSUSPENDEDFLAG,LASTCHANGED,PRICE_STATUS
D,DISPATCH,PRICE,5,"2026/06/19 17:05:00",1,NSW1,20260619157,0,80.56426,0,80.56426,0,0,"2026/06/19 17:00:06",FIRM
D,DISPATCH,PRICE,5,"2026/06/19 17:05:00",1,QLD1,20260619157,0,76.72819,0,76.72819,0,0,"2026/06/19 17:00:06",FIRM
I,DISPATCH,REGIONSUM,9,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,TOTALDEMAND,AVAILABLEGENERATION,LASTCHANGED
D,DISPATCH,REGIONSUM,9,"2026/06/19 17:05:00",1,NSW1,20260619157,0,9576.41,14826.69812,"2026/06/19 17:00:06"
D,DISPATCH,REGIONSUM,9,"2026/06/19 17:05:00",1,QLD1,20260619157,0,7372.54,11696.77762,"2026/06/19 17:00:06"
"#;

const GENERATION_MIX_CSV: &str = r#"C,NEMWEB,FUELMIX,AEMO,PUBLIC,2026/06/19,17:05:11,0000000523261987,FUELMIX,0000000523261986
I,FUELMIX,FUELREGION,1,SETTLEMENTDATE,REGIONID,FUELTYPE,GENERATIONMW,LASTCHANGED
D,FUELMIX,FUELREGION,1,"2026/06/19 17:05:00",NSW1,black_coal,4312.5,"2026/06/19 17:05:06"
D,FUELMIX,FUELREGION,1,"2026/06/19 17:05:00",NSW1,wind,1234.25,"2026/06/19 17:05:06"
"#;

const NEXT_DAY_ACTUAL_GEN_CSV: &str = r#"C,NEMP.WORLD,METER_DATA,AEMO,PUBLIC,2026/06/19,17:05:11,0000000523261987,NEXTDAYACTUALGEN,0000000523261986
I,METER_DATA,GEN_DUID,1,INTERVAL_DATETIME,DUID,MWH_READING,LASTCHANGED
D,METER_DATA,GEN_DUID,1,"2026/06/19 17:05:00",CAPTL_WF,10,"2026/06/19 17:05:06"
D,METER_DATA,GEN_DUID,1,"2026/06/19 17:05:00",BARCSF1,50,"2026/06/19 17:05:06"
D,METER_DATA,GEN_DUID,1,"2026/06/19 17:10:00",CULLRGWF,-0.06,"2026/06/19 17:10:06"
D,METER_DATA,GEN_DUID,1,"2026/06/19 17:10:00",WAUBRAWF,2.5,"2026/06/19 17:10:06"
"#;

const DISPATCHABILITY_CAPACITY_CSV: &str = r#"C,NEMWEB,DISPATCHCAPACITY,AEMO,PUBLIC,2026/06/19,17:05:11,0000000523261987,DISPATCHCAPACITY,0000000523261986
I,DISPATCH,CAPACITY,1,SETTLEMENTDATE,REGIONID,AVAILABLEGENERATION,DISPATCHABLECAPACITY,NETINTERCHANGE,LASTCHANGED
D,DISPATCH,CAPACITY,1,"2026/06/19 17:05:00",NSW1,14826.69812,13100.5,500.0,"2026/06/19 17:05:06"
"#;

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
    zip_fixture_for(
        "PUBLIC_DISPATCHIS_202606191705_0000000523261987.CSV",
        DISPATCH_CSV,
    )
}

fn generation_mix_zip_fixture() -> Vec<u8> {
    zip_fixture_for(
        "PUBLIC_FUEL_MIX_202606191705_0000000523261987.CSV",
        GENERATION_MIX_CSV,
    )
}

fn next_day_actual_gen_zip_fixture() -> Vec<u8> {
    zip_fixture_for(
        "PUBLIC_NEXT_DAY_ACTUAL_GEN_20260619_0000000523261987.CSV",
        NEXT_DAY_ACTUAL_GEN_CSV,
    )
}

fn dispatchability_capacity_zip_fixture() -> Vec<u8> {
    zip_fixture_for(
        "PUBLIC_DISPATCHCAPACITY_202606191705_0000000523261987.CSV",
        DISPATCHABILITY_CAPACITY_CSV,
    )
}

fn zip_fixture_for(file_name: &str, csv: &str) -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut zip = ZipWriter::new(&mut cursor);
        zip.start_file(
            file_name,
            SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
        )
        .expect("start zip member");
        zip.write_all(csv.as_bytes()).expect("write zip member");
        zip.finish().expect("finish zip");
    }
    cursor.into_inner()
}

async fn artifact_for(blob_store: &BlobStore, bytes: Vec<u8>) -> ArtifactRef {
    artifact_for_url(
        blob_store,
        bytes,
        "https://www.nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202606191705_0000000523261987.zip",
    )
    .await
}

async fn artifact_for_url(blob_store: &BlobStore, bytes: Vec<u8>, source_url: &str) -> ArtifactRef {
    let id = blob_store
        .put_artifact(Bytes::from(bytes.clone()))
        .await
        .expect("store fixture artifact");
    ArtifactRef {
        id,
        fetch_id: None,
        source_id: SourceId::new("aemo").unwrap(),
        source_url: source_url.into(),
        content_type: "application/zip".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 6, 19, 7, 5, 0).unwrap(),
    }
}

#[tokio::test]
async fn parses_aemo_dispatch_zip_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(&blob_store, zip_fixture()).await;
    let adapter = AemoAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 6, 0).unwrap(),
    );

    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse AEMO fixture");

    let snapshot = rows
        .into_iter()
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
        .collect::<Vec<_>>();

    insta::assert_json_snapshot!(snapshot);
}

#[tokio::test]
async fn parses_dispatchis_available_generation_as_capacity_proxy() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(&blob_store, zip_fixture()).await;
    let adapter = AemoAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 6, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("aemo.dispatchability_capacity").unwrap(),
        BTreeMap::new(),
    );

    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse AEMO DispatchIS capacity proxy fixture");

    assert_eq!(rows.len(), 2);
    let parsed = rows
        .iter()
        .map(|(series, observation)| {
            let dimensions = series
                .dimensions
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<BTreeMap<_, _>>();
            (
                series.dataflow_id.as_str(),
                dimensions["region"],
                dimensions["metric"],
                observation.value,
                observation.attributes["aemo_table"].as_str(),
                observation.attributes["aemo_field"].as_str(),
                observation.attributes["proxy_source_dataflow"].as_str(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        parsed,
        vec![
            (
                "aemo.dispatchability_capacity",
                "NSW1",
                "available_generation",
                Some(14826.69812),
                "REGIONSUM",
                "AVAILABLEGENERATION",
                "aemo.dispatch",
            ),
            (
                "aemo.dispatchability_capacity",
                "QLD1",
                "available_generation",
                Some(11696.77762),
                "REGIONSUM",
                "AVAILABLEGENERATION",
                "aemo.dispatch",
            ),
        ]
    );
}

#[tokio::test]
async fn parses_aemo_generation_mix_zip_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for_url(
        &blob_store,
        generation_mix_zip_fixture(),
        "https://www.nemweb.com.au/Reports/CURRENT/FuelMix/PUBLIC_FUEL_MIX_202606191705_0000000523261987.zip",
    )
    .await;
    let adapter = AemoAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 6, 0).unwrap(),
    );

    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse AEMO generation mix fixture");

    let snapshot = rows
        .into_iter()
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
        .collect::<Vec<_>>();

    insta::assert_json_snapshot!(snapshot);
}

#[tokio::test]
async fn parses_next_day_actual_gen_as_generation_mix_proxy() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for_url(
        &blob_store,
        next_day_actual_gen_zip_fixture(),
        "https://www.nemweb.com.au/Reports/CURRENT/Next_Day_Actual_Gen/PUBLIC_NEXT_DAY_ACTUAL_GEN_20260619_0000000523261987.zip",
    )
    .await;
    let adapter = AemoAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 6, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("aemo.generation_mix").unwrap(),
        BTreeMap::new(),
    );

    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse AEMO Next Day Actual Gen proxy fixture");

    let parsed = rows
        .iter()
        .map(|(series, observation)| {
            let dimensions = series
                .dimensions
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<BTreeMap<_, _>>();
            (
                series.dataflow_id.as_str(),
                dimensions["region"],
                dimensions["fuel_type"],
                observation.time.to_rfc3339(),
                observation.value,
                observation.attributes["proxy_source_family"].as_str(),
                observation.attributes["aemo_table"].as_str(),
                observation.attributes["aemo_field"].as_str(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        parsed,
        vec![
            (
                "aemo.generation_mix",
                "NEM",
                "wind",
                "2026-06-19T17:05:00+00:00".to_string(),
                Some(120.0),
                "Next_Day_Actual_Gen",
                "GEN_DUID",
                "MWH_READING",
            ),
            (
                "aemo.generation_mix",
                "NEM",
                "wind",
                "2026-06-19T17:10:00+00:00".to_string(),
                Some(30.0),
                "Next_Day_Actual_Gen",
                "GEN_DUID",
                "MWH_READING",
            ),
        ]
    );
}

#[tokio::test]
async fn parses_aemo_dispatchability_capacity_zip_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for_url(
        &blob_store,
        dispatchability_capacity_zip_fixture(),
        "https://www.nemweb.com.au/Reports/CURRENT/DispatchCapacity/PUBLIC_DISPATCHCAPACITY_202606191705_0000000523261987.zip",
    )
    .await;
    let adapter = AemoAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 6, 0).unwrap(),
    );

    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse AEMO dispatchability capacity fixture");

    let snapshot = rows
        .into_iter()
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
        .collect::<Vec<_>>();

    insta::assert_json_snapshot!(snapshot);
}

#[tokio::test]
async fn parse_rejects_ambiguous_aemo_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let mut artifact = artifact_for(&blob_store, zip_fixture()).await;
    artifact.source_id = SourceId::new("asx").unwrap();

    let adapter = AemoAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 6, 0).unwrap(),
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
        fetch_id: None,
        source_id: SourceId::new("aemo").unwrap(),
        source_url: "https://www.nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202606191705_0000000523261987.zip".into(),
        content_type: "application/zip".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 6, 19, 7, 5, 0).unwrap(),
    };
    let adapter = AemoAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 6, 0).unwrap(),
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
