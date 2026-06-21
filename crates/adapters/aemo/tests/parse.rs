use std::{
    collections::BTreeMap,
    io::{Cursor, Write},
};

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_aemo::AemoAdapter;
use au_kpis_domain::{ArtifactId, SourceId};
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
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut zip = ZipWriter::new(&mut cursor);
        zip.start_file(
            "PUBLIC_DISPATCHIS_202606191705_0000000523261987.CSV",
            SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
        )
        .expect("start zip member");
        zip.write_all(DISPATCH_CSV.as_bytes())
            .expect("write zip member");
        zip.finish().expect("finish zip");
    }
    cursor.into_inner()
}

async fn artifact_for(blob_store: &BlobStore, bytes: Vec<u8>) -> ArtifactRef {
    let id = blob_store
        .put_artifact(Bytes::from(bytes.clone()))
        .await
        .expect("store fixture artifact");
    ArtifactRef {
        id,
        source_id: SourceId::new("aemo").unwrap(),
        source_url: "https://www.nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/PUBLIC_DISPATCHIS_202606191705_0000000523261987.zip".into(),
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
