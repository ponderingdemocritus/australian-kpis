use std::{collections::BTreeMap, str};

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_treasury::TreasuryAdapter;
use au_kpis_domain::{ArtifactId, DataflowId, SourceId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const BP4_2026_27: &[u8] = b"%PDF-1.7\n% treasury budget fixture 2026-27\n%%EOF\n";
const BP4_2025_26: &[u8] = b"%PDF-1.7\n% treasury budget fixture 2025-26\n%%EOF\n";
const BP4_2024_25: &[u8] = b"%PDF-1.7\n% treasury budget fixture 2024-25\n%%EOF\n";

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

#[derive(Debug, Clone, Copy)]
struct FixtureCase {
    name: &'static str,
    bytes: &'static [u8],
    source_url: &'static str,
    budget_year: &'static str,
    artifact_date: &'static str,
    cells: &'static [&'static [&'static str]],
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
        source_id: SourceId::new("treasury").unwrap(),
        source_url: source_url.into(),
        content_type: "application/pdf".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
    }
}

async fn serve_sidecar_once(
    expected_storage_key: String,
    expected_artifact_date: &'static str,
    response_body: String,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind sidecar fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let request = read_http_request(&mut stream).await;
        assert!(request.starts_with("POST /extract HTTP/1.1"), "{request}");
        let body = request.split("\r\n\r\n").nth(1).expect("request body");
        let json: Value = serde_json::from_str(body).expect("extract request json");
        assert_eq!(json["s3_key"], expected_storage_key);
        assert_eq!(json["source_id"], "treasury");
        assert_eq!(json["artifact_date"], expected_artifact_date);
        assert_eq!(json["strategy"], "deterministic");

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write sidecar response");
    });

    format!("http://{addr}")
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    loop {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).await.expect("read request");
        assert_ne!(read, 0, "connection closed before request completed");
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(header_end) = find_header_end(&buffer) {
            let headers = str::from_utf8(&buffer[..header_end]).expect("utf8 headers");
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().expect("content length"))
                })
                .unwrap_or(0);
            if buffer.len() >= header_end + 4 + content_length {
                return String::from_utf8(buffer).expect("utf8 request");
            }
        }
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn sidecar_response(storage_key: &str, cells: &[&[&str]]) -> String {
    let rows = cells
        .iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    json!({
        "artifact_key": storage_key,
        "backend": {
            "kind": "deterministic",
            "name": "fixture-pdfplumber",
            "version": "2026.1",
            "model_sha256": null
        },
        "tables": [{
            "page": 12,
            "bbox": [10.0, 20.0, 500.0, 700.0],
            "cells": rows,
            "spans": [],
            "diagnostics": {"fixture": "treasury-budget"}
        }]
    })
    .to_string()
}

async fn snapshot_fixture(case: FixtureCase, blob_store: BlobStore) -> FixtureSnapshot {
    let artifact = artifact_for(&blob_store, case.bytes, case.source_url).await;
    let sidecar_url = serve_sidecar_once(
        artifact.storage_key.clone(),
        case.artifact_date,
        sidecar_response(&artifact.storage_key, case.cells),
    )
    .await;
    let adapter = TreasuryAdapter::builder().pdf_base_url(sidecar_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("treasury.budget_papers").unwrap(),
        BTreeMap::from([
            ("budget_year".into(), case.budget_year.into()),
            ("artifact_date".into(), case.artifact_date.into()),
            ("paper".into(), "Budget Paper No. 4".into()),
            ("paper_slug".into(), "bp4-agency-resourcing".into()),
            ("title".into(), "Agency resourcing table".into()),
        ]),
    );
    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse Treasury fixture through sidecar");

    let observation_count = rows.len();
    let series_count = rows
        .iter()
        .map(|(series, _)| series.series_key)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let first_rows: Vec<SnapshotRow> = rows
        .into_iter()
        .take(8)
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
        .collect();

    FixtureSnapshot {
        observation_count,
        series_count,
        first_rows,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parses_treasury_budget_pdf_fixtures_through_sidecar_contract() {
    let blob_store = BlobStore::new(InMemory::new());
    let fixtures = [
        FixtureCase {
            name: "bp4_agency_resourcing_2026_27",
            bytes: BP4_2026_27,
            source_url: "https://budget.gov.au/content/bp4/download/bp4_05_agency_resourcing_tables.pdf",
            budget_year: "2026-27",
            artifact_date: "2026-05-12",
            cells: &[
                &["Table 1: Agency resourcing by portfolio ($m)", "", "", ""],
                &[
                    "Agency",
                    "2024-25 Actual",
                    "2025-26 Budget",
                    "2026-27 Forward estimate",
                ],
                &["Department of the Treasury", "310.0", "325.5", "341.0"],
                &["Department of Finance", "420.0", "438.5", "456.0"],
            ],
        },
        FixtureCase {
            name: "bp4_agency_resourcing_2025_26",
            bytes: BP4_2025_26,
            source_url: "https://archive.budget.gov.au/2025-26/bp4/download/bp4_05_agency_resourcing_tables.pdf",
            budget_year: "2025-26",
            artifact_date: "2025-05-13",
            cells: &[
                &["Table 1: Agency resourcing by portfolio ($m)", "", "", ""],
                &[
                    "Agency",
                    "2023-24 Actual",
                    "2024-25 Estimated actual",
                    "2025-26 Budget",
                ],
                &["Department of the Treasury", "292.0", "305.5", "320.0"],
                &["Department of Finance", "398.0", "411.0", "429.5"],
            ],
        },
        FixtureCase {
            name: "bp4_agency_resourcing_2024_25",
            bytes: BP4_2024_25,
            source_url: "https://archive.budget.gov.au/2024-25/bp4/download/bp4_05_agency_resourcing_tables.pdf",
            budget_year: "2024-25",
            artifact_date: "2024-05-14",
            cells: &[
                &["Table 1: Agency resourcing by portfolio ($m)", "", "", ""],
                &[
                    "Agency",
                    "2022-23 Actual",
                    "2023-24 Estimated actual",
                    "2024-25 Budget",
                ],
                &["Department of the Treasury", "275.0", "288.5", "301.0"],
                &["Department of Finance", "377.0", "389.0", "404.5"],
            ],
        },
    ];

    for case in fixtures {
        let snapshot = snapshot_fixture(case, blob_store.clone()).await;
        assert!(snapshot.observation_count > 0);
        insta::assert_json_snapshot!(case.name, snapshot);
    }
}

#[tokio::test]
async fn parse_rejects_ambiguous_treasury_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let mut artifact = artifact_for(
        &blob_store,
        BP4_2026_27,
        "https://mirror.example.invalid/bp4_05_agency_resourcing_tables.pdf",
    )
    .await;
    artifact.source_id = SourceId::new("abs").unwrap();

    let adapter = TreasuryAdapter::default();
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
            .contains("Treasury parse received artifact for source")
    );
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact(Bytes::from_static(BP4_2026_27))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different Treasury artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        source_id: SourceId::new("treasury").unwrap(),
        source_url:
            "https://budget.gov.au/content/bp4/download/bp4_05_agency_resourcing_tables.pdf".into(),
        content_type: "application/pdf".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: BP4_2026_27.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
    };
    let adapter = TreasuryAdapter::default();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_sidecar_artifact_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        BP4_2026_27,
        "https://budget.gov.au/content/bp4/download/bp4_05_agency_resourcing_tables.pdf",
    )
    .await;
    let response = sidecar_response(
        "artifacts/not-the-requested-artifact",
        &[
            &["Agency", "2026-27 Budget"],
            &["Department of the Treasury", "1.0"],
        ],
    );
    let sidecar_url =
        serve_sidecar_once(artifact.storage_key.clone(), "2026-05-12", response).await;
    let adapter = TreasuryAdapter::builder().pdf_base_url(sidecar_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("treasury.budget_papers").unwrap(),
        BTreeMap::from([
            ("budget_year".into(), "2026-27".into()),
            ("artifact_date".into(), "2026-05-12".into()),
        ]),
    );

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("sidecar mismatch should fail");

    assert!(
        err.to_string().contains("sidecar returned artifact key"),
        "{err}"
    );
}
