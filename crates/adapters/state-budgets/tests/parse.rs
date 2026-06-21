use std::{collections::BTreeMap, str};

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_state_budgets::{NswBudgetAdapter, QldBudgetAdapter, VicBudgetAdapter};
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

const BP1_2025_26: &[u8] = b"%PDF-1.7\n% nsw budget fixture 2025-26\n%%EOF\n";
const BP1_2024_25: &[u8] = b"%PDF-1.7\n% nsw budget fixture 2024-25\n%%EOF\n";
const VIC_BP5_2026_27: &[u8] = b"%PDF-1.7\n% vic budget fixture 2026-27\n%%EOF\n";
const QLD_BP2_2025_26: &[u8] = b"%PDF-1.7\n% qld budget fixture 2025-26\n%%EOF\n";

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
        source_id: SourceId::new("state-budgets").unwrap(),
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
    expected_first_page: u64,
    expected_last_page: u64,
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
        assert_eq!(json["source_id"], "state-budgets");
        assert_eq!(json["artifact_date"], expected_artifact_date);
        assert_eq!(json["strategy"], "deterministic");
        let pages = json["pages"].as_array().expect("extract request pages");
        assert_eq!(
            pages.first().and_then(Value::as_u64),
            Some(expected_first_page)
        );
        assert_eq!(
            pages.last().and_then(Value::as_u64),
            Some(expected_last_page)
        );
        assert!(
            pages.len() <= 125,
            "state budget extraction should use bounded page windows: {json}"
        );
        assert_eq!(
            pages.len() as u64,
            expected_last_page - expected_first_page + 1,
            "state budget extraction pages should be contiguous: {json}"
        );

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
            "page": 32,
            "bbox": [10.0, 20.0, 500.0, 700.0],
            "cells": rows,
            "spans": [],
            "diagnostics": {"fixture": "nsw-budget"}
        }]
    })
    .to_string()
}

fn parse_metadata(case: FixtureCase) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("artifact_date".into(), case.artifact_date.into()),
        ("attribution".into(), "Source: NSW Treasury".into()),
        ("budget_year".into(), case.budget_year.into()),
        ("jurisdiction".into(), "NSW".into()),
        (
            "license".into(),
            "Creative Commons Attribution 3.0 Australia Licence".into(),
        ),
        (
            "license_url".into(),
            "https://creativecommons.org/licenses/by/3.0/au/".into(),
        ),
        ("paper".into(), "Budget Paper No. 1".into()),
        ("paper_slug".into(), "bp1-budget-statement".into()),
        (
            "schema_drift_policy".into(),
            "hash-pdf-table-candidates".into(),
        ),
        ("title".into(), "Budget Statement".into()),
    ])
}

fn vic_parse_metadata() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("artifact_date".into(), "2026-05-05".into()),
        (
            "attribution".into(),
            "© Copyright State Government of Victoria".into(),
        ),
        ("budget_year".into(), "2026-27".into()),
        ("jurisdiction".into(), "VIC".into()),
        (
            "license".into(),
            "Creative Commons Attribution 4.0 International licence".into(),
        ),
        (
            "license_url".into(),
            "https://creativecommons.org/licenses/by/4.0/".into(),
        ),
        ("paper".into(), "Budget Paper No. 5".into()),
        ("paper_slug".into(), "bp5-statement-of-finances".into()),
        (
            "schema_drift_policy".into(),
            "hash-pdf-table-candidates".into(),
        ),
        ("title".into(), "Statement of Finances".into()),
    ])
}

fn qld_parse_metadata() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("artifact_date".into(), "2025-06-24".into()),
        (
            "attribution".into(),
            "© The State of Queensland 2025 (Queensland Treasury)".into(),
        ),
        ("budget_year".into(), "2025-26".into()),
        ("jurisdiction".into(), "QLD".into()),
        ("license".into(), "Queensland Treasury copyright".into()),
        (
            "license_url".into(),
            "https://www.treasury.qld.gov.au/legal/copyright/".into(),
        ),
        ("paper".into(), "Budget Paper No. 2".into()),
        ("paper_slug".into(), "bp2-budget-strategy-outlook".into()),
        (
            "schema_drift_policy".into(),
            "hash-pdf-table-candidates".into(),
        ),
        ("title".into(), "Budget Strategy and Outlook".into()),
    ])
}

async fn snapshot_fixture(case: FixtureCase, blob_store: BlobStore) -> FixtureSnapshot {
    let artifact = artifact_for(&blob_store, case.bytes, case.source_url).await;
    let sidecar_url = serve_sidecar_once(
        artifact.storage_key.clone(),
        case.artifact_date,
        1,
        80,
        sidecar_response(&artifact.storage_key, case.cells),
    )
    .await;
    let adapter = NswBudgetAdapter::builder()
        .pdf_base_url(sidecar_url)
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_budgets.nsw_budget").unwrap(),
        parse_metadata(case),
    );
    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse NSW budget fixture through sidecar");

    let observation_count = rows.len();
    let series_count = rows
        .iter()
        .map(|(series, _)| series.series_key)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let first_rows: Vec<SnapshotRow> = rows
        .into_iter()
        .take(10)
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

async fn vic_snapshot_fixture(blob_store: BlobStore, cells: &[&[&str]]) -> FixtureSnapshot {
    let source_url = "https://s3.ap-southeast-2.amazonaws.com/vicbudgetfiles2026.27vicbudget/2026-27+State+Budget+-+Statement+of+Finances.pdf";
    let artifact = artifact_for(&blob_store, VIC_BP5_2026_27, source_url).await;
    let sidecar_url = serve_sidecar_once(
        artifact.storage_key.clone(),
        "2026-05-05",
        14,
        14,
        sidecar_response(&artifact.storage_key, cells),
    )
    .await;
    let adapter = VicBudgetAdapter::builder()
        .pdf_base_url(sidecar_url)
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_budgets.vic_budget").unwrap(),
        vic_parse_metadata(),
    );
    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse VIC budget fixture through sidecar");

    let observation_count = rows.len();
    let series_count = rows
        .iter()
        .map(|(series, _)| series.series_key)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let first_rows: Vec<SnapshotRow> = rows
        .into_iter()
        .take(10)
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

async fn qld_snapshot_fixture(blob_store: BlobStore, cells: &[&[&str]]) -> FixtureSnapshot {
    let source_url =
        "https://budget.qld.gov.au/files/Budget-2025-26-BP2-Budget-Strategy-Outlook.pdf";
    let artifact = artifact_for(&blob_store, QLD_BP2_2025_26, source_url).await;
    let sidecar_url = serve_sidecar_once(
        artifact.storage_key.clone(),
        "2025-06-24",
        113,
        113,
        sidecar_response(&artifact.storage_key, cells),
    )
    .await;
    let adapter = QldBudgetAdapter::builder()
        .pdf_base_url(sidecar_url)
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_budgets.qld_budget").unwrap(),
        qld_parse_metadata(),
    );
    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse QLD budget fixture through sidecar");

    let observation_count = rows.len();
    let series_count = rows
        .iter()
        .map(|(series, _)| series.series_key)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let first_rows: Vec<SnapshotRow> = rows
        .into_iter()
        .take(10)
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
async fn parses_nsw_budget_pdf_fixtures_through_sidecar_contract() {
    let blob_store = BlobStore::new(InMemory::new());
    let fixtures = [
        FixtureCase {
            name: "nsw_bp1_key_aggregates_2025_26",
            bytes: BP1_2025_26,
            source_url: "https://www.budget.nsw.gov.au/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf",
            budget_year: "2025-26",
            artifact_date: "2025-06-24",
            cells: &[
                &["Table 1.1: Key fiscal aggregates ($m)", "", "", "", ""],
                &[
                    "Fiscal aggregate",
                    "2024-25 Revised",
                    "2025-26 Budget",
                    "2026-27 Forward Estimates",
                    "2027-28 Forward Estimates",
                ],
                &["Revenue", "121,870", "125,031", "129,490", "134,860"],
                &["Expenses", "125,224", "128,419", "131,660", "135,144"],
            ],
        },
        FixtureCase {
            name: "nsw_bp1_key_aggregates_2024_25",
            bytes: BP1_2024_25,
            source_url: "https://www.budget.nsw.gov.au/sites/default/files/2024-06/bp1-budget-statement-nsw-budget-2024-25.pdf",
            budget_year: "2024-25",
            artifact_date: "2024-06-18",
            cells: &[
                &["Table 1.1: Key fiscal aggregates ($m)", "", "", "", ""],
                &[
                    "Fiscal aggregate",
                    "2023-24 Revised",
                    "2024-25 Budget",
                    "2025-26 Forward Estimates",
                    "2026-27 Forward Estimates",
                ],
                &["Revenue", "116,218", "119,303", "123,977", "128,888"],
                &["Expenses", "119,519", "122,109", "126,740", "130,322"],
            ],
        },
    ];

    for case in fixtures {
        let snapshot = snapshot_fixture(case, blob_store.clone()).await;
        assert!(snapshot.observation_count > 0);
        insta::assert_json_snapshot!(case.name, snapshot);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parses_current_nsw_budget_split_header_aggregates_table() {
    let blob_store = BlobStore::new(InMemory::new());
    let case = FixtureCase {
        name: "nsw_bp1_key_budget_aggregates_2025_26_current_shape",
        bytes: BP1_2025_26,
        source_url: "https://www.nsw.gov.au/sites/default/files/noindex/2026-03/bp1-budget-statement-nsw-budget-2025-26.pdf",
        budget_year: "2025-26",
        artifact_date: "2026-03-20",
        cells: &[
            &["Government businesses.", "", "", "", "", "", ""],
            &[
                "Table 1.2:",
                "Key budget aggregates for the general government sector",
                "",
                "",
                "",
                "",
                "",
            ],
            &[
                "", "2023-24", "2024-25", "2025-26", "2026-27", "2027-28", "2028-29",
            ],
            &[
                "",
                "Actual",
                "Revised",
                "Budget",
                "",
                "Forward Estimates",
                "",
            ],
            &[
                "Revenue ($m)",
                "110,219",
                "118,090",
                "124,154",
                "128,038",
                "133,232",
                "137,135",
            ],
            &[
                "Per cent of GSP",
                "13.4",
                "13.9",
                "14.1",
                "13.9",
                "13.8",
                "13.5",
            ],
            &[
                "Expenses ($m)",
                "120,909",
                "123,805",
                "127,581",
                "129,186",
                "132,101",
                "136,078",
            ],
            &[
                "Per cent of GSP",
                "14.7",
                "14.5",
                "14.5",
                "14.0",
                "13.6",
                "13.4",
            ],
            &[
                "", "(10,690)", "(5,715)", "(3,427)", "(1,148)", "1,132", "1,058",
            ],
            &["Budget result ($m)", "", "", "", "", "", ""],
            &[
                "Per cent of GSP",
                "(1.3)",
                "(0.7)",
                "(0.4)",
                "(0.1)",
                "0.1",
                "0.1",
            ],
            &[
                "Gross debt ($m)",
                "154,276",
                "166,012",
                "178,755",
                "188,340",
                "193,609",
                "199,680",
            ],
            &[
                "Per cent of GSP",
                "18.8",
                "19.5",
                "20.3",
                "20.4",
                "20.0",
                "19.6",
            ],
        ],
    };

    let snapshot = snapshot_fixture(case, blob_store).await;

    assert_eq!(snapshot.observation_count, 48);
    assert_eq!(snapshot.series_count, 8);
    insta::assert_json_snapshot!(case.name, snapshot);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parses_vic_budget_pdf_fixture_through_sidecar_contract() {
    let blob_store = BlobStore::new(InMemory::new());
    let cells: &[&[&str]] = &[
        &[
            "Table 1.1: Estimated financial statements for the general government sector ($ million)",
            "",
            "",
            "",
            "",
        ][..],
        &[
            "Line item",
            "2025-26 Revised",
            "2026-27 Budget",
            "2027-28 Forward Estimate",
            "2028-29 Forward Estimate",
        ][..],
        &["Revenue", "97,522", "101,402", "105,888", "110,345"][..],
        &["Expenses", "102,467", "104,910", "108,001", "111,220"][..],
    ];

    let snapshot = vic_snapshot_fixture(blob_store, cells).await;

    assert!(snapshot.observation_count > 0);
    insta::assert_json_snapshot!("vic_bp5_statement_of_finances_2026_27", snapshot);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parses_current_vic_budget_operating_statement_shape() {
    let blob_store = BlobStore::new(InMemory::new());
    let cells: &[&[&str]] = &[
        &["COMPREHENSIVE OPERATING STATEMENT", "", "", "", ""][..],
        &[
            "For the financial year ending 30 June",
            "",
            "",
            "",
            "($ million)",
        ][..],
        &["", "2026-27", "2027-28", "2028-29", "2029-30"][..],
        &["Notes", "budget", "estimate", "estimate", "estimate"][..],
        &["Revenue and income from transactions", "", "", "", ""][..],
        &["Taxation \n1.2.1", "43 179", "46 345", "47 960", "50 175"][..],
        &["Interest income", "1 636", "1 474", "1 455", "1 446"][..],
        &[
            "Total revenue and income from transactions",
            "115 564",
            "117 059",
            "121 022",
            "125 358",
        ][..],
        &["Expenses from transactions", "", "", "", ""][..],
        &[
            "Total expenses from transactions \n1.3.7",
            "114 516",
            "115 194",
            "119 085",
            "123 386",
        ][..],
        &[
            "Net result from transactions – Net operating balance",
            "1 048",
            "1 864",
            "1 936",
            "1 972",
        ][..],
    ];

    let snapshot = vic_snapshot_fixture(blob_store, cells).await;

    assert_eq!(snapshot.observation_count, 20);
    assert_eq!(snapshot.series_count, 5);
    insta::assert_json_snapshot!("vic_bp5_current_operating_statement_2026_27", snapshot);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parses_qld_budget_pdf_fixture_through_sidecar_contract() {
    let blob_store = BlobStore::new(InMemory::new());
    let cells: &[&[&str]] = &[
        &[
            "Table 8.1: General Government Sector Operating Statement ($ million)",
            "",
            "",
            "",
            "",
            "",
        ][..],
        &[
            "Section item",
            "2024-25 Estimated Actual $ million",
            "2025-26 Budget $ million",
            "2026-27 Projection $ million",
            "2027-28 Projection $ million",
            "2028-29 Projection $ million",
        ][..],
        &[
            "Revenue", "96,230", "98,442", "101,118", "104,009", "106,774",
        ][..],
        &[
            "Expenses", "103,601", "107,014", "109,290", "111,531", "113,802",
        ][..],
    ];

    let snapshot = qld_snapshot_fixture(blob_store, cells).await;

    assert!(snapshot.observation_count > 0);
    insta::assert_json_snapshot!("qld_bp2_budget_strategy_outlook_2025_26", snapshot);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parses_current_qld_budget_split_title_operating_statement_shape() {
    let blob_store = BlobStore::new(InMemory::new());
    let cells: &[&[&str]] = &[
        &[
            "Table 8.1",
            "General Government Sector Operating Statement1",
            "",
            "",
            "",
            "",
            "",
            "",
        ][..],
        &[
            "",
            "2023–24",
            "2024–25",
            "2024–25",
            "2025–26",
            "2026–27",
            "2027–28",
            "2028–29",
        ][..],
        &[
            "",
            "Outcome",
            "Budget",
            "Est.Actual",
            "Budget",
            "Projection",
            "Projection",
            "Projection",
        ][..],
        &[
            "",
            "$ million",
            "$ million",
            "$ million",
            "$ million",
            "$ million",
            "$ million",
            "$ million",
        ][..],
        &["Revenue from Transactions", "", "", "", "", "", "", ""][..],
        &[
            "Taxation revenue",
            "22,659",
            "24,799",
            "25,015",
            "26,907",
            "28,723",
            "30,442",
            "32,154",
        ][..],
        &[
            "Grants revenue",
            "42,254",
            "41,994",
            "43,598",
            "45,398",
            "46,275",
            "47,849",
            "49,741",
        ][..],
        &[
            "Sales of goods and services",
            "7,651",
            "8,351",
            "8,494",
            "8,785",
            "8,966",
            "9,098",
            "9,296",
        ][..],
        &[
            "Plus      Change in inventories",
            "(3)",
            "38",
            "(3)",
            "164",
            "..",
            "16",
            "(18)",
        ][..],
    ];

    let snapshot = qld_snapshot_fixture(blob_store, cells).await;

    assert_eq!(snapshot.observation_count, 24);
    assert_eq!(snapshot.series_count, 4);
    insta::assert_json_snapshot!("qld_bp2_current_operating_statement_2025_26", snapshot);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_nsw_schema_hash_drift() {
    let blob_store = BlobStore::new(InMemory::new());
    let case = FixtureCase {
        name: "nsw_bp1_key_aggregates_2025_26",
        bytes: BP1_2025_26,
        source_url: "https://www.budget.nsw.gov.au/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf",
        budget_year: "2025-26",
        artifact_date: "2025-06-24",
        cells: &[
            &["Table 1.1: Key fiscal aggregates ($m)", "", "", "", ""],
            &[
                "Fiscal aggregate",
                "2024-25 Actual",
                "2025-26 Budget",
                "2026-27 Forward Estimates",
                "2027-28 Forward Estimates",
            ],
            &["Revenue", "121,870", "125,031", "129,490", "134,860"],
        ],
    };
    let artifact = artifact_for(&blob_store, case.bytes, case.source_url).await;
    let sidecar_url = serve_sidecar_once(
        artifact.storage_key.clone(),
        case.artifact_date,
        1,
        80,
        sidecar_response(&artifact.storage_key, case.cells),
    )
    .await;
    let adapter = NswBudgetAdapter::builder()
        .pdf_base_url(sidecar_url)
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_budgets.nsw_budget").unwrap(),
        parse_metadata(case),
    );

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("schema drift should fail");

    assert!(err.to_string().contains("schema hash drift"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_vic_schema_hash_drift() {
    let blob_store = BlobStore::new(InMemory::new());
    let source_url = "https://s3.ap-southeast-2.amazonaws.com/vicbudgetfiles2026.27vicbudget/2026-27+State+Budget+-+Statement+of+Finances.pdf";
    let artifact = artifact_for(&blob_store, VIC_BP5_2026_27, source_url).await;
    let cells: &[&[&str]] = &[
        &[
            "Table 1.1: Estimated financial statements for the general government sector ($ million)",
            "",
            "",
            "",
            "",
        ][..],
        &[
            "Line item",
            "2025-26 Actual",
            "2026-27 Budget",
            "2027-28 Forward Estimate",
            "2028-29 Forward Estimate",
        ][..],
        &["Revenue", "97,522", "101,402", "105,888", "110,345"][..],
    ];
    let sidecar_url = serve_sidecar_once(
        artifact.storage_key.clone(),
        "2026-05-05",
        14,
        14,
        sidecar_response(&artifact.storage_key, cells),
    )
    .await;
    let adapter = VicBudgetAdapter::builder()
        .pdf_base_url(sidecar_url)
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_budgets.vic_budget").unwrap(),
        vic_parse_metadata(),
    );

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("schema drift should fail");

    assert!(err.to_string().contains("schema hash drift"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_qld_schema_hash_drift() {
    let blob_store = BlobStore::new(InMemory::new());
    let source_url =
        "https://budget.qld.gov.au/files/Budget-2025-26-BP2-Budget-Strategy-Outlook.pdf";
    let artifact = artifact_for(&blob_store, QLD_BP2_2025_26, source_url).await;
    let cells: &[&[&str]] = &[
        &[
            "Table 8.1: General Government Sector Operating Statement ($ million)",
            "",
            "",
            "",
            "",
            "",
        ][..],
        &[
            "Section item",
            "2024-25 Actual $ million",
            "2025-26 Budget $ million",
            "2026-27 Projection $ million",
            "2027-28 Projection $ million",
            "2028-29 Projection $ million",
        ][..],
        &[
            "Revenue", "96,230", "98,442", "101,118", "104,009", "106,774",
        ][..],
    ];
    let sidecar_url = serve_sidecar_once(
        artifact.storage_key.clone(),
        "2025-06-24",
        113,
        113,
        sidecar_response(&artifact.storage_key, cells),
    )
    .await;
    let adapter = QldBudgetAdapter::builder()
        .pdf_base_url(sidecar_url)
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_budgets.qld_budget").unwrap(),
        qld_parse_metadata(),
    );

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("schema drift should fail");

    assert!(err.to_string().contains("schema hash drift"), "{err}");
}

#[tokio::test]
async fn parse_rejects_ambiguous_nsw_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let mut artifact = artifact_for(
        &blob_store,
        BP1_2025_26,
        "https://mirror.example.invalid/bp1-budget-statement-nsw-budget-2025-26.pdf",
    )
    .await;
    artifact.source_id = SourceId::new("abs").unwrap();

    let adapter = NswBudgetAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let case = FixtureCase {
        name: "nsw_bp1_key_aggregates_2025_26",
        bytes: BP1_2025_26,
        source_url: "https://www.budget.nsw.gov.au/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf",
        budget_year: "2025-26",
        artifact_date: "2025-06-24",
        cells: &[],
    };
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_budgets.nsw_budget").unwrap(),
        parse_metadata(case),
    );
    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("invalid provenance should fail");

    assert!(
        err.to_string()
            .contains("NSW budget parse received artifact for source")
    );
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact(Bytes::from_static(BP1_2025_26))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different NSW budget artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        source_id: SourceId::new("state-budgets").unwrap(),
        source_url: "https://www.budget.nsw.gov.au/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf".into(),
        content_type: "application/pdf".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: BP1_2025_26.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
    };
    let adapter = NswBudgetAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let case = FixtureCase {
        name: "nsw_bp1_key_aggregates_2025_26",
        bytes: BP1_2025_26,
        source_url: "https://www.budget.nsw.gov.au/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf",
        budget_year: "2025-26",
        artifact_date: "2025-06-24",
        cells: &[],
    };
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_budgets.nsw_budget").unwrap(),
        parse_metadata(case),
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
    let case = FixtureCase {
        name: "nsw_bp1_key_aggregates_2025_26",
        bytes: BP1_2025_26,
        source_url: "https://www.budget.nsw.gov.au/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf",
        budget_year: "2025-26",
        artifact_date: "2025-06-24",
        cells: &[&["Fiscal aggregate", "2025-26 Budget"], &["Revenue", "1.0"]],
    };
    let artifact = artifact_for(&blob_store, case.bytes, case.source_url).await;
    let response = sidecar_response(
        "artifacts/not-the-requested-artifact",
        &[&["Fiscal aggregate", "2025-26 Budget"], &["Revenue", "1.0"]],
    );
    let sidecar_url = serve_sidecar_once(
        artifact.storage_key.clone(),
        case.artifact_date,
        1,
        80,
        response,
    )
    .await;
    let adapter = NswBudgetAdapter::builder()
        .pdf_base_url(sidecar_url)
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 5, 27, 1, 0, 0).unwrap(),
    )
    .with_expected_dataflow(
        DataflowId::new("state_budgets.nsw_budget").unwrap(),
        parse_metadata(case),
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
