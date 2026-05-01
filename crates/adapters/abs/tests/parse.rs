use std::{
    collections::BTreeMap,
    fmt, io,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use au_kpis_adapter::{AdapterError, AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_abs::AbsAdapter;
use au_kpis_domain::{
    ArtifactId, DataflowId, DimensionId, Observation, ObservationStatus, SeriesDescriptor,
    SeriesKey, SourceId, TimePrecision,
};
use au_kpis_error::{Classify, ErrorClass};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{FutureExt, StreamExt, stream, stream::BoxStream};
use object_store::memory::InMemory;
use object_store::{
    Attributes, Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult, UploadPart, path::Path,
};
use proptest::prelude::*;
use serde::Serialize;

const CPI_FIXTURE: &[u8] = include_bytes!("fixtures/cpi_sdmx.json");
const REORDERED_FIXTURE: &[u8] = br#"{
  "data": {
    "dataSets": [
      {
        "series": {
          "0:0": {
            "observations": {
              "0": [136.1, 0]
            }
          }
        }
      }
    ],
    "structure": {
      "dimensions": {
        "series": [
          { "id": "REGION", "values": [{ "id": "AUS" }] },
          { "id": "MEASURE", "values": [{ "id": "INDEX" }] }
        ],
        "observation": [
          { "id": "TIME_PERIOD", "values": [{ "id": "2024-Q1" }] }
        ]
      },
      "attributes": {
        "observation": [
          { "id": "OBS_STATUS", "values": [{ "id": "A" }] }
        ]
      }
    }
  }
}"#;
const EXTRA_ATTRIBUTE_FIXTURE: &[u8] = br#"{
  "data": {
    "structure": {
      "dimensions": {
        "series": [
          { "id": "REGION", "values": [{ "id": "AUS" }] },
          { "id": "MEASURE", "values": [{ "id": "INDEX" }] }
        ],
        "observation": [
          { "id": "TIME_PERIOD", "values": [{ "id": "2024-Q1" }] }
        ]
      },
      "attributes": {
        "observation": [
          { "id": "OBS_STATUS", "values": [{ "id": "A" }] }
        ]
      }
    },
    "dataSets": [
      {
        "series": {
          "0:0": {
            "observations": {
              "0": [136.1, 0, 0]
            }
          }
        }
      }
    ]
  }
}"#;
const MISSING_STRUCTURE_FIXTURE: &[u8] = br#"{
  "data": {
    "dataSets": []
  }
}"#;
const MISSING_MEASURE_FIXTURE: &[u8] = br#"{
  "data": {
    "structure": {
      "dimensions": {
        "series": [
          { "id": "REGION", "values": [{ "id": "AUS" }] }
        ],
        "observation": [
          { "id": "TIME_PERIOD", "values": [{ "id": "2024-Q1" }] }
        ]
      },
      "attributes": {
        "observation": [
          { "id": "OBS_STATUS", "values": [{ "id": "A" }] }
        ]
      }
    },
    "dataSets": [
      {
        "series": {
          "0": {
            "observations": {
              "0": [136.1, 0]
            }
          }
        }
      }
    ]
  }
}"#;
const DUPLICATE_SERIES_DIMENSION_FIXTURE: &[u8] = br#"{
  "data": {
    "structure": {
      "dimensions": {
        "series": [
          { "id": "REGION", "values": [{ "id": "AUS" }] },
          { "id": "region", "values": [{ "id": "NSW" }] },
          { "id": "MEASURE", "values": [{ "id": "INDEX" }] }
        ],
        "observation": [
          { "id": "TIME_PERIOD", "values": [{ "id": "2024-Q1" }] }
        ]
      },
      "attributes": {
        "observation": [
          { "id": "OBS_STATUS", "values": [{ "id": "A" }] }
        ]
      }
    },
    "dataSets": [
      {
        "series": {
          "0:0:0": {
            "observations": {
              "0": [136.1, 0]
            }
          }
        }
      }
    ]
  }
}"#;
const NON_TIME_OBSERVATION_DIMENSION_FIXTURE: &[u8] = br#"{
  "data": {
    "structure": {
      "dimensions": {
        "series": [
          { "id": "REGION", "values": [{ "id": "AUS" }] },
          { "id": "MEASURE", "values": [{ "id": "INDEX" }] }
        ],
        "observation": [
          { "id": "PERIOD", "values": [{ "id": "2024-Q1" }] }
        ]
      },
      "attributes": {
        "observation": [
          { "id": "OBS_STATUS", "values": [{ "id": "A" }] }
        ]
      }
    },
    "dataSets": [
      {
        "series": {
          "0:0": {
            "observations": {
              "0": [136.1, 0]
            }
          }
        }
      }
    ]
  }
}"#;
const UNKNOWN_STATUS_FIXTURE: &[u8] = br#"{
  "data": {
    "structure": {
      "dimensions": {
        "series": [
          { "id": "REGION", "values": [{ "id": "AUS" }] },
          { "id": "MEASURE", "values": [{ "id": "INDEX" }] }
        ],
        "observation": [
          { "id": "TIME_PERIOD", "values": [{ "id": "2024-Q1" }] }
        ]
      },
      "attributes": {
        "observation": [
          { "id": "OBS_STATUS", "values": [{ "id": "X" }] }
        ]
      }
    },
    "dataSets": [
      {
        "series": {
          "0:0": {
            "observations": {
              "0": [136.1, 0]
            }
          }
        }
      }
    ]
  }
}"#;
const MISSING_STATUS_WITH_VALUE_FIXTURE: &[u8] = br#"{
  "data": {
    "structure": {
      "dimensions": {
        "series": [
          { "id": "REGION", "values": [{ "id": "AUS" }] },
          { "id": "MEASURE", "values": [{ "id": "INDEX" }] }
        ],
        "observation": [
          { "id": "TIME_PERIOD", "values": [{ "id": "2024-Q1" }] }
        ]
      },
      "attributes": {
        "observation": [
          { "id": "OBS_STATUS", "values": [{ "id": "M" }] }
        ]
      }
    },
    "dataSets": [
      {
        "series": {
          "0:0": {
            "observations": {
              "0": [136.1, 0]
            }
          }
        }
      }
    ]
  }
}"#;
const LOWERCASE_STATUS_ATTRIBUTE_FIXTURE: &[u8] = br#"{
  "data": {
    "structure": {
      "dimensions": {
        "series": [
          { "id": "REGION", "values": [{ "id": "AUS" }] },
          { "id": "MEASURE", "values": [{ "id": "INDEX" }] }
        ],
        "observation": [
          { "id": "TIME_PERIOD", "values": [{ "id": "2024-Q1" }] }
        ]
      },
      "attributes": {
        "observation": [
          { "id": "obs_status", "values": [{ "id": "M" }] }
        ]
      }
    },
    "dataSets": [
      {
        "series": {
          "0:0": {
            "observations": {
              "0": [136.1, 0]
            }
          }
        }
      }
    ]
  }
}"#;

#[derive(Debug, Serialize)]
struct ParsedRow {
    series_key: String,
    dataflow_id: String,
    measure_id: String,
    dimensions: BTreeMap<String, String>,
    time: String,
    time_precision: String,
    value: Option<f64>,
    status: String,
    attributes: BTreeMap<String, String>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_streams_cpi_sdmx_observations_from_artifact() {
    let rows = parse_fixture_rows().await;

    insta::assert_json_snapshot!(rows);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_cpi_fixture_stays_under_runtime_budget() {
    let started = Instant::now();
    let rows = parse_fixture_raw(CPI_FIXTURE).await.expect("parse fixture");
    let elapsed = started.elapsed();

    assert_eq!(rows.len(), 6);
    assert!(
        elapsed < Duration::from_secs(2),
        "CPI fixture parse took {elapsed:?}, exceeding 2s budget"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_maps_null_observations_to_missing_status() {
    let rows = parse_fixture_rows().await;

    let missing = rows
        .iter()
        .find(|row| row.dimensions["region"] == "VIC" && row.time == "2024-01-01T00:00:00+00:00")
        .expect("fixture contains missing VIC observation");
    assert_eq!(missing.value, None);
    assert_eq!(missing.status, "missing");
    assert_eq!(missing.attributes["OBS_STATUS"], "M");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_emits_canonical_series_key_boundary_ids() {
    let rows = parse_fixture_raw(CPI_FIXTURE).await.expect("parse fixture");
    let (series, observation) = rows.first().expect("fixture has observations");
    let dataflow = DataflowId::new("abs.cpi").expect("static dataflow id is valid");

    assert_eq!(series.measure_id.as_str(), "index");
    assert_eq!(series.unit, "index");
    assert_eq!(
        series.dimensions[&DimensionId::new("region").expect("valid dimension")].as_str(),
        "AUS"
    );
    assert_eq!(
        series.dimensions[&DimensionId::new("measure").expect("valid dimension")].as_str(),
        "index"
    );
    assert_eq!(
        series.series_key,
        SeriesKey::derive(&dataflow, [("measure", "index"), ("region", "AUS")])
    );
    assert_eq!(observation.series_key, series.series_key);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_uses_measure_code_for_series_unit() {
    let fixture = String::from_utf8(CPI_FIXTURE.to_vec())
        .expect("fixture is utf-8")
        .replace(r#""INDEX""#, r#""PCT""#)
        .into_bytes();
    let rows = parse_fixture_owned(fixture).await.expect("parse fixture");
    let (series, _) = rows.first().expect("fixture has observations");

    assert_eq!(series.measure_id.as_str(), "pct");
    assert_eq!(series.unit, "pct");
    assert_eq!(
        series.dimensions[&DimensionId::new("measure").expect("valid dimension")].as_str(),
        "pct"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_accepts_data_sets_before_structure() {
    let rows = parse_fixture_raw(REORDERED_FIXTURE)
        .await
        .expect("parse reordered fixture");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].0.dimensions[&DimensionId::new("region").expect("valid dimension")].as_str(),
        "AUS"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_extra_observation_attribute_indexes() {
    let err = parse_fixture_raw(EXTRA_ATTRIBUTE_FIXTURE)
        .await
        .expect_err("extra observation attribute index should be format drift");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(
        err.to_string()
            .contains("observation tuple has 2 attribute indexes, expected at most 1"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_reports_sdmx_shape_drift_as_format_drift() {
    let err = parse_fixture_raw(MISSING_STRUCTURE_FIXTURE)
        .await
        .expect_err("missing structure should be format drift");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(
        err.to_string()
            .contains("ABS SDMX data is missing `structure`"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_series_without_measure_dimension() {
    let err = parse_fixture_raw(MISSING_MEASURE_FIXTURE)
        .await
        .expect_err("missing measure dimension should be format drift");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(
        err.to_string()
            .contains("ABS SDMX series structure is missing `MEASURE` dimension"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_duplicate_series_dimensions() {
    let err = parse_fixture_raw(DUPLICATE_SERIES_DIMENSION_FIXTURE)
        .await
        .expect_err("duplicate series dimension should be format drift");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(
        err.to_string()
            .contains("ABS SDMX series structure has duplicate `region` dimensions"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_non_time_observation_dimension() {
    let err = parse_fixture_raw(NON_TIME_OBSERVATION_DIMENSION_FIXTURE)
        .await
        .expect_err("non-time observation dimension should be format drift");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(
        err.to_string()
            .contains("expected ABS observation dimension `TIME_PERIOD`, got `PERIOD`"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_non_cpi_artifact_url() {
    let err = parse_fixture_owned_with_url(
        CPI_FIXTURE.to_vec(),
        "https://data.api.abs.gov.au/rest/data/ABS,WPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD",
    )
    .await
    .expect_err("non-CPI artifact should be rejected before parsing");

    assert!(matches!(err, AdapterError::Validation(_)));
    assert_eq!(err.class(), ErrorClass::Validation);
    assert!(
        err.to_string().contains("does not match CPI dataflow"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_artifact_url_without_cpi_dataflow_provenance() {
    let err = parse_fixture_owned_with_url(
        CPI_FIXTURE.to_vec(),
        "https://mirror.example.test/archive/cpi_sdmx.json",
    )
    .await
    .expect_err("mirrored artifact without dataflow provenance should be rejected");

    assert!(matches!(err, AdapterError::Validation(_)));
    assert_eq!(err.class(), ErrorClass::Validation);
    assert!(
        err.to_string().contains("missing CPI dataflow provenance"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_accepts_cpi_artifact_url_with_harmless_query_changes() {
    let rows = parse_fixture_owned_with_url(
        CPI_FIXTURE.to_vec(),
        "https://mirror.example.test/archive/data/ABS,CPI,2.0.0/all?detail=full&dimensionAtObservation=TIME_PERIOD",
    )
    .await
    .expect("parse CPI artifact with reordered query");

    assert_eq!(rows.len(), 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact_stream(stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(
            CPI_FIXTURE,
        ))]))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different ABS artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        source_id: SourceId::new("abs").expect("static source id is valid"),
        source_url: "https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD".into(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: CPI_FIXTURE.len() as u64,
        fetched_at: DateTime::parse_from_rfc3339("2024-04-24T00:00:00Z")
            .expect("valid fixture date")
            .with_timezone(&Utc),
    };

    let adapter = AbsAdapter::default();
    let ctx = ParseCtx::new(
        AdapterHttpClient::new(adapter.manifest().rate_limit),
        blob_store,
        DateTime::parse_from_rfc3339("2024-04-30T00:00:00Z")
            .expect("valid fixture date")
            .with_timezone(&Utc),
    );

    let mut rows = adapter.parse(artifact, &ctx);
    let err = rows
        .next()
        .await
        .expect("parse should emit validation error")
        .expect_err("mismatched storage key should fail");

    assert!(matches!(err, AdapterError::Validation(_)));
    assert_eq!(err.class(), ErrorClass::Validation);
    assert!(
        err.to_string().contains("does not match artifact id"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_unknown_observation_status() {
    let err = parse_fixture_raw(UNKNOWN_STATUS_FIXTURE)
        .await
        .expect_err("unknown observation status should be format drift");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(
        err.to_string().contains("unknown ABS OBS_STATUS `X`"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_treats_obs_status_attribute_case_insensitively() {
    let err = parse_fixture_raw(LOWERCASE_STATUS_ATTRIBUTE_FIXTURE)
        .await
        .expect_err("lowercase OBS_STATUS should still drive status validation");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(
        err.to_string()
            .contains("OBS_STATUS `M` cannot carry a numeric observation value"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_unsupported_cpi_time_period_shapes() {
    let fixture = String::from_utf8(CPI_FIXTURE.to_vec())
        .expect("fixture is utf-8")
        .replace(r#""2023-Q4""#, r#""2023-12-31""#)
        .into_bytes();
    let err = parse_fixture_owned(fixture)
        .await
        .expect_err("daily CPI time periods should be format drift");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(
        err.to_string()
            .contains("unsupported ABS CPI TIME_PERIOD `2023-12-31`"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_missing_status_with_numeric_value() {
    let err = parse_fixture_raw(MISSING_STATUS_WITH_VALUE_FIXTURE)
        .await
        .expect_err("missing status with numeric value should be format drift");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(
        err.to_string()
            .contains("OBS_STATUS `M` cannot carry a numeric observation value"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_preserves_transient_storage_read_classification() {
    let blob_store = BlobStore::new(FailingReadObjectStore);
    let artifact = ArtifactRef {
        id: ArtifactId::of_content(b"partial"),
        source_id: SourceId::new("abs").expect("static source id is valid"),
        source_url: "https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD".into(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::new(),
        storage_key: "cold/partial".into(),
        size_bytes: 128,
        fetched_at: DateTime::parse_from_rfc3339("2024-04-24T00:00:00Z")
            .expect("valid fixture date")
            .with_timezone(&Utc),
    };
    let adapter = AbsAdapter::default();
    let ctx = ParseCtx::new(
        AdapterHttpClient::new(adapter.manifest().rate_limit),
        blob_store,
        DateTime::parse_from_rfc3339("2024-04-30T00:00:00Z")
            .expect("valid fixture date")
            .with_timezone(&Utc),
    );

    let mut stream = adapter.parse(artifact, &ctx);
    let err = stream
        .next()
        .await
        .expect("stream yields storage failure")
        .expect_err("storage failure should fail parse");

    assert!(matches!(err, AdapterError::Storage(_)));
    assert_eq!(err.class(), ErrorClass::Transient);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_trailing_bytes_after_json_document() {
    let mut fixture = CPI_FIXTURE.to_vec();
    fixture.extend_from_slice(b" trailing");

    let err = parse_fixture_owned(fixture)
        .await
        .expect_err("trailing bytes should be rejected");

    assert!(matches!(err, AdapterError::FormatDrift(_)));
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(err.to_string().contains("trailing characters"), "{err}");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10_000))]

    #[test]
    fn series_key_is_deterministic_for_dimension_order(
        region in "[A-Z]{2,4}",
        measure in "[A-Z]{2,8}",
    ) {
        let dataflow = DataflowId::new("abs.cpi").expect("static dataflow id is valid");
        let measure = measure.to_ascii_lowercase();
        let forward = SeriesKey::derive(&dataflow, [("region", region.as_str()), ("measure", measure.as_str())]);
        let reverse = SeriesKey::derive(&dataflow, [("measure", measure.as_str()), ("region", region.as_str())]);

        prop_assert_eq!(forward, reverse);
    }

    #[test]
    fn observation_json_roundtrips(
        value in prop::option::of(-1_000_000_i32..1_000_000),
        revision_no in 0_u32..32,
    ) {
        let dataflow = DataflowId::new("abs.cpi").expect("static dataflow id is valid");
        let series_key = SeriesKey::derive(&dataflow, [("region", "AUS"), ("measure", "index")]);
        let observation = Observation {
            series_key,
            time: DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
                .expect("valid fixture date")
                .with_timezone(&Utc),
            time_precision: TimePrecision::Quarter,
            value: value.map(|value| f64::from(value) / 10.0),
            status: if value.is_some() {
                ObservationStatus::Normal
            } else {
                ObservationStatus::Missing
            },
            revision_no,
            attributes: BTreeMap::from([("OBS_STATUS".to_string(), "A".to_string())]),
            ingested_at: DateTime::parse_from_rfc3339("2024-04-30T00:00:00Z")
                .expect("valid fixture date")
                .with_timezone(&Utc),
            source_artifact_id: ArtifactId::of_content(CPI_FIXTURE),
        };

        let json = serde_json::to_string(&observation).expect("serialize observation");
        let roundtrip: Observation = serde_json::from_str(&json).expect("deserialize observation");

        prop_assert_eq!(observation, roundtrip);
    }
}

async fn parse_fixture_rows() -> Vec<ParsedRow> {
    parse_fixture_raw(CPI_FIXTURE)
        .await
        .expect("parse fixture")
        .into_iter()
        .map(|(series, observation)| ParsedRow {
            series_key: series.series_key.to_string(),
            dataflow_id: series.dataflow_id.to_string(),
            measure_id: series.measure_id.to_string(),
            dimensions: series
                .dimensions
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            time: observation.time.to_rfc3339(),
            time_precision: format!("{:?}", observation.time_precision).to_lowercase(),
            value: observation.value,
            status: format!("{:?}", observation.status).to_lowercase(),
            attributes: observation.attributes,
        })
        .collect()
}

async fn parse_fixture_raw(
    fixture: &'static [u8],
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    parse_fixture_owned(fixture.to_vec()).await
}

async fn parse_fixture_owned(
    fixture: Vec<u8>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    parse_fixture_owned_with_url(
        fixture,
        "https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD",
    )
    .await
}

async fn parse_fixture_owned_with_url(
    fixture: Vec<u8>,
    source_url: &str,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact_id_expected = ArtifactId::of_content(&fixture);
    let size_bytes = fixture.len() as u64;
    let artifact_id = blob_store
        .put_artifact_stream(stream::iter([Ok::<_, std::io::Error>(Bytes::from(
            fixture.clone(),
        ))]))
        .await
        .expect("store fixture artifact");

    let artifact = ArtifactRef {
        id: artifact_id,
        source_id: SourceId::new("abs").expect("static source id is valid"),
        source_url: source_url.into(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&artifact_id).to_string(),
        size_bytes,
        fetched_at: DateTime::parse_from_rfc3339("2024-04-24T00:00:00Z")
            .expect("valid fixture date")
            .with_timezone(&Utc),
    };
    assert_eq!(artifact.id, artifact_id_expected);

    let adapter = AbsAdapter::default();
    let ctx = ParseCtx::new(
        AdapterHttpClient::new(adapter.manifest().rate_limit),
        blob_store,
        DateTime::parse_from_rfc3339("2024-04-30T00:00:00Z")
            .expect("valid fixture date")
            .with_timezone(&Utc),
    );

    let mut stream = adapter.parse(artifact, &ctx);
    let mut rows = Vec::new();
    while let Some(next) = stream.next().await {
        rows.push(next?);
    }
    Ok(rows)
}

#[derive(Debug)]
struct FailingReadObjectStore;

impl fmt::Display for FailingReadObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FailingReadObjectStore")
    }
}

#[async_trait]
impl ObjectStore for FailingReadObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        Ok(put_result())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        Ok(Box::new(UnsupportedMultipartUpload))
    }

    async fn get_opts(
        &self,
        location: &Path,
        _options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let stream = stream::iter([
            Ok(Bytes::from_static(br#"{"data":{"structure":"#)),
            Err(ObjectStoreError::Generic {
                store: "failing-read",
                source: io::Error::other("backend reset").into(),
            }),
        ]);

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream.boxed()),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: Utc::now(),
                size: 128,
                e_tag: None,
                version: None,
            },
            range: 0..128,
            attributes: Attributes::new(),
        })
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        Err(ObjectStoreError::NotFound {
            path: location.to_string(),
            source: "not persisted in failing read store".into(),
        })
    }

    async fn delete(&self, _location: &Path) -> ObjectStoreResult<()> {
        Ok(())
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        stream::empty().boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        Ok(ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        Ok(())
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct UnsupportedMultipartUpload;

#[async_trait]
impl MultipartUpload for UnsupportedMultipartUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        async { Ok(()) }.boxed()
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        Ok(put_result())
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        Ok(())
    }
}

fn put_result() -> PutResult {
    PutResult {
        e_tag: None,
        version: None,
    }
}
