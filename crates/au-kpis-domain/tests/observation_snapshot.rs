use std::collections::BTreeMap;

use au_kpis_domain::{
    ArtifactId, DataflowId, Observation, ObservationStatus, SeriesKey, TimePrecision,
};
use chrono::{DateTime, Utc};

fn example_observation() -> Observation {
    let dataflow = DataflowId::new("abs.cpi").expect("valid dataflow");
    Observation {
        series_key: SeriesKey::derive(&dataflow, [("region", "AUS"), ("measure", "headline")]),
        time: DateTime::parse_from_rfc3339("2024-03-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc),
        time_precision: TimePrecision::Quarter,
        value: Some(134.2),
        status: ObservationStatus::Revised,
        revision_no: 2,
        attributes: BTreeMap::from([
            ("OBS_STATUS".to_string(), "R".to_string()),
            ("UNIT_MULT".to_string(), "0".to_string()),
        ]),
        ingested_at: DateTime::parse_from_rfc3339("2024-04-30T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc),
        source_artifact_id: ArtifactId::of_content(b"abs-cpi-fixture"),
    }
}

#[test]
fn observation_contract_snapshot() {
    insta::assert_yaml_snapshot!("observation_contract", example_observation());
}
