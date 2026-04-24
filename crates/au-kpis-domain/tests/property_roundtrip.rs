use std::collections::BTreeMap;

use au_kpis_domain::{
    ArtifactId, DataflowId, Observation, ObservationStatus, SeriesKey, TimePrecision,
};
use chrono::{TimeZone, Utc};
use proptest::prelude::*;

fn slug_fragment() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9._-]{0,15}".prop_map(|value| value)
}

fn observation_status() -> impl Strategy<Value = ObservationStatus> {
    prop_oneof![
        Just(ObservationStatus::Normal),
        Just(ObservationStatus::Estimated),
        Just(ObservationStatus::Forecast),
        Just(ObservationStatus::Imputed),
        Just(ObservationStatus::Missing),
        Just(ObservationStatus::Provisional),
        Just(ObservationStatus::Revised),
        Just(ObservationStatus::Break),
    ]
}

fn time_precision() -> impl Strategy<Value = TimePrecision> {
    prop_oneof![
        Just(TimePrecision::Day),
        Just(TimePrecision::Week),
        Just(TimePrecision::Month),
        Just(TimePrecision::Quarter),
        Just(TimePrecision::Year),
    ]
}

fn timestamp() -> impl Strategy<Value = chrono::DateTime<Utc>> {
    (946684800i64..2_209_075_200).prop_map(|seconds| {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("timestamp in supported range")
    })
}

fn observation_strategy() -> impl Strategy<Value = Observation> {
    (
        slug_fragment(),
        slug_fragment(),
        slug_fragment(),
        time_precision(),
        prop::option::of(-100_000i64..100_000i64).prop_map(|value| value.map(|value| value as f64)),
        observation_status(),
        0u32..8,
        prop::collection::btree_map(slug_fragment(), slug_fragment(), 0..4),
        timestamp(),
        timestamp(),
        prop::collection::vec(any::<u8>(), 1..64),
    )
        .prop_map(
            |(
                dataflow,
                dimension,
                dimension_value,
                time_precision,
                value,
                status,
                revision_no,
                attributes,
                time,
                ingested_at,
                artifact_bytes,
            )| {
                let dataflow = DataflowId::new(dataflow).expect("generated dataflow id");
                let series_key =
                    SeriesKey::derive(&dataflow, [(dimension.as_str(), dimension_value.as_str())]);
                Observation {
                    series_key,
                    time,
                    time_precision,
                    value,
                    status,
                    revision_no,
                    attributes: BTreeMap::from_iter(attributes),
                    ingested_at,
                    source_artifact_id: ArtifactId::of_content(&artifact_bytes),
                }
            },
        )
}

proptest! {
    #[test]
    fn observation_json_roundtrip_preserves_value(obs in observation_strategy()) {
        let json = serde_json::to_string(&obs).expect("serialize observation");
        let roundtrip: Observation = serde_json::from_str(&json).expect("deserialize observation");
        prop_assert_eq!(roundtrip, obs);
    }
}
