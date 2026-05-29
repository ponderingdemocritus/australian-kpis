//! Observation — one data point in the `observations` hypertable.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::{ArtifactId, SeriesKey};

/// Temporal granularity of an observation's timestamp. Matches the SDMX
/// `@FREQ` facet on a coarse scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum TimePrecision {
    Minute,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

/// Observation status flags sourced from SDMX. Subset in use across Australian
/// publishers; extend as new codes appear in upstream feeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ObservationStatus {
    Normal,
    Estimated,
    Forecast,
    Imputed,
    Missing,
    Provisional,
    Revised,
    Break,
}

/// One observation — a single row in the `observations` hypertable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Observation {
    pub series_key: SeriesKey,
    pub time: DateTime<Utc>,
    pub time_precision: TimePrecision,
    /// `None` when `status = Missing`.
    pub value: Option<f64>,
    pub status: ObservationStatus,
    /// `0` for the original publication; `N` for the Nth revision.
    pub revision_no: u32,
    /// Free-form attributes attached to this observation (e.g. SDMX `OBS_STATUS`
    /// or adapter-specific annotations).
    pub attributes: BTreeMap<String, String>,
    pub ingested_at: DateTime<Utc>,
    pub source_artifact_id: ArtifactId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{ArtifactId, DataflowId, SeriesKey, Sha256Digest};
    use chrono::TimeZone;
    use proptest::{collection::btree_map, prelude::*};

    #[test]
    fn roundtrips() {
        let df = DataflowId::new("abs.cpi").unwrap();
        let key = SeriesKey::derive(&df, [("region", "AUS")]);
        let obs = Observation {
            series_key: key,
            time: DateTime::parse_from_rfc3339("2024-03-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            time_precision: TimePrecision::Quarter,
            value: Some(134.2),
            status: ObservationStatus::Normal,
            revision_no: 0,
            attributes: [("OBS_STATUS".to_string(), "A".to_string())]
                .into_iter()
                .collect(),
            ingested_at: DateTime::parse_from_rfc3339("2024-04-30T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            source_artifact_id: ArtifactId::of_content(b"raw-sdmx-bytes"),
        };
        let json = serde_json::to_string(&obs).unwrap();
        let back: Observation = serde_json::from_str(&json).unwrap();
        assert_eq!(obs, back);
    }

    #[test]
    fn missing_value_roundtrips() {
        let df = DataflowId::new("abs.cpi").unwrap();
        let key = SeriesKey::derive(&df, [("region", "AUS")]);
        let obs = Observation {
            series_key: key,
            time: DateTime::parse_from_rfc3339("2024-03-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            time_precision: TimePrecision::Quarter,
            value: None,
            status: ObservationStatus::Missing,
            revision_no: 0,
            attributes: BTreeMap::new(),
            ingested_at: DateTime::parse_from_rfc3339("2024-04-30T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            source_artifact_id: ArtifactId::of_content(b"raw"),
        };
        let json = serde_json::to_string(&obs).unwrap();
        let back: Observation = serde_json::from_str(&json).unwrap();
        assert_eq!(obs, back);
    }

    #[test]
    fn time_precision_lowercases() {
        assert_eq!(
            serde_json::to_string(&TimePrecision::Quarter).unwrap(),
            "\"quarter\""
        );
    }

    #[test]
    fn observation_status_lowercases() {
        assert_eq!(
            serde_json::to_string(&ObservationStatus::Provisional).unwrap(),
            "\"provisional\""
        );
    }

    fn arb_datetime() -> impl Strategy<Value = DateTime<Utc>> {
        (946_684_800_i64..1_893_456_000_i64, 0_u32..1_000_000_000_u32)
            .prop_map(|(secs, nanos)| Utc.timestamp_opt(secs, nanos).single().unwrap())
    }

    fn arb_time_precision() -> impl Strategy<Value = TimePrecision> {
        prop_oneof![
            Just(TimePrecision::Minute),
            Just(TimePrecision::Day),
            Just(TimePrecision::Week),
            Just(TimePrecision::Month),
            Just(TimePrecision::Quarter),
            Just(TimePrecision::Year),
        ]
    }

    fn arb_observation_status() -> impl Strategy<Value = ObservationStatus> {
        prop_oneof![
            Just(ObservationStatus::Normal),
            Just(ObservationStatus::Estimated),
            Just(ObservationStatus::Forecast),
            Just(ObservationStatus::Imputed),
            Just(ObservationStatus::Provisional),
            Just(ObservationStatus::Revised),
            Just(ObservationStatus::Break),
        ]
    }

    fn arb_observation() -> impl Strategy<Value = Observation> {
        (
            any::<[u8; 32]>(),
            arb_datetime(),
            arb_time_precision(),
            prop::option::of(
                (-1_000_000_000_i64..1_000_000_000_i64).prop_map(|value| value as f64 / 100.0),
            ),
            arb_observation_status(),
            0_u32..1_000,
            btree_map("[A-Z_]{1,16}", "[A-Za-z0-9 ._:/-]{0,48}", 0..8),
            arb_datetime(),
            any::<[u8; 32]>(),
        )
            .prop_map(
                |(
                    series_key,
                    time,
                    time_precision,
                    value,
                    status,
                    revision_no,
                    attributes,
                    ingested_at,
                    artifact_id,
                )| Observation {
                    series_key: SeriesKey::from_digest(Sha256Digest::from_bytes(series_key)),
                    time,
                    time_precision,
                    value,
                    status: if value.is_some() {
                        status
                    } else {
                        ObservationStatus::Missing
                    },
                    revision_no,
                    attributes,
                    ingested_at,
                    source_artifact_id: ArtifactId::from_digest(Sha256Digest::from_bytes(
                        artifact_id,
                    )),
                },
            )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn observation_json_roundtrips_for_generated_values(obs in arb_observation()) {
            let json = serde_json::to_string(&obs).unwrap();
            let back: Observation = serde_json::from_str(&json).unwrap();

            prop_assert_eq!(obs, back);
        }
    }
}
