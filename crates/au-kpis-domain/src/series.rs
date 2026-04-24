//! Series — metadata per time series, separated from the narrow
//! `observations` hot table (see `Spec.md § Data model`).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::{CodeId, DataflowId, DimensionId, MeasureId, SeriesKey};

/// One time series within a dataflow. `dimensions` is the sorted bag of
/// `(key, value)` pairs that, together with `dataflow_id`, seeds `series_key`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Series {
    pub series_key: SeriesKey,
    pub dataflow_id: DataflowId,
    pub measure_id: MeasureId,
    /// Dimension values keyed by typed dimension ids and code ids. Stored
    /// sorted (via `BTreeMap`) so iteration order is stable across
    /// serialisation + re-hashing.
    pub dimensions: BTreeMap<DimensionId, CodeId>,
    pub unit: String,
    pub first_observed: Option<DateTime<Utc>>,
    pub last_observed: Option<DateTime<Utc>>,
    /// Whether the upstream source still publishes this series. Inactive
    /// series are retained for historical queries but excluded from the
    /// default `/v1/series` listing.
    pub active: bool,
}

impl Series {
    /// Compute the canonical series key from the dataflow + dimension pairs.
    /// Equivalent to `SeriesKey::derive`, but borrows directly from an
    /// already-populated `Series`.
    pub fn compute_series_key(&self) -> SeriesKey {
        SeriesKey::derive(
            &self.dataflow_id,
            self.dimensions
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        )
    }
}

/// Lightweight descriptor emitted by adapters during `parse`. The loader
/// inserts the full `Series` row on first sighting and then references the
/// key on subsequent observations (see `Spec.md § Adapter trait`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SeriesDescriptor {
    pub series_key: SeriesKey,
    pub dataflow_id: DataflowId,
    pub measure_id: MeasureId,
    pub dimensions: BTreeMap<DimensionId, CodeId>,
    pub unit: String,
}

impl SeriesDescriptor {
    pub fn compute_series_key(&self) -> SeriesKey {
        SeriesKey::derive(
            &self.dataflow_id,
            self.dimensions
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{CodeId, DimensionId};

    fn example_series() -> Series {
        let df = DataflowId::new("abs.cpi").unwrap();
        let dims: BTreeMap<DimensionId, CodeId> = [(
            DimensionId::new("region").unwrap(),
            CodeId::new("AUS").unwrap(),
        )]
        .into_iter()
        .collect();
        let key = SeriesKey::derive(&df, dims.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        Series {
            series_key: key,
            dataflow_id: df,
            measure_id: MeasureId::new("index").unwrap(),
            dimensions: dims,
            unit: "index".into(),
            first_observed: Some(
                DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
            ),
            last_observed: None,
            active: true,
        }
    }

    #[test]
    fn series_roundtrips() {
        let s = example_series();
        let json = serde_json::to_string(&s).unwrap();
        let back: Series = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn series_key_matches_computed() {
        let s = example_series();
        assert_eq!(s.series_key, s.compute_series_key());
    }

    #[test]
    fn descriptor_roundtrips_and_matches_series_key() {
        let s = example_series();
        let d = SeriesDescriptor {
            series_key: s.series_key,
            dataflow_id: s.dataflow_id.clone(),
            measure_id: s.measure_id.clone(),
            dimensions: s.dimensions.clone(),
            unit: s.unit.clone(),
        };
        let json = serde_json::to_string(&d).unwrap();
        let back: SeriesDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);
        assert_eq!(d.compute_series_key(), s.series_key);
    }
}
