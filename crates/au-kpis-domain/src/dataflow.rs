//! Dataflow — a conceptually coherent statistical dataset.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::{DataflowId, DimensionId, MeasureId, SourceId};

/// Canonical license identifier. Ingestion refuses to load a dataflow whose
/// `license` field is absent (see `Spec.md § Data licensing and attribution`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum License {
    #[serde(rename = "CC-BY-4.0")]
    CcBy40,
    #[serde(rename = "CC-BY-ND-4.0")]
    CcByNd40,
    #[serde(rename = "CC-BY-SA-4.0")]
    CcBySa40,
    #[serde(rename = "public-domain")]
    PublicDomain,
    /// Escape hatch for licenses not yet enumerated. Carries the SPDX-style id.
    Other(String),
}

/// Cadence at which a dataflow publishes new observations. Informational —
/// used by the scheduler to pick discovery intervals and by the SLO alerting
/// rule `dataflow-no-new-observations`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Frequency {
    Daily,
    Weekly,
    Monthly,
    Quarterly,
    Annual,
    Irregular,
}

/// A dataflow — a named collection of series sharing dimensions and measures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Dataflow {
    pub id: DataflowId,
    pub source_id: SourceId,
    pub name: String,
    pub description: Option<String>,
    /// Ordered list of dimension ids that key series within this dataflow.
    pub dimensions: Vec<DimensionId>,
    /// Measures published by this dataflow (index, rate, count, ...).
    pub measures: Vec<MeasureId>,
    pub frequency: Frequency,
    pub license: License,
    /// Required attribution string shown to end-users alongside derived charts.
    pub attribution: String,
    /// Canonical citation URL.
    pub source_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        let df = Dataflow {
            id: DataflowId::new("abs.cpi").unwrap(),
            source_id: SourceId::new("abs").unwrap(),
            name: "Consumer Price Index, Australia".into(),
            description: Some("Quarterly CPI across capital cities.".into()),
            dimensions: vec![
                DimensionId::new("region").unwrap(),
                DimensionId::new("measure").unwrap(),
            ],
            measures: vec![MeasureId::new("index").unwrap()],
            frequency: Frequency::Quarterly,
            license: License::CcBy40,
            attribution: "Source: Australian Bureau of Statistics".into(),
            source_url: "https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia".into(),
        };
        let json = serde_json::to_string(&df).unwrap();
        let back: Dataflow = serde_json::from_str(&json).unwrap();
        assert_eq!(df, back);
    }

    #[test]
    fn license_renames_match_spec() {
        assert_eq!(
            serde_json::to_string(&License::CcBy40).unwrap(),
            "\"CC-BY-4.0\""
        );
        assert_eq!(
            serde_json::to_string(&License::PublicDomain).unwrap(),
            "\"public-domain\""
        );
    }

    #[test]
    fn other_license_roundtrips() {
        let l = License::Other("LGPL-2.1".into());
        let json = serde_json::to_string(&l).unwrap();
        let back: License = serde_json::from_str(&json).unwrap();
        assert_eq!(l, back);
    }
}
