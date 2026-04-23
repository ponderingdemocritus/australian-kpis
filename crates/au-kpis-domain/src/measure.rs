//! Measures — what an observation is counting.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::MeasureId;

/// A measure — the quantity being reported (index, rate, count, dollars).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Measure {
    pub id: MeasureId,
    pub name: String,
    pub description: Option<String>,
    /// Human-readable unit string (e.g. `"index"`, `"percent"`, `"AUD"`).
    pub unit: String,
    /// Optional multiplier hint (e.g. `1e6` for "$M"). Omitted for
    /// dimensionless measures.
    pub scale: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        let m = Measure {
            id: MeasureId::new("cpi_index").unwrap(),
            name: "CPI Index".into(),
            description: Some("Consumer Price Index, base 2011-12 = 100".into()),
            unit: "index".into(),
            scale: None,
        };
        let json = serde_json::to_string(&m).unwrap();
        let back: Measure = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn scale_roundtrips() {
        let m = Measure {
            id: MeasureId::new("gdp_usd_millions").unwrap(),
            name: "GDP".into(),
            description: None,
            unit: "AUD".into(),
            scale: Some(1_000_000.0),
        };
        let json = serde_json::to_string(&m).unwrap();
        let back: Measure = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }
}
