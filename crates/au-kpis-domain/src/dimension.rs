//! Dimensions — categorical axes of a dataflow (e.g. `region`, `measure`).

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::{CodelistId, DimensionId};

/// A single dimension within a dataflow. Binds a dimension id to a codelist
/// that enumerates the permitted values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Dimension {
    pub id: DimensionId,
    pub name: String,
    pub description: Option<String>,
    pub codelist_id: CodelistId,
    /// Position of this dimension in the dataflow's canonical dimension order.
    /// Adapters must emit dimensions in this order so series-key derivation is
    /// stable across codebases (see `Spec.md § Data model`).
    pub position: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        let d = Dimension {
            id: DimensionId::new("region").unwrap(),
            name: "Region".into(),
            description: Some("Geographic region".into()),
            codelist_id: CodelistId::new("CL_STATE_AU").unwrap(),
            position: 0,
        };
        let json = serde_json::to_string(&d).unwrap();
        let back: Dimension = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);
    }
}
