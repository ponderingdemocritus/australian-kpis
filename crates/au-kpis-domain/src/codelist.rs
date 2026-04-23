//! Codelists and the codes they contain.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::{CodeId, CodelistId};

/// A reusable vocabulary of codes attached to dimensions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Codelist {
    pub id: CodelistId,
    pub name: String,
    pub description: Option<String>,
    pub codes: Vec<Code>,
}

/// A single code within a codelist (e.g. `VIC` → "Victoria").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Code {
    pub id: CodeId,
    /// The codelist this code belongs to. Denormalised here so a `Code`
    /// emitted from the API is self-contained.
    pub codelist_id: CodelistId,
    pub name: String,
    pub description: Option<String>,
    /// Optional parent code id for hierarchical codelists (e.g. regions with
    /// state → LGA containment).
    pub parent_id: Option<CodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codelist_roundtrips() {
        let cl = Codelist {
            id: CodelistId::new("CL_STATE_AU").unwrap(),
            name: "Australian states and territories".into(),
            description: None,
            codes: vec![Code {
                id: CodeId::new("VIC").unwrap(),
                codelist_id: CodelistId::new("CL_STATE_AU").unwrap(),
                name: "Victoria".into(),
                description: None,
                parent_id: None,
            }],
        };
        let json = serde_json::to_string(&cl).unwrap();
        let back: Codelist = serde_json::from_str(&json).unwrap();
        assert_eq!(cl, back);
    }

    #[test]
    fn code_with_parent_roundtrips() {
        let c = Code {
            id: CodeId::new("VIC_MELB").unwrap(),
            codelist_id: CodelistId::new("CL_REGION_AU").unwrap(),
            name: "Greater Melbourne".into(),
            description: None,
            parent_id: Some(CodeId::new("VIC").unwrap()),
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: Code = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
    }
}
