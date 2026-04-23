//! Upstream data source metadata.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::SourceId;

/// An upstream publisher of statistical data (ABS, RBA, APRA, ...).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Source {
    pub id: SourceId,
    /// Human-readable name, e.g. "Australian Bureau of Statistics".
    pub name: String,
    /// Canonical homepage for citation.
    pub homepage: String,
    /// Short description of the source and its scope.
    pub description: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        let src = Source {
            id: SourceId::new("abs").unwrap(),
            name: "Australian Bureau of Statistics".into(),
            homepage: "https://www.abs.gov.au".into(),
            description: Some("Official statistical agency of Australia.".into()),
        };
        let json = serde_json::to_string(&src).unwrap();
        let back: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(src, back);
    }
}
