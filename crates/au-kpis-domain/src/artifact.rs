//! Artifact — raw upstream file, content-addressed by SHA-256.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::{ArtifactId, SourceId};

/// MIME-style content type declared by the fetcher. Free-form string so new
/// upstream formats don't need a schema bump.
pub type ContentType = String;

/// A raw upstream artifact persisted in R2 under `artifacts/<hex>`. Records
/// where it came from, when it was fetched, and its on-wire size so loaders
/// can audit re-ingestion without re-downloading.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Artifact {
    pub id: ArtifactId,
    pub source_id: SourceId,
    /// Canonical URL the artifact was fetched from.
    pub source_url: String,
    pub content_type: ContentType,
    /// HTTP response headers captured when the artifact was fetched.
    pub response_headers: BTreeMap<String, String>,
    /// Size in bytes as stored in R2 — equals the hashed byte stream length.
    pub size_bytes: u64,
    /// R2 object key; equal to `artifacts/<id.to_hex()>` by convention but
    /// persisted explicitly so retention moves can rewrite it.
    pub storage_key: String,
    pub fetched_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        let id = ArtifactId::of_content(b"sdmx-payload");
        let a = Artifact {
            id,
            source_id: SourceId::new("abs").unwrap(),
            source_url: "https://data.api.abs.gov.au/rest/data/CPI".into(),
            content_type: "application/vnd.sdmx.data+json".into(),
            response_headers: BTreeMap::from([("etag".to_string(), "\"abc\"".to_string())]),
            size_bytes: 12,
            storage_key: format!("artifacts/{}", id.to_hex()),
            fetched_at: DateTime::parse_from_rfc3339("2024-04-30T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        };
        let json = serde_json::to_string(&a).unwrap();
        let back: Artifact = serde_json::from_str(&json).unwrap();
        assert_eq!(a, back);
    }
}
