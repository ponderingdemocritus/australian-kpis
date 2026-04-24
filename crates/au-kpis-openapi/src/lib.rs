//! OpenAPI spec emitter.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

pub use au_kpis_api_http::ApiDoc;
use thiserror::Error;
use utoipa::OpenApi;

/// Errors returned by the OpenAPI emitter.
#[derive(Debug, Error)]
pub enum OpenApiError {
    /// Serializing the generated spec failed.
    #[error("openapi serialization: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Emit the current OpenAPI document as OpenAPI 3.1 JSON.
pub fn emit() -> Result<String, OpenApiError> {
    Ok(ApiDoc::openapi().to_pretty_json()?)
}
