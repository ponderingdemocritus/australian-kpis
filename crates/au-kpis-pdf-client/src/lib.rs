//! HTTP client for the Python PDF extractor sidecar.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{collections::BTreeMap, time::Duration};

use au_kpis_error::{Classify, ErrorClass};
use reqwest::{StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_tracing::{OtelName, TracingMiddleware};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP client for `POST /extract`.
#[derive(Debug, Clone)]
pub struct PdfClient {
    client: ClientWithMiddleware,
    base_url: Url,
    retry_policy: RetryPolicy,
    request_timeout: Duration,
}

impl PdfClient {
    /// Start building a PDF client.
    #[must_use]
    pub fn builder() -> PdfClientBuilder {
        PdfClientBuilder::default()
    }

    /// Construct a client with default retry policy.
    pub fn new(base_url: impl AsRef<str>) -> Result<Self, PdfClientError> {
        Self::builder().base_url(base_url).build()
    }

    /// Request table extraction for one stored PDF artifact.
    #[tracing::instrument(skip(self, request), fields(s3_key = %request.s3_key))]
    pub async fn extract(
        &self,
        request: ExtractRequest,
    ) -> Result<ExtractionResponse, PdfClientError> {
        let url = self
            .base_url
            .join("extract")
            .map_err(|err| PdfClientError::InvalidUrl(err.to_string()))?;
        let mut attempt = 1;

        loop {
            let result =
                match tokio::time::timeout(self.request_timeout, self.post_extract(&url, &request))
                    .await
                {
                    Ok(result) => result,
                    Err(_) => Err(PdfClientError::Timeout {
                        timeout: self.request_timeout,
                    }),
                };
            match result {
                Ok(response) => return Ok(response),
                Err(err)
                    if attempt < self.retry_policy.max_attempts && err.class().is_retryable() =>
                {
                    tokio::time::sleep(self.retry_policy.delay_for_attempt(attempt)).await;
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn post_extract(
        &self,
        url: &Url,
        request: &ExtractRequest,
    ) -> Result<ExtractionResponse, PdfClientError> {
        let response = self
            .client
            .post(url.clone())
            .with_extension(OtelName("pdf.extract".into()))
            .json(request)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(response.json().await?);
        }

        let body = response.text().await.unwrap_or_default();
        Err(PdfClientError::Status { status, body })
    }
}

/// Builder for [`PdfClient`].
#[derive(Debug, Clone)]
pub struct PdfClientBuilder {
    client: Option<reqwest::Client>,
    base_url: Option<String>,
    retry_policy: RetryPolicy,
    request_timeout: Duration,
}

impl Default for PdfClientBuilder {
    fn default() -> Self {
        Self {
            client: None,
            base_url: None,
            retry_policy: RetryPolicy::default(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }
}

impl PdfClientBuilder {
    /// Override the underlying HTTP client.
    #[must_use]
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the PDF sidecar base URL.
    #[must_use]
    pub fn base_url(mut self, base_url: impl AsRef<str>) -> Self {
        self.base_url = Some(base_url.as_ref().trim_end_matches('/').to_string() + "/");
        self
    }

    /// Set retry policy.
    #[must_use]
    pub const fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Set the per-attempt request timeout.
    #[must_use]
    pub const fn timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<PdfClient, PdfClientError> {
        let base_url = self
            .base_url
            .ok_or(PdfClientError::MissingBaseUrl)
            .and_then(|url| {
                Url::parse(&url).map_err(|err| PdfClientError::InvalidUrl(err.to_string()))
            })?;
        let client = match self.client {
            Some(client) => client,
            None => reqwest::Client::builder()
                .timeout(self.request_timeout)
                .build()?,
        };
        Ok(PdfClient {
            client: traced_http_client(client),
            base_url,
            retry_policy: self.retry_policy,
            request_timeout: self.request_timeout,
        })
    }
}

fn traced_http_client(client: reqwest::Client) -> ClientWithMiddleware {
    ClientBuilder::new(client)
        .with(TracingMiddleware::default())
        .build()
}

/// Retry policy for transient sidecar failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: u32,
    initial_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
        }
    }
}

impl RetryPolicy {
    /// Construct a retry policy.
    pub fn new(max_attempts: u32, initial_backoff: Duration) -> Result<Self, PdfClientError> {
        if max_attempts == 0 {
            return Err(PdfClientError::Validation(
                "max_attempts must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            max_attempts,
            initial_backoff,
        })
    }

    /// Disable retries.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff: Duration::ZERO,
        }
    }

    fn delay_for_attempt(self, attempt: u32) -> Duration {
        let multiplier = 1_u32 << attempt.saturating_sub(1).min(6);
        self.initial_backoff.saturating_mul(multiplier)
    }
}

/// Request body for `POST /extract`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExtractRequest {
    s3_key: String,
    source_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    strategy: Option<ExtractionStrategy>,
}

impl ExtractRequest {
    /// Construct a request for the stored artifact key and source id.
    #[must_use]
    pub fn new(s3_key: impl Into<String>, source_id: impl Into<String>) -> Self {
        Self {
            s3_key: s3_key.into(),
            source_id: source_id.into(),
            artifact_date: None,
            strategy: None,
        }
    }

    /// Set artifact publication date context.
    #[must_use]
    pub fn artifact_date(mut self, artifact_date: impl Into<String>) -> Self {
        self.artifact_date = Some(artifact_date.into());
        self
    }

    /// Set extraction strategy preference.
    #[must_use]
    pub const fn strategy(mut self, strategy: ExtractionStrategy) -> Self {
        self.strategy = Some(strategy);
        self
    }
}

/// Extraction strategy requested from the sidecar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtractionStrategy {
    /// Deterministic `pdfplumber`/`camelot` path.
    Deterministic,
    /// Local model fallback path.
    ModelFallback,
}

/// Response body returned by the PDF sidecar.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ExtractionResponse {
    /// Artifact key extracted by the sidecar.
    #[serde(alias = "s3_key")]
    pub artifact_key: String,
    /// Backend that produced table candidates.
    pub backend: BackendInfo,
    /// Extracted table candidates.
    pub tables: Vec<TableCandidate>,
}

/// Backend metadata for extraction.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BackendInfo {
    /// Backend kind.
    pub kind: ExtractionBackendKind,
    /// Backend name.
    pub name: String,
    /// Backend version.
    pub version: String,
    /// Optional model weights checksum.
    pub model_sha256: Option<String>,
}

/// Backend family that produced extraction candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtractionBackendKind {
    /// Deterministic backend.
    Deterministic,
    /// Model backend.
    Model,
}

/// Extracted table candidate.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TableCandidate {
    /// 1-indexed page number.
    pub page: u32,
    /// Bounding box `[x1, y1, x2, y2]`.
    pub bbox: [f64; 4],
    /// Raw table cells, preserving sidecar row/column structure.
    pub cells: Vec<Vec<String>>,
    /// Row/column span metadata for merged cells when available.
    #[serde(default, alias = "cell_spans")]
    pub spans: Vec<CellSpan>,
    /// Backend diagnostics such as confidence.
    #[serde(default)]
    pub diagnostics: BTreeMap<String, serde_json::Value>,
}

/// Row/column span metadata for a table cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct CellSpan {
    /// Zero-indexed cell row.
    pub row: u32,
    /// Zero-indexed cell column.
    pub column: u32,
    /// Number of rows covered by the cell.
    pub row_span: u32,
    /// Number of columns covered by the cell.
    pub column_span: u32,
}

/// Errors returned by the PDF client.
#[derive(Debug, Error)]
pub enum PdfClientError {
    /// Base URL was not configured.
    #[error("pdf client base_url is required")]
    MissingBaseUrl,

    /// URL parsing or joining failed.
    #[error("invalid url: {0}")]
    InvalidUrl(String),

    /// HTTP request failed before a response was available.
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),

    /// Middleware-wrapped HTTP request failed before a response was available.
    #[error("http middleware: {0}")]
    Middleware(#[from] reqwest_middleware::Error),

    /// Sidecar returned an unsuccessful status.
    #[error("pdf sidecar returned {status}: {body}")]
    Status {
        /// HTTP status code.
        status: StatusCode,
        /// Response body, if available.
        body: String,
    },

    /// Sidecar request exceeded the configured timeout.
    #[error("pdf sidecar request timed out after {timeout:?}")]
    Timeout {
        /// Configured per-attempt timeout.
        timeout: Duration,
    },

    /// Caller supplied invalid configuration.
    #[error("validation: {0}")]
    Validation(String),
}

impl Classify for PdfClientError {
    fn class(&self) -> ErrorClass {
        match self {
            Self::MissingBaseUrl | Self::InvalidUrl(_) | Self::Validation(_) => {
                ErrorClass::Validation
            }
            Self::Http(err) => {
                if err.is_decode() {
                    ErrorClass::Permanent
                } else {
                    ErrorClass::Transient
                }
            }
            Self::Middleware(err) => {
                if err.is_decode() {
                    ErrorClass::Permanent
                } else {
                    ErrorClass::Transient
                }
            }
            Self::Status { status, .. } => {
                if status.is_server_error() {
                    ErrorClass::Transient
                } else {
                    ErrorClass::Permanent
                }
            }
            Self::Timeout { .. } => ErrorClass::Transient,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_requires_base_url_and_rejects_invalid_url() {
        assert_eq!(
            PdfClient::builder().build().unwrap_err().class(),
            ErrorClass::Validation
        );
        assert_eq!(
            PdfClient::new("not a url").unwrap_err().class(),
            ErrorClass::Validation
        );
    }

    #[test]
    fn builder_normalizes_base_url_and_accepts_custom_http_client() {
        let client = reqwest::Client::builder().build().unwrap();
        let pdf_client = PdfClient::builder()
            .http_client(client)
            .base_url("http://127.0.0.1:3000////")
            .retry_policy(RetryPolicy::none())
            .build()
            .unwrap();

        assert_eq!(pdf_client.base_url.as_str(), "http://127.0.0.1:3000/");
        assert_eq!(pdf_client.retry_policy, RetryPolicy::none());
    }

    #[test]
    fn retry_policy_rejects_zero_attempts_and_caps_backoff() {
        assert_eq!(
            RetryPolicy::new(0, Duration::ZERO).unwrap_err().class(),
            ErrorClass::Validation
        );

        let retry_policy = RetryPolicy::new(3, Duration::from_millis(10)).unwrap();
        assert_eq!(retry_policy.delay_for_attempt(0), Duration::from_millis(10));
        assert_eq!(retry_policy.delay_for_attempt(1), Duration::from_millis(10));
        assert_eq!(retry_policy.delay_for_attempt(2), Duration::from_millis(20));
        assert_eq!(
            retry_policy.delay_for_attempt(99),
            Duration::from_millis(640)
        );
        assert_eq!(RetryPolicy::none().delay_for_attempt(2), Duration::ZERO);
    }

    #[test]
    fn extraction_request_requires_source_id_and_omits_optional_fields_until_set() {
        let minimal =
            serde_json::to_value(ExtractRequest::new("raw/report.pdf", "treasury")).unwrap();
        assert_eq!(
            minimal,
            serde_json::json!({ "s3_key": "raw/report.pdf", "source_id": "treasury" })
        );

        let enriched = serde_json::to_value(
            ExtractRequest::new("raw/report.pdf", "treasury")
                .artifact_date("2026-05-12")
                .strategy(ExtractionStrategy::ModelFallback),
        )
        .unwrap();
        assert_eq!(enriched["source_id"], "treasury");
        assert_eq!(enriched["artifact_date"], "2026-05-12");
        assert_eq!(enriched["strategy"], "model_fallback");
    }

    #[test]
    fn extraction_response_accepts_s3_key_alias_and_requires_tables() {
        let response: ExtractionResponse = serde_json::from_value(serde_json::json!({
            "s3_key": "raw/report.pdf",
            "backend": {
                "kind": "model",
                "name": "layoutlm",
                "version": "1.0.0",
                "model_sha256": "abc123"
            },
            "tables": []
        }))
        .unwrap();

        assert_eq!(response.artifact_key, "raw/report.pdf");
        assert_eq!(response.backend.kind, ExtractionBackendKind::Model);
        assert_eq!(response.backend.model_sha256.as_deref(), Some("abc123"));
        assert!(response.tables.is_empty());

        let err = serde_json::from_value::<ExtractionResponse>(serde_json::json!({
            "s3_key": "raw/report.pdf",
            "backend": {
                "kind": "model",
                "name": "layoutlm",
                "version": "1.0.0",
                "model_sha256": "abc123"
            }
        }))
        .expect_err("missing tables should be a response contract error");
        assert!(err.to_string().contains("tables"));
    }

    #[test]
    fn extraction_response_preserves_cell_span_metadata() {
        let response: ExtractionResponse = serde_json::from_value(serde_json::json!({
            "artifact_key": "raw/report.pdf",
            "backend": {
                "kind": "deterministic",
                "name": "camelot",
                "version": "1.0.0",
                "model_sha256": null
            },
            "tables": [
                {
                    "page": 3,
                    "bbox": [10.0, 20.0, 30.0, 40.0],
                    "cells": [["Year", "Revenue"]],
                    "spans": [
                        {
                            "row": 0,
                            "column": 0,
                            "row_span": 1,
                            "column_span": 2
                        }
                    ]
                }
            ]
        }))
        .unwrap();

        assert_eq!(response.tables[0].spans[0].row, 0);
        assert_eq!(response.tables[0].spans[0].column, 0);
        assert_eq!(response.tables[0].spans[0].row_span, 1);
        assert_eq!(response.tables[0].spans[0].column_span, 2);
    }

    #[test]
    fn error_classification_separates_retryable_statuses() {
        assert_eq!(
            PdfClientError::Status {
                status: StatusCode::BAD_REQUEST,
                body: "bad request".to_string()
            }
            .class(),
            ErrorClass::Permanent
        );
        assert_eq!(
            PdfClientError::Status {
                status: StatusCode::SERVICE_UNAVAILABLE,
                body: "busy".to_string()
            }
            .class(),
            ErrorClass::Transient
        );
    }
}
