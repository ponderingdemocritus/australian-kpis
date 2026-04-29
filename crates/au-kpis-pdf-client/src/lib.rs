//! HTTP client for the Python PDF extractor sidecar.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{collections::BTreeMap, time::Duration};

use au_kpis_error::{Classify, ErrorClass};
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// HTTP client for `POST /extract`.
#[derive(Debug, Clone)]
pub struct PdfClient {
    client: reqwest::Client,
    base_url: Url,
    retry_policy: RetryPolicy,
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
            let result = self.post_extract(&url, &request).await;
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
        let response = self.client.post(url.clone()).json(request).send().await?;
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
    client: reqwest::Client,
    base_url: Option<String>,
    retry_policy: RetryPolicy,
}

impl Default for PdfClientBuilder {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: None,
            retry_policy: RetryPolicy::default(),
        }
    }
}

impl PdfClientBuilder {
    /// Override the underlying HTTP client.
    #[must_use]
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
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

    /// Build the client.
    pub fn build(self) -> Result<PdfClient, PdfClientError> {
        let base_url = self
            .base_url
            .ok_or(PdfClientError::MissingBaseUrl)
            .and_then(|url| {
                Url::parse(&url).map_err(|err| PdfClientError::InvalidUrl(err.to_string()))
            })?;
        Ok(PdfClient {
            client: self.client,
            base_url,
            retry_policy: self.retry_policy,
        })
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    source_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    strategy: Option<ExtractionStrategy>,
}

impl ExtractRequest {
    /// Construct a request for the stored artifact key.
    #[must_use]
    pub fn new(s3_key: impl Into<String>) -> Self {
        Self {
            s3_key: s3_key.into(),
            source_id: None,
            artifact_date: None,
            strategy: None,
        }
    }

    /// Set source id context.
    #[must_use]
    pub fn source_id(mut self, source_id: impl Into<String>) -> Self {
        self.source_id = Some(source_id.into());
        self
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
    #[serde(default)]
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
    /// Backend diagnostics such as confidence.
    #[serde(default)]
    pub diagnostics: BTreeMap<String, serde_json::Value>,
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

    /// Sidecar returned an unsuccessful status.
    #[error("pdf sidecar returned {status}: {body}")]
    Status {
        /// HTTP status code.
        status: StatusCode,
        /// Response body, if available.
        body: String,
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
            Self::Status { status, .. } => {
                if status.is_server_error() {
                    ErrorClass::Transient
                } else {
                    ErrorClass::Permanent
                }
            }
        }
    }
}
