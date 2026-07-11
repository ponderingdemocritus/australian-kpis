use au_kpis_domain::ids::DataflowId;

use crate::ApiError;

/// Response format selected by an observations query plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseFormat {
    /// JSON envelope response.
    Json,
    /// CSV streaming response.
    Csv,
    /// Parquet streaming response.
    Parquet,
}

impl From<super::ResponseFormat> for ResponseFormat {
    fn from(format: super::ResponseFormat) -> Self {
        match format {
            super::ResponseFormat::Json => Self::Json,
            super::ResponseFormat::Csv => Self::Csv,
            super::ResponseFormat::Parquet => Self::Parquet,
        }
    }
}

/// Parsed observations query plus derived planning decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservationsQueryPlan {
    query: super::ParsedObservationsQuery,
}

impl ObservationsQueryPlan {
    /// Parse raw query parameters into an observations query plan.
    pub(crate) fn parse(raw: Option<&str>) -> Result<Self, ApiError> {
        Ok(Self {
            query: super::parse_observations_query(raw)?,
        })
    }

    /// Requested dataflow id.
    pub(crate) fn dataflow(&self) -> &DataflowId {
        &self.query.dataflow
    }

    /// Validated page/export limit.
    pub(crate) const fn limit(&self) -> usize {
        self.query.limit
    }

    /// Planned response format.
    pub(crate) fn response_format(&self) -> ResponseFormat {
        self.query.format.into()
    }

    /// Whether this query needs a cache fingerprint before rendering.
    pub(crate) fn requires_cache_fingerprint(&self) -> bool {
        super::requires_cache_fingerprint(&self.query)
    }

    /// Whether this request consumes one of the four long-stream slots.
    pub(crate) fn is_bulk(&self) -> bool {
        self.query.format != super::ResponseFormat::Json || self.query.limit > super::DEFAULT_LIMIT
    }

    /// Consume the plan and return the parsed query used by existing renderers.
    pub(crate) fn into_query(self) -> super::ParsedObservationsQuery {
        self.query
    }
}
