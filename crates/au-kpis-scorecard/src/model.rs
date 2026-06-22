use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

/// Scorecard axis used by APS v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Axis {
    /// Delivery throughput normalized to `0..1`.
    Throughput,
    /// Policy orientation normalized to `-1..1`.
    Orientation,
}

/// Direction used to normalize raw indicator values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Larger raw values move toward the best reference.
    HigherIsBetter,
    /// Smaller raw values move toward the best reference.
    LowerIsBetter,
}

/// Confidence label exposed for configs, contributions, and snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// High-confidence source and transform.
    High,
    /// Medium-confidence source, transform, or coverage.
    Medium,
    /// Low-confidence source, transform, or coverage.
    Low,
}

impl Confidence {
    pub(crate) const fn severity(self) -> u8 {
        match self {
            Self::High => 0,
            Self::Medium => 1,
            Self::Low => 2,
        }
    }

    pub(crate) fn worst(self, other: Self) -> Self {
        if self.severity() >= other.severity() {
            self
        } else {
            other
        }
    }
}

/// Contribution coverage state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CoverageStatus {
    /// Scored observation exists and is fresh enough for its cadence.
    Resolved,
    /// Scored observation exists but is older than expected cadence.
    Stale,
    /// Configured scored input has no resolved observation yet.
    MissingExpected,
    /// Source scope intentionally does not cover this jurisdiction or input.
    CoverageGap,
    /// Manual or curated source is expected but not reviewed yet.
    ManualPending,
    /// Visible context only; excluded from score and confidence band.
    VisibleUnscored,
}

impl CoverageStatus {
    pub(crate) const fn is_visible_unscored(self) -> bool {
        matches!(self, Self::VisibleUnscored)
    }

    pub(crate) const fn can_score(self) -> bool {
        matches!(self, Self::Resolved | Self::Stale)
    }
}

/// APS score zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScoreZone {
    /// Score in `0..33`.
    Red,
    /// Score in `34..66`.
    Yellow,
    /// Score in `67..100`.
    Green,
}

/// Trend arrow comparing the latest score to a prior comparable snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Trend {
    /// Latest score increased by at least one point.
    Up,
    /// Latest score decreased by at least one point.
    Down,
    /// Latest score moved by less than one point.
    Flat,
    /// No comparable prior score exists.
    Unavailable,
}

/// Linear normalization references for one indicator.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Normalization {
    /// Worst reference value for this indicator.
    pub worst: f64,
    /// Best reference value for this indicator.
    pub best: f64,
}

/// Source and licensing metadata for a scorecard input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Provenance {
    /// Canonical source URL for this input.
    pub source_url: String,
    /// License identifier or source-specific license note.
    pub license: String,
    /// Attribution text shown in API/UI responses.
    pub attribution: String,
    /// Optional retrieval date for curated/manual inputs.
    #[serde(default)]
    pub retrieved_at: Option<String>,
    /// Optional reviewer identifier for curated/manual inputs.
    #[serde(default)]
    pub reviewed_by: Option<String>,
    /// Optional review date for curated/manual inputs.
    #[serde(default)]
    pub reviewed_at: Option<String>,
    /// Optional source or review notes.
    #[serde(default)]
    pub notes: Option<String>,
}

/// Static config for one APS indicator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct IndicatorConfig {
    /// Stable indicator id.
    pub indicator_id: String,
    /// Canonical source dataflow id.
    pub source_dataflow_id: String,
    /// Source measure id.
    pub measure_id: String,
    /// Exact dimension selector for the intended APS series.
    #[serde(default)]
    pub dimension_selector: BTreeMap<String, String>,
    /// APS axis.
    pub axis: Axis,
    /// Display grouping within the axis.
    pub component: String,
    /// Scored weight within the axis. Visible-unscored inputs may use zero.
    pub weight: f64,
    /// Direction-aware normalization rule.
    pub direction: Direction,
    /// Normalization references.
    pub normalization: Normalization,
    /// User-facing indicator label.
    pub display_label: String,
    /// User-facing indicator description.
    pub description: String,
    /// Display unit.
    pub unit: String,
    /// Confidence for this input.
    pub confidence: Confidence,
    /// Default configured coverage status.
    pub coverage_status: CoverageStatus,
    /// Expected update cadence.
    pub cadence: String,
    /// Source and licensing metadata.
    pub provenance: Provenance,
}

/// APS config metadata and indicator list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ScorecardConfig {
    /// Scorecard id.
    pub id: String,
    /// Versioned config id.
    pub version: String,
    /// User-facing label.
    pub label: String,
    /// User-facing description.
    pub description: String,
    /// Formula metadata.
    pub formula: String,
    /// Indicator definitions.
    pub indicators: Vec<IndicatorConfig>,
    /// License note for the derived scorecard config.
    pub license: String,
    /// Attribution note for the derived scorecard config.
    pub attribution: String,
}

/// Lower and upper score bounds.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ConfidenceBand {
    /// Worst-case score.
    pub low: f64,
    /// Best-case score.
    pub high: f64,
}

/// Component-level score inside a sub-index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ComponentScore {
    /// Component name.
    pub component: String,
    /// Component score in the axis scale.
    pub score: f64,
    /// Weight represented by this component.
    pub weight: f64,
    /// Weight-aware coverage percentage.
    pub coverage_pct: f64,
}

/// Axis-level sub-index score.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SubIndexScore {
    /// Axis name.
    pub axis: Axis,
    /// Axis score (`0..1` for throughput, `-1..1` for orientation).
    pub score: f64,
    /// Weight-aware coverage percentage.
    pub coverage_pct: f64,
    /// Expected scored weight for this axis.
    pub weight: f64,
    /// Axis confidence band in the axis scale.
    pub confidence_band: ConfidenceBand,
    /// Component scores for this axis.
    pub components: Vec<ComponentScore>,
}

/// Contribution row for one indicator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct IndicatorContribution {
    /// Indicator id.
    pub indicator_id: String,
    /// Indicator label.
    pub label: String,
    /// APS axis.
    pub axis: Axis,
    /// Component name.
    pub component: String,
    /// Source dataflow id.
    pub source_dataflow_id: String,
    /// Source measure id.
    pub measure_id: String,
    /// Dimension selector used for this input.
    pub dimensions: BTreeMap<String, String>,
    /// Resolved series key, when available.
    pub series_key: Option<String>,
    /// Resolved source artifact id, when available.
    pub source_artifact_id: Option<String>,
    /// Latest resolved period, when available.
    pub latest_period: Option<String>,
    /// Display unit.
    pub unit: String,
    /// Raw observation value, when resolved.
    pub raw_value: Option<f64>,
    /// Normalized value in the axis scale, when scored.
    pub normalized_value: Option<f64>,
    /// Configured weight.
    pub weight: f64,
    /// Normalization direction.
    pub direction: Direction,
    /// Coverage status.
    pub coverage_status: CoverageStatus,
    /// Contribution confidence.
    pub confidence: Confidence,
    /// License metadata.
    pub license: String,
    /// Attribution metadata.
    pub attribution: String,
    /// Source URL.
    pub source_url: String,
    /// Optional notes.
    pub notes: Option<String>,
}

/// Full APS snapshot produced by the pure scorer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ScorecardSnapshot {
    /// Scorecard id.
    pub scorecard_id: String,
    /// Config version used for the score.
    pub config_version: String,
    /// Snapshot timestamp or date chosen by the caller.
    pub as_of: String,
    /// Latest period represented by resolved inputs.
    pub latest_period: Option<String>,
    /// APS score in `0..100`.
    pub score: f64,
    /// Score zone.
    pub zone: ScoreZone,
    /// Trend relative to prior comparable score.
    pub trend: Trend,
    /// Worst/best APS confidence band.
    pub confidence_band: ConfidenceBand,
    /// Snapshot confidence label.
    pub confidence: Confidence,
    /// Weight-aware coverage percentage.
    pub coverage_pct: f64,
    /// Axis-level scores.
    pub sub_indexes: Vec<SubIndexScore>,
    /// Indicator-level contribution rows.
    pub contributions: Vec<IndicatorContribution>,
}

/// Errors returned by scorecard config parsing, validation, and scoring.
#[derive(Debug, Error)]
pub enum ScorecardError {
    /// TOML config parse failure.
    #[error("scorecard config parse failed: {0}")]
    ConfigParse(#[from] toml::de::Error),
    /// Config failed semantic validation.
    #[error("invalid scorecard config: {0}")]
    InvalidConfig(String),
    /// Runtime input referenced an unknown indicator.
    #[error("unknown scorecard indicator `{0}`")]
    UnknownIndicator(String),
}
