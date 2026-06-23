//! Pure scorecard config validation and APS scoring logic.
//!
//! This crate has no database, HTTP, or async dependencies. API handlers and
//! SDK-facing layers can use these types and functions without duplicating the
//! APS formula or config validation rules.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

/// APS formula, scoring, normalization, coverage, and trend logic.
pub mod aps;
/// Scorecard config loading and validation.
pub mod config;
/// Serializable scorecard config and snapshot model types.
pub mod model;

pub use aps::{IndicatorObservation, aps_score, score_aps_snapshot, score_zone, trend_from_scores};
pub use config::{APS_V1_CONFIG_TOML, load_aps_v1_config, parse_scorecard_config, validate_config};
pub use model::{
    Axis, ComponentScore, Confidence, ConfidenceBand, CoverageStatus, Direction, IndicatorConfig,
    IndicatorContribution, Normalization, Provenance, ScoreZone, ScorecardConfig, ScorecardError,
    ScorecardSnapshot, SubIndexScore, Trend,
};
