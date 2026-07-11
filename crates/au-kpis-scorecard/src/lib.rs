//! APS config validation, scoring, and immutable publication persistence.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

/// APS formula, scoring, normalization, coverage, and trend logic.
pub mod aps;
/// Scorecard config loading and validation.
pub mod config;
/// Serializable scorecard config and snapshot model types.
pub mod model;
/// Immutable APS snapshot persistence and daily materialization.
pub mod store;

pub use aps::{IndicatorObservation, aps_score, score_aps_snapshot, score_zone, trend_from_scores};
pub use config::{APS_V1_CONFIG_TOML, load_aps_v1_config, parse_scorecard_config, validate_config};
pub use model::{
    Axis, ComponentScore, Confidence, ConfidenceBand, CoverageStatus, CoverageThresholds,
    Direction, IndicatorConfig, IndicatorContribution, Normalization, Provenance, ScoreZone,
    ScorecardConfig, ScorecardError, ScorecardSnapshot, SubIndexScore, Trend, ZoneThresholds,
};
pub use store::{
    ApsCorrection, ApsSnapshotSummary, HistoryView, PublicationState, PublishedApsSnapshot,
    ScorecardStoreError, load_aps_history, load_aps_snapshot, load_latest_aps_snapshot,
    materialize_aps_snapshot,
};
