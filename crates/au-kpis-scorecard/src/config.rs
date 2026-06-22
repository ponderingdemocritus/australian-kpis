use std::collections::{BTreeMap, BTreeSet};

use crate::model::{
    Axis, CoverageStatus, Direction, IndicatorConfig, ScorecardConfig, ScorecardError,
};

/// Checked-in APS v1 config.
pub const APS_V1_CONFIG_TOML: &str = include_str!("../config/aps.v1.toml");

/// Load and validate the checked-in APS v1 config.
pub fn load_aps_v1_config() -> Result<ScorecardConfig, ScorecardError> {
    parse_scorecard_config(APS_V1_CONFIG_TOML)
}

/// Parse and validate a TOML scorecard config.
pub fn parse_scorecard_config(raw: &str) -> Result<ScorecardConfig, ScorecardError> {
    let config: ScorecardConfig = toml::from_str(raw)?;
    validate_config(&config)?;
    Ok(config)
}

/// Validate scorecard config invariants that cannot be expressed by serde.
pub fn validate_config(config: &ScorecardConfig) -> Result<(), ScorecardError> {
    require_text("id", &config.id)?;
    require_text("version", &config.version)?;
    require_text("label", &config.label)?;
    require_text("formula", &config.formula)?;
    require_text("license", &config.license)?;
    require_text("attribution", &config.attribution)?;

    if config.indicators.is_empty() {
        return Err(ScorecardError::InvalidConfig(
            "config must define at least one indicator".into(),
        ));
    }

    let mut seen = BTreeSet::new();
    let mut scored_weights: BTreeMap<Axis, f64> = BTreeMap::new();
    for indicator in &config.indicators {
        validate_indicator(indicator, &mut seen, &mut scored_weights)?;
    }

    for axis in [Axis::Throughput, Axis::Orientation] {
        if scored_weights.get(&axis).copied().unwrap_or_default() <= 0.0 {
            return Err(ScorecardError::InvalidConfig(format!(
                "{axis:?} must define positive scored weight"
            )));
        }
    }

    Ok(())
}

fn validate_indicator(
    indicator: &IndicatorConfig,
    seen: &mut BTreeSet<String>,
    scored_weights: &mut BTreeMap<Axis, f64>,
) -> Result<(), ScorecardError> {
    require_text("indicator_id", &indicator.indicator_id)?;
    if !seen.insert(indicator.indicator_id.clone()) {
        return Err(ScorecardError::InvalidConfig(format!(
            "duplicate indicator id `{}`",
            indicator.indicator_id
        )));
    }
    require_text("source_dataflow_id", &indicator.source_dataflow_id)?;
    require_text("measure_id", &indicator.measure_id)?;
    require_text("component", &indicator.component)?;
    require_text("display_label", &indicator.display_label)?;
    require_text("unit", &indicator.unit)?;
    require_text("cadence", &indicator.cadence)?;
    require_text("provenance.source_url", &indicator.provenance.source_url)?;
    require_text("provenance.license", &indicator.provenance.license)?;
    require_text("provenance.attribution", &indicator.provenance.attribution)?;
    validate_curated_review_metadata(indicator)?;
    validate_normalization(indicator)?;

    if indicator.coverage_status.is_visible_unscored() {
        if indicator.weight < 0.0 {
            return Err(ScorecardError::InvalidConfig(format!(
                "visible-unscored indicator `{}` cannot have negative weight",
                indicator.indicator_id
            )));
        }
    } else {
        if !indicator.weight.is_finite() || indicator.weight <= 0.0 {
            return Err(ScorecardError::InvalidConfig(format!(
                "scored indicator `{}` must have positive finite weight",
                indicator.indicator_id
            )));
        }
        *scored_weights.entry(indicator.axis).or_default() += indicator.weight;
    }

    Ok(())
}

fn validate_curated_review_metadata(indicator: &IndicatorConfig) -> Result<(), ScorecardError> {
    if !requires_curated_review_metadata(indicator) {
        return Ok(());
    }
    require_optional_text(
        "provenance.retrieved_at",
        indicator.provenance.retrieved_at.as_deref(),
        &indicator.indicator_id,
    )?;
    require_optional_text(
        "provenance.reviewed_by",
        indicator.provenance.reviewed_by.as_deref(),
        &indicator.indicator_id,
    )?;
    require_optional_text(
        "provenance.reviewed_at",
        indicator.provenance.reviewed_at.as_deref(),
        &indicator.indicator_id,
    )?;
    Ok(())
}

fn requires_curated_review_metadata(indicator: &IndicatorConfig) -> bool {
    indicator.source_dataflow_id.starts_with("curated.")
        || indicator.source_dataflow_id.starts_with("compute.")
        || matches!(
            indicator.coverage_status,
            CoverageStatus::ManualPending | CoverageStatus::VisibleUnscored
        )
}

fn validate_normalization(indicator: &IndicatorConfig) -> Result<(), ScorecardError> {
    let worst = indicator.normalization.worst;
    let best = indicator.normalization.best;
    if !worst.is_finite() || !best.is_finite() {
        return Err(ScorecardError::InvalidConfig(format!(
            "indicator `{}` normalization references must be finite",
            indicator.indicator_id
        )));
    }

    let valid = match indicator.direction {
        Direction::HigherIsBetter => best > worst,
        Direction::LowerIsBetter => best < worst,
    };
    if !valid {
        return Err(ScorecardError::InvalidConfig(format!(
            "indicator `{}` normalization references conflict with direction",
            indicator.indicator_id
        )));
    }

    Ok(())
}

fn require_text(field: &str, value: &str) -> Result<(), ScorecardError> {
    if value.trim().is_empty() {
        Err(ScorecardError::InvalidConfig(format!(
            "`{field}` must not be empty"
        )))
    } else {
        Ok(())
    }
}

fn require_optional_text(
    field: &str,
    value: Option<&str>,
    indicator_id: &str,
) -> Result<(), ScorecardError> {
    if value.is_some_and(|value| !value.trim().is_empty()) {
        Ok(())
    } else {
        Err(ScorecardError::InvalidConfig(format!(
            "curated/manual indicator `{indicator_id}` requires review metadata `{field}`"
        )))
    }
}
