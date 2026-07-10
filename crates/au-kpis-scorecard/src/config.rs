use std::collections::{BTreeMap, BTreeSet};

use au_kpis_source_register::load_source_register;
use sha2::{Digest, Sha256};

use crate::model::{
    Axis, CoverageStatus, Direction, IndicatorConfig, ScorecardConfig, ScorecardError,
};

/// Checked-in APS v1 config.
pub const APS_V1_CONFIG_TOML: &str = include_str!("../config/aps.v1.toml");

/// Load and validate the checked-in APS v1 config.
pub fn load_aps_v1_config() -> Result<ScorecardConfig, ScorecardError> {
    let mut config = parse_scorecard_config(APS_V1_CONFIG_TOML)?;
    let register =
        load_source_register().map_err(|error| ScorecardError::InvalidConfig(error.to_string()))?;
    for indicator in &mut config.indicators {
        if let Some(policy) = register
            .dataflows
            .iter()
            .find(|entry| entry.dataflow_id == indicator.source_dataflow_id)
            .and_then(|entry| entry.freshness_policy.as_ref())
        {
            indicator.soft_after_seconds = policy.soft_after_seconds;
            indicator.hard_after_seconds = policy.hard_after_seconds;
        }
    }
    refresh_digest(&mut config)?;
    validate_config(&config)?;
    Ok(config)
}

/// Parse and validate a TOML scorecard config.
pub fn parse_scorecard_config(raw: &str) -> Result<ScorecardConfig, ScorecardError> {
    let mut config: ScorecardConfig = toml::from_str(raw)?;
    for indicator in &mut config.indicators {
        if indicator.soft_after_seconds == 0 || indicator.hard_after_seconds == 0 {
            (indicator.soft_after_seconds, indicator.hard_after_seconds) =
                cadence_freshness_seconds(&indicator.cadence);
        }
    }
    refresh_digest(&mut config)?;
    validate_config(&config)?;
    Ok(config)
}

/// Validate scorecard config invariants that cannot be expressed by serde.
pub fn validate_config(config: &ScorecardConfig) -> Result<(), ScorecardError> {
    require_text("id", &config.id)?;
    require_text("version", &config.version)?;
    require_text("label", &config.label)?;
    require_text("formula", &config.formula)?;
    require_text("methodology_citation", &config.methodology_citation)?;
    require_text("license", &config.license)?;
    require_text("attribution", &config.attribution)?;
    if !config.coverage_thresholds.overall_pct.is_finite()
        || !(0.0..=100.0).contains(&config.coverage_thresholds.overall_pct)
        || !config.coverage_thresholds.axis_pct.is_finite()
        || !(0.0..=100.0).contains(&config.coverage_thresholds.axis_pct)
    {
        return Err(ScorecardError::InvalidConfig(
            "coverage thresholds must be finite percentages".into(),
        ));
    }
    if !config.zone_thresholds.scarcity_max.is_finite()
        || !config.zone_thresholds.mixed_max.is_finite()
        || config.zone_thresholds.scarcity_max < 0.0
        || config.zone_thresholds.scarcity_max >= config.zone_thresholds.mixed_max
        || config.zone_thresholds.mixed_max > 100.0
    {
        return Err(ScorecardError::InvalidConfig(
            "zone thresholds must be finite, ordered, and inside 0..=100".into(),
        ));
    }
    if !config.trend_threshold.is_finite() || config.trend_threshold <= 0.0 {
        return Err(ScorecardError::InvalidConfig(
            "trend threshold must be positive and finite".into(),
        ));
    }
    if !config.digest.is_empty()
        && !matches!(hex::decode(&config.digest), Ok(bytes) if bytes.len() == 32)
    {
        return Err(ScorecardError::InvalidConfig(
            "config digest must be a SHA-256 hex value".into(),
        ));
    }

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
    if !matches!(
        indicator.cadence.as_str(),
        "5-minute" | "daily" | "weekly" | "monthly" | "quarterly" | "annual"
    ) {
        return Err(ScorecardError::InvalidConfig(format!(
            "indicator `{}` has unknown cadence `{}`",
            indicator.indicator_id, indicator.cadence
        )));
    }
    if indicator.soft_after_seconds == 0
        || indicator.hard_after_seconds <= indicator.soft_after_seconds
    {
        return Err(ScorecardError::InvalidConfig(format!(
            "indicator `{}` freshness hard threshold must exceed its positive soft threshold",
            indicator.indicator_id
        )));
    }
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

fn refresh_digest(config: &mut ScorecardConfig) -> Result<(), ScorecardError> {
    config.digest.clear();
    config.digest = hex::encode(Sha256::digest(serde_json::to_vec(config)?));
    Ok(())
}

fn cadence_freshness_seconds(cadence: &str) -> (u64, u64) {
    match cadence {
        "5-minute" => (900, 3_600),
        "daily" => (2 * 86_400, 4 * 86_400),
        "weekly" => (14 * 86_400, 28 * 86_400),
        "monthly" => (45 * 86_400, 90 * 86_400),
        "quarterly" => (120 * 86_400, 240 * 86_400),
        "annual" => (400 * 86_400, 800 * 86_400),
        _ => (0, 0),
    }
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
