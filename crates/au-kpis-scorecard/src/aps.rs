use std::collections::{BTreeMap, BTreeSet};

use crate::{
    config::validate_config,
    model::{
        Axis, ComponentScore, Confidence, ConfidenceBand, CoverageStatus, Direction,
        IndicatorConfig, IndicatorContribution, ScoreZone, ScorecardConfig, ScorecardError,
        ScorecardSnapshot, SubIndexScore, Trend,
    },
};

/// Runtime observation data resolved for one configured indicator.
#[derive(Debug, Clone, PartialEq)]
pub struct IndicatorObservation {
    /// Indicator id matching config.
    pub indicator_id: String,
    /// Raw observation value, when resolved.
    pub raw_value: Option<f64>,
    /// Runtime coverage status.
    pub coverage_status: CoverageStatus,
    /// Latest period, when resolved.
    pub latest_period: Option<String>,
    /// Series key, when resolved.
    pub series_key: Option<String>,
    /// Source artifact id, when resolved.
    pub source_artifact_id: Option<String>,
    /// Optional runtime note.
    pub notes: Option<String>,
}

impl IndicatorObservation {
    /// Build a resolved runtime observation.
    pub fn resolved(indicator_id: impl Into<String>, raw_value: f64) -> Self {
        Self {
            indicator_id: indicator_id.into(),
            raw_value: Some(raw_value),
            coverage_status: CoverageStatus::Resolved,
            latest_period: None,
            series_key: None,
            source_artifact_id: None,
            notes: None,
        }
    }

    /// Build a missing runtime observation.
    pub fn missing(indicator_id: impl Into<String>, coverage_status: CoverageStatus) -> Self {
        Self {
            indicator_id: indicator_id.into(),
            raw_value: None,
            coverage_status,
            latest_period: None,
            series_key: None,
            source_artifact_id: None,
            notes: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AxisTotals {
    expected_weight: f64,
    resolved_weight: f64,
    point_weighted_sum: f64,
    band_low_weighted_sum: f64,
    band_high_weighted_sum: f64,
}

impl AxisTotals {
    const fn empty() -> Self {
        Self {
            expected_weight: 0.0,
            resolved_weight: 0.0,
            point_weighted_sum: 0.0,
            band_low_weighted_sum: 0.0,
            band_high_weighted_sum: 0.0,
        }
    }
}

/// Score an APS snapshot from config and resolved indicator observations.
pub fn score_aps_snapshot(
    config: &ScorecardConfig,
    observations: &[IndicatorObservation],
    as_of: impl Into<String>,
    previous_score: Option<f64>,
) -> Result<ScorecardSnapshot, ScorecardError> {
    validate_config(config)?;
    validate_observation_ids(config, observations)?;

    let by_id: BTreeMap<&str, &IndicatorObservation> = observations
        .iter()
        .map(|observation| (observation.indicator_id.as_str(), observation))
        .collect();

    let mut throughput = AxisTotals::empty();
    let mut orientation = AxisTotals::empty();
    let mut component_totals: BTreeMap<(Axis, String), AxisTotals> = BTreeMap::new();
    let mut contributions = Vec::with_capacity(config.indicators.len());
    let mut confidence = Confidence::High;
    let mut latest_period = None;

    for indicator in &config.indicators {
        let observation = by_id.get(indicator.indicator_id.as_str()).copied();
        let status = observation.map_or(indicator.coverage_status, |value| value.coverage_status);
        let raw_value = observation.and_then(|value| value.raw_value);
        let normalized = normalized_value(indicator, raw_value, status)?;
        let scored =
            !status.is_visible_unscored() && !indicator.coverage_status.is_visible_unscored();

        if scored {
            if normalized.is_some() {
                let contribution_confidence = if status == CoverageStatus::Stale {
                    indicator.confidence.worst(Confidence::Medium)
                } else {
                    indicator.confidence
                };
                confidence = confidence.worst(contribution_confidence);
            }
            add_axis_contribution(
                axis_totals_mut(indicator.axis, &mut throughput, &mut orientation),
                indicator,
                normalized,
            );
            add_axis_contribution(
                component_totals
                    .entry((indicator.axis, indicator.component.clone()))
                    .or_insert_with(AxisTotals::empty),
                indicator,
                normalized,
            );
        }

        if let Some(period) = observation.and_then(|value| value.latest_period.clone()) {
            latest_period = latest_period.max(Some(period));
        }

        contributions.push(contribution_row(
            indicator,
            observation,
            status,
            raw_value,
            normalized,
        ));
    }

    let throughput_score = axis_point_score(throughput, Axis::Throughput);
    let orientation_score = axis_point_score(orientation, Axis::Orientation);
    let score = aps_score(throughput_score, orientation_score);
    let score_band = ConfidenceBand {
        low: aps_score(
            axis_band_score(throughput, Axis::Throughput, true),
            axis_band_score(orientation, Axis::Orientation, true),
        ),
        high: aps_score(
            axis_band_score(throughput, Axis::Throughput, false),
            axis_band_score(orientation, Axis::Orientation, false),
        ),
    };
    let snapshot_coverage_pct = coverage_pct(
        throughput.resolved_weight + orientation.resolved_weight,
        throughput.expected_weight + orientation.expected_weight,
    );
    if snapshot_coverage_pct < 100.0 {
        confidence = confidence.worst(Confidence::Medium);
    }
    if snapshot_coverage_pct < 50.0 {
        confidence = confidence.worst(Confidence::Low);
    }

    let sub_indexes = [Axis::Throughput, Axis::Orientation]
        .into_iter()
        .map(|axis| {
            let totals = match axis {
                Axis::Throughput => throughput,
                Axis::Orientation => orientation,
            };
            SubIndexScore {
                axis,
                score: axis_point_score(totals, axis),
                coverage_pct: coverage_pct(totals.resolved_weight, totals.expected_weight),
                weight: totals.expected_weight,
                confidence_band: ConfidenceBand {
                    low: axis_band_score(totals, axis, true),
                    high: axis_band_score(totals, axis, false),
                },
                components: component_scores(axis, &component_totals),
            }
        })
        .collect();

    Ok(ScorecardSnapshot {
        scorecard_id: config.id.clone(),
        config_version: config.version.clone(),
        as_of: as_of.into(),
        latest_period,
        score,
        zone: score_zone(score),
        trend: trend_from_scores(score, previous_score),
        confidence_band: score_band,
        confidence,
        coverage_pct: snapshot_coverage_pct,
        sub_indexes,
        contributions,
    })
}

fn validate_observation_ids(
    config: &ScorecardConfig,
    observations: &[IndicatorObservation],
) -> Result<(), ScorecardError> {
    let known: BTreeSet<&str> = config
        .indicators
        .iter()
        .map(|indicator| indicator.indicator_id.as_str())
        .collect();
    for observation in observations {
        if !known.contains(observation.indicator_id.as_str()) {
            return Err(ScorecardError::UnknownIndicator(
                observation.indicator_id.clone(),
            ));
        }
    }
    Ok(())
}

fn axis_totals_mut<'a>(
    axis: Axis,
    throughput: &'a mut AxisTotals,
    orientation: &'a mut AxisTotals,
) -> &'a mut AxisTotals {
    match axis {
        Axis::Throughput => throughput,
        Axis::Orientation => orientation,
    }
}

fn add_axis_contribution(
    totals: &mut AxisTotals,
    indicator: &IndicatorConfig,
    normalized: Option<f64>,
) {
    totals.expected_weight += indicator.weight;
    if let Some(normalized) = normalized {
        totals.resolved_weight += indicator.weight;
        totals.point_weighted_sum += normalized * indicator.weight;
        totals.band_low_weighted_sum += normalized * indicator.weight;
        totals.band_high_weighted_sum += normalized * indicator.weight;
    } else {
        totals.band_low_weighted_sum += missing_axis_bound(indicator.axis, true) * indicator.weight;
        totals.band_high_weighted_sum +=
            missing_axis_bound(indicator.axis, false) * indicator.weight;
    }
}

fn contribution_row(
    indicator: &IndicatorConfig,
    observation: Option<&IndicatorObservation>,
    status: CoverageStatus,
    raw_value: Option<f64>,
    normalized: Option<f64>,
) -> IndicatorContribution {
    IndicatorContribution {
        indicator_id: indicator.indicator_id.clone(),
        label: indicator.display_label.clone(),
        axis: indicator.axis,
        component: indicator.component.clone(),
        source_dataflow_id: indicator.source_dataflow_id.clone(),
        measure_id: indicator.measure_id.clone(),
        dimensions: indicator.dimension_selector.clone(),
        series_key: observation.and_then(|value| value.series_key.clone()),
        source_artifact_id: observation.and_then(|value| value.source_artifact_id.clone()),
        latest_period: observation.and_then(|value| value.latest_period.clone()),
        unit: indicator.unit.clone(),
        raw_value,
        normalized_value: normalized,
        weight: indicator.weight,
        direction: indicator.direction,
        coverage_status: status,
        confidence: indicator.confidence,
        license: indicator.provenance.license.clone(),
        attribution: indicator.provenance.attribution.clone(),
        source_url: indicator.provenance.source_url.clone(),
        notes: observation
            .and_then(|value| value.notes.clone())
            .or_else(|| indicator.provenance.notes.clone()),
    }
}

fn component_scores(
    axis: Axis,
    component_totals: &BTreeMap<(Axis, String), AxisTotals>,
) -> Vec<ComponentScore> {
    component_totals
        .iter()
        .filter(|((component_axis, _), _)| *component_axis == axis)
        .map(|((_, component), totals)| ComponentScore {
            component: component.clone(),
            score: axis_point_score(*totals, axis),
            weight: totals.expected_weight,
            coverage_pct: coverage_pct(totals.resolved_weight, totals.expected_weight),
        })
        .collect()
}

fn normalized_value(
    indicator: &IndicatorConfig,
    raw_value: Option<f64>,
    status: CoverageStatus,
) -> Result<Option<f64>, ScorecardError> {
    let Some(raw_value) = raw_value else {
        return Ok(None);
    };
    if !status.can_score() || status.is_visible_unscored() {
        return Ok(None);
    }
    let unit_value = normalize_unit_interval(indicator, raw_value)?;
    Ok(Some(match indicator.axis {
        Axis::Throughput => unit_value,
        Axis::Orientation => unit_value.mul_add(2.0, -1.0),
    }))
}

fn normalize_unit_interval(
    indicator: &IndicatorConfig,
    raw_value: f64,
) -> Result<f64, ScorecardError> {
    if !raw_value.is_finite() {
        return Err(ScorecardError::InvalidConfig(format!(
            "indicator `{}` raw value must be finite",
            indicator.indicator_id
        )));
    }
    let worst = indicator.normalization.worst;
    let best = indicator.normalization.best;
    let normalized = match indicator.direction {
        Direction::HigherIsBetter => (raw_value - worst) / (best - worst),
        Direction::LowerIsBetter => (worst - raw_value) / (worst - best),
    };
    Ok(normalized.clamp(0.0, 1.0))
}

fn axis_point_score(totals: AxisTotals, axis: Axis) -> f64 {
    if totals.resolved_weight <= 0.0 {
        return match axis {
            Axis::Throughput => 0.0,
            Axis::Orientation => 0.0,
        };
    }
    totals.point_weighted_sum / totals.resolved_weight
}

fn axis_band_score(totals: AxisTotals, axis: Axis, low: bool) -> f64 {
    if totals.expected_weight <= 0.0 {
        return axis_point_score(totals, axis);
    }
    let value = if low {
        totals.band_low_weighted_sum
    } else {
        totals.band_high_weighted_sum
    } / totals.expected_weight;
    match axis {
        Axis::Throughput => value.clamp(0.0, 1.0),
        Axis::Orientation => value.clamp(-1.0, 1.0),
    }
}

fn missing_axis_bound(axis: Axis, low: bool) -> f64 {
    match (axis, low) {
        (Axis::Throughput, true) => 0.0,
        (Axis::Throughput, false) => 1.0,
        (Axis::Orientation, true) => -1.0,
        (Axis::Orientation, false) => 1.0,
    }
}

fn coverage_pct(resolved_weight: f64, expected_weight: f64) -> f64 {
    if expected_weight <= 0.0 {
        100.0
    } else {
        (resolved_weight / expected_weight * 100.0).clamp(0.0, 100.0)
    }
}

/// Compute the APS formula from axis scores.
pub fn aps_score(throughput: f64, orientation: f64) -> f64 {
    let throughput = throughput.clamp(0.0, 1.0);
    let orientation = orientation.clamp(-1.0, 1.0);
    (100.0 * throughput * (0.5 + 0.5 * orientation)).clamp(0.0, 100.0)
}

/// Assign an inclusive score zone.
pub fn score_zone(score: f64) -> ScoreZone {
    if score <= 33.0 {
        ScoreZone::Red
    } else if score <= 66.0 {
        ScoreZone::Yellow
    } else {
        ScoreZone::Green
    }
}

/// Compute the trend arrow from latest and prior comparable scores.
pub fn trend_from_scores(latest: f64, previous: Option<f64>) -> Trend {
    let Some(previous) = previous else {
        return Trend::Unavailable;
    };
    let delta = latest - previous;
    if delta >= 1.0 {
        Trend::Up
    } else if delta <= -1.0 {
        Trend::Down
    } else {
        Trend::Flat
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::{
        config::{load_aps_v1_config, validate_config},
        model::{Normalization, Provenance},
    };

    #[derive(Debug, Clone, Copy)]
    struct IndicatorFixture {
        id: &'static str,
        axis: Axis,
        component: &'static str,
        weight: f64,
        direction: Direction,
        worst: f64,
        best: f64,
        confidence: Confidence,
        coverage_status: CoverageStatus,
    }

    fn test_config() -> ScorecardConfig {
        ScorecardConfig {
            id: "aps".into(),
            version: "test".into(),
            label: "APS".into(),
            description: "test config".into(),
            formula: "APS = 100 * T * (0.5 + 0.5 * O)".into(),
            license: "Apache-2.0".into(),
            attribution: "test".into(),
            indicators: vec![
                indicator(IndicatorFixture {
                    id: "throughput.fast",
                    axis: Axis::Throughput,
                    component: "delivery",
                    weight: 1.0,
                    direction: Direction::HigherIsBetter,
                    worst: 0.0,
                    best: 100.0,
                    confidence: Confidence::High,
                    coverage_status: CoverageStatus::MissingExpected,
                }),
                indicator(IndicatorFixture {
                    id: "throughput.wait",
                    axis: Axis::Throughput,
                    component: "delivery",
                    weight: 1.0,
                    direction: Direction::LowerIsBetter,
                    worst: 100.0,
                    best: 0.0,
                    confidence: Confidence::High,
                    coverage_status: CoverageStatus::MissingExpected,
                }),
                indicator(IndicatorFixture {
                    id: "orientation.ready",
                    axis: Axis::Orientation,
                    component: "settings",
                    weight: 1.0,
                    direction: Direction::HigherIsBetter,
                    worst: 0.0,
                    best: 100.0,
                    confidence: Confidence::High,
                    coverage_status: CoverageStatus::MissingExpected,
                }),
                indicator(IndicatorFixture {
                    id: "orientation.low_confidence",
                    axis: Axis::Orientation,
                    component: "settings",
                    weight: 1.0,
                    direction: Direction::HigherIsBetter,
                    worst: 0.0,
                    best: 100.0,
                    confidence: Confidence::Low,
                    coverage_status: CoverageStatus::MissingExpected,
                }),
                indicator(IndicatorFixture {
                    id: "context.unscored",
                    axis: Axis::Orientation,
                    component: "settings",
                    weight: 0.0,
                    direction: Direction::HigherIsBetter,
                    worst: 0.0,
                    best: 100.0,
                    confidence: Confidence::Low,
                    coverage_status: CoverageStatus::VisibleUnscored,
                }),
            ],
        }
    }

    fn indicator(fixture: IndicatorFixture) -> IndicatorConfig {
        IndicatorConfig {
            indicator_id: fixture.id.into(),
            source_dataflow_id: format!("source.{}", fixture.id),
            measure_id: "value".into(),
            dimension_selector: [("region".into(), "AUS".into())].into_iter().collect(),
            axis: fixture.axis,
            component: fixture.component.into(),
            weight: fixture.weight,
            direction: fixture.direction,
            normalization: Normalization {
                worst: fixture.worst,
                best: fixture.best,
            },
            display_label: fixture.id.into(),
            description: "test indicator".into(),
            unit: "index".into(),
            confidence: fixture.confidence,
            coverage_status: fixture.coverage_status,
            cadence: "monthly".into(),
            provenance: Provenance {
                source_url: "https://example.test/source".into(),
                license: "CC-BY-4.0".into(),
                attribution: "example".into(),
                retrieved_at: Some("2026-06-22".into()),
                reviewed_by: Some("aps-test".into()),
                reviewed_at: Some("2026-06-22".into()),
                notes: None,
            },
        }
    }

    fn resolved(id: &str, value: f64) -> IndicatorObservation {
        IndicatorObservation::resolved(id, value)
    }

    #[test]
    fn checked_in_aps_config_loads_and_validates() {
        let config = load_aps_v1_config().expect("load APS v1 config");

        assert_eq!(config.id, "aps");
        assert_eq!(config.version, "aps.v1");
        assert!(
            config
                .indicators
                .iter()
                .any(|indicator| indicator.coverage_status == CoverageStatus::VisibleUnscored)
        );
        assert!(config.indicators.iter().any(|indicator| {
            indicator.indicator_id == "housing.commencements"
                && indicator.source_dataflow_id == "abs.building_activity"
                && indicator.measure_id == "dwellings_commenced"
                && indicator
                    .dimension_selector
                    .get("region")
                    .map(String::as_str)
                    == Some("AUS")
                && indicator
                    .dimension_selector
                    .get("measure")
                    .map(String::as_str)
                    == Some("dwellings_commenced")
        }));
        assert!(config.indicators.iter().any(|indicator| {
            indicator.indicator_id == "construction.completion-time"
                && indicator.source_dataflow_id == "abs.dwelling_completion_times"
                && indicator.measure_id == "average_completion_months"
                && indicator
                    .dimension_selector
                    .get("region")
                    .map(String::as_str)
                    == Some("AUS")
                && indicator
                    .dimension_selector
                    .get("dwelling_type")
                    .map(String::as_str)
                    == Some("apartments")
                && indicator
                    .dimension_selector
                    .get("measure")
                    .map(String::as_str)
                    == Some("average_completion_months")
        }));
        assert!(config.indicators.iter().any(|indicator| {
            indicator.indicator_id == "energy.price"
                && indicator.source_dataflow_id == "aemo.dispatch"
                && indicator.measure_id == "value"
                && indicator
                    .dimension_selector
                    .get("region")
                    .map(String::as_str)
                    == Some("NSW1")
                && indicator
                    .dimension_selector
                    .get("metric")
                    .map(String::as_str)
                    == Some("regional_reference_price")
        }));
        assert!(config.indicators.iter().any(|indicator| {
            indicator.indicator_id == "energy.renewable-generation"
                && indicator.source_dataflow_id == "aemo.generation_mix"
                && indicator.measure_id == "generation_mw"
                && indicator
                    .dimension_selector
                    .get("fuel_type")
                    .map(String::as_str)
                    == Some("wind")
        }));
        assert!(config.indicators.iter().any(|indicator| {
            indicator.indicator_id == "energy.dispatchable-capacity"
                && indicator.source_dataflow_id == "aemo.dispatchability_capacity"
                && indicator.measure_id == "value"
                && indicator
                    .dimension_selector
                    .get("metric")
                    .map(String::as_str)
                    == Some("dispatchable_capacity")
        }));
        let oversight = config
            .indicators
            .iter()
            .find(|indicator| indicator.indicator_id == "oversight.reviewed-strength")
            .expect("oversight curated input");
        assert_eq!(oversight.coverage_status, CoverageStatus::ManualPending);
        assert!(oversight.provenance.retrieved_at.is_some());
        assert!(oversight.provenance.reviewed_by.is_some());
        assert!(oversight.provenance.reviewed_at.is_some());
        assert!(config.indicators.iter().any(|indicator| {
            indicator.indicator_id == "compute.datacentre-capacity"
                && indicator.source_dataflow_id == "compute.au_datacentre_capacity_mw"
                && indicator.confidence == Confidence::Low
        }));
        assert!(config.indicators.iter().any(|indicator| {
            indicator.indicator_id == "surveillance.intensity"
                && indicator.coverage_status == CoverageStatus::VisibleUnscored
        }));
    }

    #[test]
    fn aps_formula_returns_scores_in_declared_bounds() {
        assert_eq!(aps_score(0.0, -1.0), 0.0);
        assert_eq!(aps_score(1.0, 1.0), 100.0);
        assert_eq!(aps_score(0.5, 0.0), 25.0);
        assert_eq!(aps_score(2.0, 2.0), 100.0);
    }

    #[test]
    fn zones_use_inclusive_boundaries() {
        assert_eq!(score_zone(0.0), ScoreZone::Red);
        assert_eq!(score_zone(33.0), ScoreZone::Red);
        assert_eq!(score_zone(34.0), ScoreZone::Yellow);
        assert_eq!(score_zone(66.0), ScoreZone::Yellow);
        assert_eq!(score_zone(67.0), ScoreZone::Green);
        assert_eq!(score_zone(100.0), ScoreZone::Green);
    }

    #[test]
    fn missing_inputs_are_excluded_from_point_estimates() {
        let config = test_config();
        let snapshot = score_aps_snapshot(
            &config,
            &[
                resolved("throughput.fast", 50.0),
                IndicatorObservation::missing("throughput.wait", CoverageStatus::MissingExpected),
                resolved("orientation.ready", 50.0),
                resolved("orientation.low_confidence", 50.0),
            ],
            "2026-06-22",
            None,
        )
        .expect("score snapshot");

        assert_eq!(snapshot.score, 25.0);
        assert_eq!(snapshot.coverage_pct, 75.0);
        assert_eq!(
            snapshot
                .contributions
                .iter()
                .find(|row| row.indicator_id == "context.unscored")
                .expect("visible row")
                .normalized_value,
            None
        );
    }

    #[test]
    fn missing_inputs_expand_confidence_band() {
        let config = test_config();
        let snapshot = score_aps_snapshot(
            &config,
            &[
                resolved("throughput.fast", 50.0),
                IndicatorObservation::missing("throughput.wait", CoverageStatus::MissingExpected),
                resolved("orientation.ready", 75.0),
                IndicatorObservation::missing(
                    "orientation.low_confidence",
                    CoverageStatus::CoverageGap,
                ),
            ],
            "2026-06-22",
            None,
        )
        .expect("score snapshot");

        assert!(snapshot.confidence_band.low < snapshot.score);
        assert!(snapshot.confidence_band.high > snapshot.score);
        assert_eq!(snapshot.confidence, Confidence::Medium);
    }

    #[test]
    fn normalization_handles_higher_and_lower_is_better() {
        let config = test_config();
        let snapshot = score_aps_snapshot(
            &config,
            &[
                resolved("throughput.fast", 75.0),
                resolved("throughput.wait", 25.0),
                resolved("orientation.ready", 75.0),
                resolved("orientation.low_confidence", 25.0),
            ],
            "2026-06-22",
            None,
        )
        .expect("score snapshot");

        let fast = snapshot
            .contributions
            .iter()
            .find(|row| row.indicator_id == "throughput.fast")
            .expect("fast contribution");
        let wait = snapshot
            .contributions
            .iter()
            .find(|row| row.indicator_id == "throughput.wait")
            .expect("wait contribution");
        let orientation = snapshot
            .contributions
            .iter()
            .find(|row| row.indicator_id == "orientation.ready")
            .expect("orientation contribution");

        assert_eq!(fast.normalized_value, Some(0.75));
        assert_eq!(wait.normalized_value, Some(0.75));
        assert_eq!(orientation.normalized_value, Some(0.5));
        assert_eq!(snapshot.confidence, Confidence::Low);
    }

    #[test]
    fn coverage_percentage_is_weight_aware_and_deterministic() {
        let mut config = test_config();
        config.indicators[0].weight = 3.0;
        config.indicators[1].weight = 1.0;
        let snapshot = score_aps_snapshot(
            &config,
            &[
                resolved("throughput.fast", 50.0),
                IndicatorObservation::missing("throughput.wait", CoverageStatus::MissingExpected),
                resolved("orientation.ready", 50.0),
                resolved("orientation.low_confidence", 50.0),
            ],
            "2026-06-22",
            None,
        )
        .expect("score snapshot");

        assert_eq!(snapshot.coverage_pct, 5.0 / 6.0 * 100.0);
    }

    #[test]
    fn trend_arrows_follow_one_point_threshold() {
        assert_eq!(trend_from_scores(50.0, None), Trend::Unavailable);
        assert_eq!(trend_from_scores(51.0, Some(50.0)), Trend::Up);
        assert_eq!(trend_from_scores(49.0, Some(50.0)), Trend::Down);
        assert_eq!(trend_from_scores(50.5, Some(50.0)), Trend::Flat);
    }

    #[test]
    fn invalid_configs_return_actionable_errors_without_panicking() {
        let mut duplicate = test_config();
        duplicate.indicators[1].indicator_id = duplicate.indicators[0].indicator_id.clone();
        let err = validate_config(&duplicate).expect_err("duplicate id should fail");
        assert!(err.to_string().contains("duplicate indicator id"));

        let mut bad_weight = test_config();
        bad_weight.indicators[0].weight = 0.0;
        let err = validate_config(&bad_weight).expect_err("zero scored weight should fail");
        assert!(err.to_string().contains("positive finite weight"));

        let mut bad_normalization = test_config();
        bad_normalization.indicators[0].normalization.best = -1.0;
        let err = validate_config(&bad_normalization)
            .expect_err("direction/reference mismatch should fail");
        assert!(err.to_string().contains("normalization references"));

        let mut missing_url = test_config();
        missing_url.indicators[0].provenance.source_url.clear();
        let err = validate_config(&missing_url).expect_err("missing URL should fail");
        assert!(err.to_string().contains("provenance.source_url"));

        let mut missing_review = test_config();
        missing_review.indicators[0].source_dataflow_id = "curated.oversight_strength".into();
        missing_review.indicators[0].coverage_status = CoverageStatus::ManualPending;
        missing_review.indicators[0].provenance.retrieved_at = None;
        missing_review.indicators[0].provenance.reviewed_by = None;
        missing_review.indicators[0].provenance.reviewed_at = None;
        let err = validate_config(&missing_review)
            .expect_err("curated input without review metadata should fail");
        assert!(err.to_string().contains("review metadata"), "{err}");
    }

    proptest! {
        #[test]
        fn generated_valid_inputs_keep_scores_and_bands_in_range(
            t1 in 0.0_f64..100.0,
            t2 in 0.0_f64..100.0,
            o1 in 0.0_f64..100.0,
            o2 in 0.0_f64..100.0,
        ) {
            let config = test_config();
            let snapshot = score_aps_snapshot(
                &config,
                &[
                    resolved("throughput.fast", t1),
                    resolved("throughput.wait", t2),
                    resolved("orientation.ready", o1),
                    resolved("orientation.low_confidence", o2),
                ],
                "2026-06-22",
                None,
            ).expect("score snapshot");

            prop_assert!((0.0..=100.0).contains(&snapshot.score));
            prop_assert!((0.0..=100.0).contains(&snapshot.confidence_band.low));
            prop_assert!((0.0..=100.0).contains(&snapshot.confidence_band.high));
            prop_assert!(snapshot.confidence_band.low <= snapshot.score + f64::EPSILON);
            prop_assert!(snapshot.confidence_band.high + f64::EPSILON >= snapshot.score);
            prop_assert!(matches!(snapshot.zone, ScoreZone::Red | ScoreZone::Yellow | ScoreZone::Green));
        }
    }
}
