//! Coverage report generation for local and production ingestion runs.

use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CoverageStatus {
    Loaded,
    Partial,
    Failed,
    ZeroRows,
    MissingExpected,
    CoverageGap,
    ManualPending,
    VisibleUnscored,
    Stale,
}

impl CoverageStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Loaded => "loaded",
            Self::Partial => "partial",
            Self::Failed => "failed",
            Self::ZeroRows => "zero_rows",
            Self::MissingExpected => "missing_expected",
            Self::CoverageGap => "coverage_gap",
            Self::ManualPending => "manual_pending",
            Self::VisibleUnscored => "visible_unscored",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawCoverageRow {
    pub(crate) source_id: String,
    pub(crate) dataflow_id: String,
    pub(crate) name: String,
    pub(crate) source_url: String,
    pub(crate) series_count: i64,
    pub(crate) artifact_count: i64,
    pub(crate) observations_loaded: i64,
    pub(crate) parse_errors: i64,
    pub(crate) latest_load: Option<String>,
    pub(crate) expected_status: Option<CoverageStatus>,
    pub(crate) governance_status: String,
    pub(crate) launch_required: bool,
    pub(crate) freshness_hard_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CoverageDataflow {
    pub(crate) source_id: String,
    pub(crate) dataflow_id: String,
    pub(crate) name: String,
    pub(crate) source_url: String,
    pub(crate) series_count: i64,
    pub(crate) artifact_count: i64,
    pub(crate) observations_loaded: i64,
    pub(crate) parse_errors: i64,
    pub(crate) latest_load: Option<String>,
    pub(crate) status: CoverageStatus,
    pub(crate) status_reason: String,
    pub(crate) governance_status: String,
    pub(crate) launch_required: bool,
    pub(crate) launch_ready: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct CoverageTotals {
    pub(crate) dataflows: usize,
    pub(crate) loaded: usize,
    pub(crate) partial: usize,
    pub(crate) failed: usize,
    pub(crate) zero_rows: usize,
    pub(crate) missing_expected: usize,
    pub(crate) coverage_gap: usize,
    pub(crate) manual_pending: usize,
    pub(crate) visible_unscored: usize,
    pub(crate) stale: usize,
    pub(crate) launch_required: usize,
    pub(crate) launch_ready: usize,
    pub(crate) launch_blockers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CoverageReport {
    pub(crate) dataflows: Vec<CoverageDataflow>,
    pub(crate) totals: CoverageTotals,
}

pub(crate) fn build_report(rows: Vec<RawCoverageRow>) -> CoverageReport {
    build_report_at(rows, Utc::now())
}

fn build_report_at(rows: Vec<RawCoverageRow>, now: DateTime<Utc>) -> CoverageReport {
    let dataflows = rows
        .into_iter()
        .map(|row| {
            let (mut status, mut status_reason) = classify_status(
                row.artifact_count,
                row.observations_loaded,
                row.parse_errors,
                row.expected_status,
            );
            let fresh = row.freshness_hard_seconds.is_none_or(|hard_seconds| {
                row.latest_load
                    .as_deref()
                    .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                    .is_some_and(|latest| {
                        now.signed_duration_since(latest.with_timezone(&Utc))
                            <= chrono::Duration::seconds(
                                i64::try_from(hard_seconds).unwrap_or(i64::MAX),
                            )
                    })
            });
            if row.launch_required && status == CoverageStatus::Loaded && !fresh {
                status = CoverageStatus::Stale;
                status_reason = match (&row.latest_load, row.freshness_hard_seconds) {
                    (Some(latest), Some(seconds)) => format!(
                        "latest load at {latest} exceeds the {seconds}-second hard freshness limit"
                    ),
                    _ => "no timestamp is available to prove launch freshness".to_string(),
                };
            }
            let launch_ready = row.launch_required && status == CoverageStatus::Loaded;
            CoverageDataflow {
                source_id: row.source_id,
                dataflow_id: row.dataflow_id,
                name: row.name,
                source_url: row.source_url,
                series_count: row.series_count,
                artifact_count: row.artifact_count,
                observations_loaded: row.observations_loaded,
                parse_errors: row.parse_errors,
                latest_load: row.latest_load,
                status,
                status_reason,
                governance_status: row.governance_status,
                launch_required: row.launch_required,
                launch_ready,
            }
        })
        .collect::<Vec<_>>();
    let totals = dataflows.iter().fold(
        CoverageTotals {
            dataflows: dataflows.len(),
            ..CoverageTotals::default()
        },
        |mut totals, dataflow| {
            match dataflow.status {
                CoverageStatus::Loaded => totals.loaded += 1,
                CoverageStatus::Partial => totals.partial += 1,
                CoverageStatus::Failed => totals.failed += 1,
                CoverageStatus::ZeroRows => totals.zero_rows += 1,
                CoverageStatus::MissingExpected => totals.missing_expected += 1,
                CoverageStatus::CoverageGap => totals.coverage_gap += 1,
                CoverageStatus::ManualPending => totals.manual_pending += 1,
                CoverageStatus::VisibleUnscored => totals.visible_unscored += 1,
                CoverageStatus::Stale => totals.stale += 1,
            }
            if dataflow.launch_required {
                totals.launch_required += 1;
                if dataflow.launch_ready {
                    totals.launch_ready += 1;
                } else {
                    totals.launch_blockers += 1;
                }
            }
            totals
        },
    );
    CoverageReport { dataflows, totals }
}

pub(crate) fn render_markdown(report: &CoverageReport) -> String {
    let mut markdown = String::from("# Ingestion Coverage Report\n\n");
    markdown.push_str("| Metric | Count |\n|---|---:|\n");
    markdown.push_str(&format!("| Dataflows | {} |\n", report.totals.dataflows));
    markdown.push_str(&format!("| Loaded | {} |\n", report.totals.loaded));
    markdown.push_str(&format!("| Partial | {} |\n", report.totals.partial));
    markdown.push_str(&format!("| Failed | {} |\n", report.totals.failed));
    markdown.push_str(&format!("| Zero rows | {} |\n", report.totals.zero_rows));
    markdown.push_str(&format!(
        "| Missing expected | {} |\n",
        report.totals.missing_expected
    ));
    markdown.push_str(&format!(
        "| Coverage gaps | {} |\n",
        report.totals.coverage_gap
    ));
    markdown.push_str(&format!(
        "| Manual pending | {} |\n",
        report.totals.manual_pending
    ));
    markdown.push_str(&format!(
        "| Visible unscored | {} |\n",
        report.totals.visible_unscored
    ));
    markdown.push_str(&format!("| Stale | {} |\n", report.totals.stale));
    markdown.push_str(&format!(
        "| Launch required | {} |\n| Launch ready | {} |\n| Launch blockers | {} |\n\n",
        report.totals.launch_required, report.totals.launch_ready, report.totals.launch_blockers
    ));
    markdown.push_str(
        "| Dataflow | Source | Governance | Status | Launch required | Artifacts | Loaded rows | Parse errors | Reason |\n\
         |---|---|---|---|---:|---:|---:|---:|---|\n",
    );
    for dataflow in &report.dataflows {
        markdown.push_str(&format!(
            "| `{}` | `{}` | `{}` | `{}` | {} | {} | {} | {} | {} |\n",
            dataflow.dataflow_id,
            dataflow.source_id,
            dataflow.governance_status,
            dataflow.status.as_str(),
            dataflow.launch_required,
            dataflow.artifact_count,
            dataflow.observations_loaded,
            dataflow.parse_errors,
            escape_markdown_table_cell(&dataflow.status_reason)
        ));
    }
    markdown
}

fn classify_status(
    artifact_count: i64,
    observations_loaded: i64,
    parse_errors: i64,
    expected_status: Option<CoverageStatus>,
) -> (CoverageStatus, String) {
    if observations_loaded > 0 && parse_errors > 0 {
        return (
            CoverageStatus::Partial,
            format!(
                "loaded {observations_loaded} {} with {parse_errors} {}",
                pluralize(observations_loaded, "observation", "observations"),
                pluralize(parse_errors, "parse error", "parse errors")
            ),
        );
    }
    if observations_loaded > 0 {
        return (
            CoverageStatus::Loaded,
            format!(
                "loaded {observations_loaded} {}",
                pluralize(observations_loaded, "observation", "observations")
            ),
        );
    }
    if parse_errors > 0 {
        return (
            CoverageStatus::Failed,
            format!(
                "{parse_errors} {} and no loaded observations",
                pluralize(parse_errors, "parse error", "parse errors")
            ),
        );
    }
    if artifact_count > 0 {
        return (
            CoverageStatus::ZeroRows,
            format!(
                "{artifact_count} {} produced no observations",
                pluralize(artifact_count, "artifact", "artifacts")
            ),
        );
    }
    match expected_status {
        Some(CoverageStatus::CoverageGap) => (
            CoverageStatus::CoverageGap,
            "configured as a source coverage gap".to_string(),
        ),
        Some(CoverageStatus::ManualPending) => (
            CoverageStatus::ManualPending,
            "configured as manual review pending".to_string(),
        ),
        Some(CoverageStatus::VisibleUnscored) => (
            CoverageStatus::VisibleUnscored,
            "configured as visible unscored context".to_string(),
        ),
        _ => (
            CoverageStatus::MissingExpected,
            "no artifacts or observations recorded".to_string(),
        ),
    }
}

fn pluralize(count: i64, singular: &'static str, plural: &'static str) -> &'static str {
    if count == 1 { singular } else { plural }
}

fn escape_markdown_table_cell(value: &str) -> String {
    value.replace('|', r"\|")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_status_prioritizes_loaded_partial_failed_zero_and_missing() {
        assert_eq!(
            classify_status(1, 100, 0, None),
            (
                CoverageStatus::Loaded,
                "loaded 100 observations".to_string()
            )
        );
        assert_eq!(
            classify_status(2, 100, 1, None),
            (
                CoverageStatus::Partial,
                "loaded 100 observations with 1 parse error".to_string()
            )
        );
        assert_eq!(
            classify_status(1, 0, 2, None),
            (
                CoverageStatus::Failed,
                "2 parse errors and no loaded observations".to_string()
            )
        );
        assert_eq!(
            classify_status(1, 0, 0, None),
            (
                CoverageStatus::ZeroRows,
                "1 artifact produced no observations".to_string()
            )
        );
        assert_eq!(
            classify_status(0, 0, 0, None),
            (
                CoverageStatus::MissingExpected,
                "no artifacts or observations recorded".to_string()
            )
        );
        assert_eq!(
            classify_status(0, 0, 0, Some(CoverageStatus::CoverageGap)),
            (
                CoverageStatus::CoverageGap,
                "configured as a source coverage gap".to_string()
            )
        );
    }

    #[test]
    fn build_report_summarizes_status_totals_and_markdown_rows() {
        let report = build_report(vec![
            raw_row("abs", "abs.cpi", 1, 100, 0),
            raw_row("apra", "apra.super_asset_allocation", 1, 0, 1),
            raw_row("nhsac", "nhsac.housing_accord_progress", 0, 0, 0),
        ]);

        assert_eq!(report.totals.dataflows, 3);
        assert_eq!(report.totals.loaded, 1);
        assert_eq!(report.totals.failed, 1);
        assert_eq!(report.totals.missing_expected, 1);
        assert_eq!(
            report.dataflows[1].status,
            CoverageStatus::Failed,
            "parse errors without loaded observations should be failed"
        );

        let markdown = render_markdown(&report);
        assert!(
            markdown.contains(
                "| `abs.cpi` | `abs` | `coverage_gap` | `loaded` | false | 1 | 100 | 0 |"
            )
        );
        assert!(
            markdown.contains("| `apra.super_asset_allocation` | `apra` | `coverage_gap` | `failed` | false | 1 | 0 | 1 |")
        );
        assert!(markdown.contains(
            "| `nhsac.housing_accord_progress` | `nhsac` | `coverage_gap` | `missing_expected` | false | 0 | 0 | 0 |"
        ));
    }

    #[test]
    fn launch_readiness_requires_a_recent_load() {
        let now = DateTime::parse_from_rfc3339("2026-07-10T12:00:00Z")
            .expect("fixed now")
            .with_timezone(&Utc);
        let mut fresh = raw_row("aemo", "aemo.dispatch", 1, 10, 0);
        fresh.governance_status = "active".to_string();
        fresh.launch_required = true;
        fresh.freshness_hard_seconds = Some(3_600);
        fresh.latest_load = Some("2026-07-10T11:30:00Z".to_string());
        let mut stale = fresh.clone();
        stale.dataflow_id = "aemo.generation_mix".to_string();
        stale.latest_load = Some("2026-07-10T10:00:00Z".to_string());

        let report = build_report_at(vec![fresh, stale], now);

        assert_eq!(report.totals.launch_required, 2);
        assert_eq!(report.totals.launch_ready, 1);
        assert_eq!(report.totals.launch_blockers, 1);
        assert_eq!(report.dataflows[1].status, CoverageStatus::Stale);
    }

    fn raw_row(
        source_id: &'static str,
        dataflow_id: &'static str,
        artifact_count: i64,
        observations_loaded: i64,
        parse_errors: i64,
    ) -> RawCoverageRow {
        RawCoverageRow {
            source_id: source_id.to_string(),
            dataflow_id: dataflow_id.to_string(),
            name: dataflow_id.to_string(),
            source_url: format!("https://example.test/{dataflow_id}"),
            series_count: 0,
            artifact_count,
            observations_loaded,
            parse_errors,
            latest_load: None,
            expected_status: None,
            governance_status: "coverage_gap".to_string(),
            launch_required: false,
            freshness_hard_seconds: None,
        }
    }
}
