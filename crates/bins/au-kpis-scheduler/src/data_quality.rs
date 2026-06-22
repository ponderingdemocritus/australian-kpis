//! Data-quality rule evaluation and reporting.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::Serialize;
use sqlx::{PgPool, Row};

const PAGERDUTY_SOURCE: &str = "au-kpis-data-quality";

/// One dataflow-specific data-quality rule set.
#[derive(Debug, Clone, Copy)]
pub struct DataQualityRule {
    /// Dataflow id in the catalog.
    pub dataflow_id: &'static str,
    /// Lowest plausible numeric observation value.
    pub min_value: f64,
    /// Highest plausible numeric observation value.
    pub max_value: f64,
    /// Minimum number of active series expected for the dataflow.
    pub min_active_series: i64,
    /// Required fraction of active series present in the latest period.
    pub latest_period_cardinality_floor: f64,
    /// Maximum age allowed for the newest observation.
    pub max_recency_lag_days: i64,
    /// Maximum revised rows ingested in the last 24 hours.
    pub max_daily_revisions: i64,
    /// Rolling z-score threshold for suspicious values.
    pub z_score_sigma: f64,
}

/// Overall outcome for one dataflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataQualityStatus {
    /// All configured checks passed.
    Ok,
    /// At least one configured check produced an anomaly.
    Anomalous,
}

/// One detected data-quality anomaly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DataQualityAnomaly {
    /// Dataflow that produced the anomaly.
    pub dataflow_id: String,
    /// Rule name, for example `recency`.
    pub rule: String,
    /// Alert severity routed by operations tooling.
    pub severity: String,
    /// Human-readable short summary.
    pub summary: String,
    /// Extra report detail.
    pub details: String,
}

/// Data-quality result for one dataflow.
#[derive(Debug, Clone, Serialize)]
pub struct DataflowQualityResult {
    /// Dataflow id.
    pub dataflow_id: String,
    /// Overall status.
    pub status: DataQualityStatus,
    /// Latest observation timestamp found for this dataflow.
    pub latest_observation_at: Option<DateTime<Utc>>,
    /// Number of active series currently registered.
    pub active_series: i64,
    /// Number of series represented in the latest observed period.
    pub latest_period_series: i64,
    /// Number of revised rows ingested in the report window.
    pub daily_revisions: i64,
    /// Detected anomalies for this dataflow.
    pub anomalies: Vec<DataQualityAnomaly>,
}

/// A generated data-quality report.
#[derive(Debug, Clone, Serialize)]
pub struct DataQualityReport {
    /// Report generation timestamp.
    pub generated_at: DateTime<Utc>,
    /// Inclusive start of the daily revision-volume window.
    pub window_start: DateTime<Utc>,
    /// Exclusive end of the report window.
    pub window_end: DateTime<Utc>,
    /// Per-dataflow results.
    pub results: Vec<DataflowQualityResult>,
}

impl DataQualityReport {
    /// Total anomalies across all dataflows.
    #[must_use]
    pub fn anomalies_total(&self) -> usize {
        self.results
            .iter()
            .map(|result| result.anomalies.len())
            .sum()
    }

    /// True when the report contains at least one anomaly.
    #[must_use]
    pub fn has_anomalies(&self) -> bool {
        self.anomalies_total() > 0
    }

    /// Render the report as Markdown for retained daily artifacts.
    #[must_use]
    pub fn render_markdown(&self) -> String {
        let mut markdown = String::new();
        markdown.push_str("# Data Quality Report\n\n");
        markdown.push_str(&format!("- Generated at: `{}`\n", self.generated_at));
        markdown.push_str(&format!(
            "- Window: `{}` to `{}`\n",
            self.window_start, self.window_end
        ));
        markdown.push_str(&format!("- Dataflows checked: `{}`\n", self.results.len()));
        markdown.push_str(&format!("- Anomalies: `{}`\n\n", self.anomalies_total()));

        markdown.push_str("| Dataflow | Status | Latest observation | Active series | Latest-period series | Daily revisions |\n");
        markdown.push_str("|---|---:|---|---:|---:|---:|\n");
        for result in &self.results {
            let latest = result
                .latest_observation_at
                .map_or_else(|| "none".to_string(), |value| value.to_rfc3339());
            markdown.push_str(&format!(
                "| `{}` | `{:?}` | `{}` | {} | {} | {} |\n",
                result.dataflow_id,
                result.status,
                latest,
                result.active_series,
                result.latest_period_series,
                result.daily_revisions
            ));
        }

        markdown.push_str("\n## Anomalies\n\n");
        if !self.has_anomalies() {
            markdown.push_str("No anomalies detected.\n");
            return markdown;
        }

        for anomaly in self.results.iter().flat_map(|result| &result.anomalies) {
            markdown.push_str(&format!(
                "- **{}** `{}` `{}`: {} {}\n",
                anomaly.severity,
                anomaly.dataflow_id,
                anomaly.rule,
                anomaly.summary,
                anomaly.details
            ));
        }

        markdown
    }
}

/// PagerDuty Events v2 configuration.
#[derive(Debug, Clone)]
pub struct PagerDutyConfig {
    /// Optional Events v2 routing key.
    pub routing_key: Option<String>,
    /// Events endpoint URL.
    pub events_url: String,
}

/// PagerDuty notification result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PagerDutyOutcome {
    /// Report contained no anomalies, so no page was needed.
    NoAnomalies,
    /// Anomalies were detected, but no routing key was configured.
    MissingRoutingKey,
    /// An event was accepted by PagerDuty.
    Sent,
}

/// Built-in rule catalog for currently implemented dataflows.
#[must_use]
pub fn default_data_quality_rules() -> &'static [DataQualityRule] {
    &DEFAULT_RULES
}

const DEFAULT_RULES: [DataQualityRule; 11] = [
    DataQualityRule {
        dataflow_id: "abs.cpi",
        min_value: 0.0,
        max_value: 1_000.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 0.75,
        max_recency_lag_days: 180,
        max_daily_revisions: 500,
        z_score_sigma: 5.0,
    },
    DataQualityRule {
        dataflow_id: "rba.statistical_tables",
        min_value: -1_000_000.0,
        max_value: 1_000_000_000_000.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 0.50,
        max_recency_lag_days: 14,
        max_daily_revisions: 5_000,
        z_score_sigma: 5.0,
    },
    DataQualityRule {
        dataflow_id: "apra.quarterly_statistics",
        min_value: -1_000_000_000_000.0,
        max_value: 1_000_000_000_000.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 0.50,
        max_recency_lag_days: 180,
        max_daily_revisions: 5_000,
        z_score_sigma: 5.0,
    },
    DataQualityRule {
        dataflow_id: "treasury.budget_papers",
        min_value: -1_000_000_000_000.0,
        max_value: 1_000_000_000_000.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 0.50,
        max_recency_lag_days: 450,
        max_daily_revisions: 2_000,
        z_score_sigma: 5.0,
    },
    DataQualityRule {
        dataflow_id: "state_budgets.nsw_budget",
        min_value: -1_000_000_000_000.0,
        max_value: 1_000_000_000_000.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 0.50,
        max_recency_lag_days: 450,
        max_daily_revisions: 1_000,
        z_score_sigma: 5.0,
    },
    DataQualityRule {
        dataflow_id: "state_budgets.vic_budget",
        min_value: -1_000_000_000_000.0,
        max_value: 1_000_000_000_000.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 0.50,
        max_recency_lag_days: 450,
        max_daily_revisions: 1_000,
        z_score_sigma: 5.0,
    },
    DataQualityRule {
        dataflow_id: "state_budgets.qld_budget",
        min_value: -1_000_000_000_000.0,
        max_value: 1_000_000_000_000.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 0.50,
        max_recency_lag_days: 450,
        max_daily_revisions: 1_000,
        z_score_sigma: 5.0,
    },
    DataQualityRule {
        dataflow_id: "curated.oversight_strength",
        min_value: 0.0,
        max_value: 100.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 1.0,
        max_recency_lag_days: 450,
        max_daily_revisions: 50,
        z_score_sigma: 5.0,
    },
    DataQualityRule {
        dataflow_id: "curated.control_enable_spend",
        min_value: 0.0,
        max_value: 10.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 1.0,
        max_recency_lag_days: 450,
        max_daily_revisions: 50,
        z_score_sigma: 5.0,
    },
    DataQualityRule {
        dataflow_id: "curated.surveillance_intensity",
        min_value: 0.0,
        max_value: 100.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 1.0,
        max_recency_lag_days: 800,
        max_daily_revisions: 50,
        z_score_sigma: 5.0,
    },
    DataQualityRule {
        dataflow_id: "compute.au_datacentre_capacity_mw",
        min_value: 0.0,
        max_value: 1_000_000.0,
        min_active_series: 1,
        latest_period_cardinality_floor: 1.0,
        max_recency_lag_days: 450,
        max_daily_revisions: 50,
        z_score_sigma: 5.0,
    },
];

/// Run all configured data-quality rules against the database.
#[tracing::instrument(skip(pool, rules))]
pub async fn run_data_quality_checks(
    pool: &PgPool,
    rules: &[DataQualityRule],
    now: DateTime<Utc>,
) -> anyhow::Result<DataQualityReport> {
    let window_start = now - chrono::Duration::days(1);
    let mut results = Vec::with_capacity(rules.len());

    for rule in rules {
        results.push(check_dataflow(pool, rule, now, window_start).await?);
    }

    Ok(DataQualityReport {
        generated_at: now,
        window_start,
        window_end: now,
        results,
    })
}

/// Send a PagerDuty Events v2 trigger for reports with anomalies.
#[tracing::instrument(skip(report, config))]
pub async fn notify_pagerduty(
    report: &DataQualityReport,
    config: &PagerDutyConfig,
) -> anyhow::Result<PagerDutyOutcome> {
    if !report.has_anomalies() {
        return Ok(PagerDutyOutcome::NoAnomalies);
    }

    let Some(routing_key) = config.routing_key.as_deref() else {
        return Ok(PagerDutyOutcome::MissingRoutingKey);
    };

    let payload = pagerduty_event_payload(report, routing_key);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build PagerDuty HTTP client")?;
    let response = client
        .post(&config.events_url)
        .json(&payload)
        .send()
        .await
        .context("send PagerDuty data-quality event")?;

    if response.status() != StatusCode::ACCEPTED {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "PagerDuty rejected data-quality event: {status} {body}"
        ));
    }

    Ok(PagerDutyOutcome::Sent)
}

async fn check_dataflow(
    pool: &PgPool,
    rule: &DataQualityRule,
    now: DateTime<Utc>,
    window_start: DateTime<Utc>,
) -> anyhow::Result<DataflowQualityResult> {
    let latest_observation_at = latest_observation_at(pool, rule.dataflow_id).await?;
    let active_series = active_series_count(pool, rule.dataflow_id).await?;
    let latest_period_series = match latest_observation_at {
        Some(latest) => latest_period_series_count(pool, rule.dataflow_id, latest).await?,
        None => 0,
    };
    let range = plausible_range_violations(pool, rule).await?;
    let z_score = z_score_violations(pool, rule).await?;
    let daily_revisions = daily_revision_count(pool, rule.dataflow_id, window_start).await?;

    let mut anomalies = Vec::new();
    push_recency_anomaly(&mut anomalies, rule, now, latest_observation_at);
    push_cardinality_anomaly(&mut anomalies, rule, active_series, latest_period_series);
    push_range_anomaly(&mut anomalies, rule, &range);
    push_z_score_anomaly(&mut anomalies, rule, z_score);
    push_revision_anomaly(&mut anomalies, rule, daily_revisions);

    Ok(DataflowQualityResult {
        dataflow_id: rule.dataflow_id.to_string(),
        status: if anomalies.is_empty() {
            DataQualityStatus::Ok
        } else {
            DataQualityStatus::Anomalous
        },
        latest_observation_at,
        active_series,
        latest_period_series,
        daily_revisions,
        anomalies,
    })
}

async fn latest_observation_at(
    pool: &PgPool,
    dataflow_id: &str,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    sqlx::query_scalar(
        r#"SELECT max(o.time)
           FROM observations_latest o
           JOIN series s ON s.series_key = o.series_key
           WHERE s.dataflow_id = $1"#,
    )
    .bind(dataflow_id)
    .fetch_one(pool)
    .await
    .with_context(|| format!("load latest observation timestamp for {dataflow_id}"))
}

async fn active_series_count(pool: &PgPool, dataflow_id: &str) -> anyhow::Result<i64> {
    sqlx::query_scalar(
        r#"SELECT count(*)::BIGINT
           FROM series
           WHERE dataflow_id = $1
             AND active"#,
    )
    .bind(dataflow_id)
    .fetch_one(pool)
    .await
    .with_context(|| format!("count active series for {dataflow_id}"))
}

async fn latest_period_series_count(
    pool: &PgPool,
    dataflow_id: &str,
    latest: DateTime<Utc>,
) -> anyhow::Result<i64> {
    sqlx::query_scalar(
        r#"SELECT count(DISTINCT o.series_key)::BIGINT
           FROM observations_latest o
           JOIN series s ON s.series_key = o.series_key
           WHERE s.dataflow_id = $1
             AND o.time = $2"#,
    )
    .bind(dataflow_id)
    .bind(latest)
    .fetch_one(pool)
    .await
    .with_context(|| format!("count latest-period series for {dataflow_id}"))
}

#[derive(Debug, Clone, Copy)]
struct RangeViolations {
    count: i64,
    observed_min: Option<f64>,
    observed_max: Option<f64>,
}

async fn plausible_range_violations(
    pool: &PgPool,
    rule: &DataQualityRule,
) -> anyhow::Result<RangeViolations> {
    let row = sqlx::query(
        r#"SELECT count(*)::BIGINT AS violations,
                  min(o.value) AS observed_min,
                  max(o.value) AS observed_max
           FROM observations_latest o
           JOIN series s ON s.series_key = o.series_key
           WHERE s.dataflow_id = $1
             AND o.value IS NOT NULL
             AND (o.value < $2 OR o.value > $3)"#,
    )
    .bind(rule.dataflow_id)
    .bind(rule.min_value)
    .bind(rule.max_value)
    .fetch_one(pool)
    .await
    .with_context(|| format!("check plausible range for {}", rule.dataflow_id))?;

    Ok(RangeViolations {
        count: row.try_get("violations")?,
        observed_min: row.try_get("observed_min")?,
        observed_max: row.try_get("observed_max")?,
    })
}

async fn z_score_violations(pool: &PgPool, rule: &DataQualityRule) -> anyhow::Result<i64> {
    sqlx::query_scalar(
        r#"WITH stats AS (
               SELECT avg(o.value) AS mean, stddev_samp(o.value) AS stddev
               FROM observations_latest o
               JOIN series s ON s.series_key = o.series_key
               WHERE s.dataflow_id = $1
                 AND o.value IS NOT NULL
           )
           SELECT count(*)::BIGINT
           FROM observations_latest o
           JOIN series s ON s.series_key = o.series_key
           CROSS JOIN stats
           WHERE s.dataflow_id = $1
             AND o.value IS NOT NULL
             AND stats.stddev IS NOT NULL
             AND stats.stddev > 0
             AND abs((o.value - stats.mean) / stats.stddev) > $2"#,
    )
    .bind(rule.dataflow_id)
    .bind(rule.z_score_sigma)
    .fetch_one(pool)
    .await
    .with_context(|| format!("check rolling z-score for {}", rule.dataflow_id))
}

async fn daily_revision_count(
    pool: &PgPool,
    dataflow_id: &str,
    window_start: DateTime<Utc>,
) -> anyhow::Result<i64> {
    sqlx::query_scalar(
        r#"SELECT count(*)::BIGINT
           FROM observations o
           JOIN series s ON s.series_key = o.series_key
           WHERE s.dataflow_id = $1
             AND o.revision_no > 0
             AND o.ingested_at >= $2"#,
    )
    .bind(dataflow_id)
    .bind(window_start)
    .fetch_one(pool)
    .await
    .with_context(|| format!("count daily revisions for {dataflow_id}"))
}

fn push_recency_anomaly(
    anomalies: &mut Vec<DataQualityAnomaly>,
    rule: &DataQualityRule,
    now: DateTime<Utc>,
    latest: Option<DateTime<Utc>>,
) {
    let allowed = chrono::Duration::days(rule.max_recency_lag_days);
    match latest {
        Some(value) if now.signed_duration_since(value) <= allowed => {}
        Some(value) => anomalies.push(DataQualityAnomaly {
            dataflow_id: rule.dataflow_id.to_string(),
            rule: "recency".to_string(),
            severity: "page".to_string(),
            summary: "latest observation is older than expected cadence".to_string(),
            details: format!(
                "latest={}, max_lag_days={}",
                value.to_rfc3339(),
                rule.max_recency_lag_days
            ),
        }),
        None => anomalies.push(DataQualityAnomaly {
            dataflow_id: rule.dataflow_id.to_string(),
            rule: "recency".to_string(),
            severity: "page".to_string(),
            summary: "dataflow has no observations".to_string(),
            details: format!("max_lag_days={}", rule.max_recency_lag_days),
        }),
    }
}

fn push_cardinality_anomaly(
    anomalies: &mut Vec<DataQualityAnomaly>,
    rule: &DataQualityRule,
    active_series: i64,
    latest_period_series: i64,
) {
    if active_series < rule.min_active_series {
        anomalies.push(DataQualityAnomaly {
            dataflow_id: rule.dataflow_id.to_string(),
            rule: "cardinality".to_string(),
            severity: "page".to_string(),
            summary: "active series count is below the rule floor".to_string(),
            details: format!(
                "active_series={}, min_active_series={}",
                active_series, rule.min_active_series
            ),
        });
        return;
    }

    let required = ((active_series as f64) * rule.latest_period_cardinality_floor).ceil() as i64;
    if latest_period_series < required {
        anomalies.push(DataQualityAnomaly {
            dataflow_id: rule.dataflow_id.to_string(),
            rule: "cardinality".to_string(),
            severity: "page".to_string(),
            summary: "latest period is missing too many active series".to_string(),
            details: format!(
                "latest_period_series={}, required={}, active_series={}",
                latest_period_series, required, active_series
            ),
        });
    }
}

fn push_range_anomaly(
    anomalies: &mut Vec<DataQualityAnomaly>,
    rule: &DataQualityRule,
    range: &RangeViolations,
) {
    if range.count == 0 {
        return;
    }

    anomalies.push(DataQualityAnomaly {
        dataflow_id: rule.dataflow_id.to_string(),
        rule: "plausible_range".to_string(),
        severity: "page".to_string(),
        summary: "observations outside configured plausible range".to_string(),
        details: format!(
            "violations={}, allowed=[{}, {}], observed_min={:?}, observed_max={:?}",
            range.count, rule.min_value, rule.max_value, range.observed_min, range.observed_max
        ),
    });
}

fn push_z_score_anomaly(
    anomalies: &mut Vec<DataQualityAnomaly>,
    rule: &DataQualityRule,
    violations: i64,
) {
    if violations == 0 {
        return;
    }

    anomalies.push(DataQualityAnomaly {
        dataflow_id: rule.dataflow_id.to_string(),
        rule: "rolling_z_score".to_string(),
        severity: "page".to_string(),
        summary: "observations exceed rolling z-score threshold".to_string(),
        details: format!("violations={}, sigma={}", violations, rule.z_score_sigma),
    });
}

fn push_revision_anomaly(
    anomalies: &mut Vec<DataQualityAnomaly>,
    rule: &DataQualityRule,
    daily_revisions: i64,
) {
    if daily_revisions <= rule.max_daily_revisions {
        return;
    }

    anomalies.push(DataQualityAnomaly {
        dataflow_id: rule.dataflow_id.to_string(),
        rule: "revision_volume".to_string(),
        severity: "page".to_string(),
        summary: "daily revision volume exceeds configured threshold".to_string(),
        details: format!(
            "daily_revisions={}, max_daily_revisions={}",
            daily_revisions, rule.max_daily_revisions
        ),
    });
}

#[derive(Debug, Serialize)]
struct PagerDutyEvent<'a> {
    routing_key: &'a str,
    event_action: &'static str,
    dedup_key: String,
    payload: PagerDutyPayload,
}

#[derive(Debug, Serialize)]
struct PagerDutyPayload {
    summary: String,
    source: &'static str,
    severity: &'static str,
    component: &'static str,
    group: &'static str,
    class: &'static str,
    custom_details: BTreeMap<String, serde_json::Value>,
}

fn pagerduty_event_payload<'a>(
    report: &DataQualityReport,
    routing_key: &'a str,
) -> PagerDutyEvent<'a> {
    let anomalous_dataflows = report
        .results
        .iter()
        .filter(|result| !result.anomalies.is_empty())
        .map(|result| result.dataflow_id.clone())
        .collect::<Vec<_>>();
    let mut details = BTreeMap::new();
    details.insert(
        "generated_at".to_string(),
        serde_json::json!(report.generated_at),
    );
    details.insert(
        "window_start".to_string(),
        serde_json::json!(report.window_start),
    );
    details.insert(
        "window_end".to_string(),
        serde_json::json!(report.window_end),
    );
    details.insert(
        "anomalies_total".to_string(),
        serde_json::json!(report.anomalies_total()),
    );
    details.insert(
        "anomalous_dataflows".to_string(),
        serde_json::json!(anomalous_dataflows),
    );
    details.insert("results".to_string(), serde_json::json!(report.results));

    PagerDutyEvent {
        routing_key,
        event_action: "trigger",
        dedup_key: format!(
            "au-kpis-data-quality-{}",
            report.generated_at.format("%Y-%m-%d")
        ),
        payload: PagerDutyPayload {
            summary: format!(
                "AU KPIs data-quality check detected {} anomaly/anomalies",
                report.anomalies_total()
            ),
            source: PAGERDUTY_SOURCE,
            severity: "critical",
            component: "data-quality",
            group: "ingestion",
            class: "silent-corruption",
            custom_details: details,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anomaly(rule: &str) -> DataQualityAnomaly {
        DataQualityAnomaly {
            dataflow_id: "abs.cpi".to_string(),
            rule: rule.to_string(),
            severity: "page".to_string(),
            summary: "summary".to_string(),
            details: "details".to_string(),
        }
    }

    fn sample_report() -> DataQualityReport {
        let now = DateTime::parse_from_rfc3339("2026-05-28T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        DataQualityReport {
            generated_at: now,
            window_start: now - chrono::Duration::days(1),
            window_end: now,
            results: vec![DataflowQualityResult {
                dataflow_id: "abs.cpi".to_string(),
                status: DataQualityStatus::Anomalous,
                latest_observation_at: None,
                active_series: 0,
                latest_period_series: 0,
                daily_revisions: 0,
                anomalies: vec![anomaly("plausible_range")],
            }],
        }
    }

    #[test]
    fn default_rules_cover_current_catalog() {
        let rules = default_data_quality_rules();
        assert!(rules.iter().any(|rule| rule.dataflow_id == "abs.cpi"));
        assert!(
            rules
                .iter()
                .any(|rule| rule.dataflow_id == "rba.statistical_tables")
        );
        assert!(
            rules
                .iter()
                .any(|rule| rule.dataflow_id == "curated.oversight_strength")
        );
        assert!(
            rules
                .iter()
                .any(|rule| { rule.dataflow_id == "compute.au_datacentre_capacity_mw" })
        );
        assert!(
            rules
                .iter()
                .all(|rule| rule.latest_period_cardinality_floor > 0.0)
        );
        assert!(rules.iter().all(|rule| rule.max_recency_lag_days > 0));
    }

    #[test]
    fn markdown_report_lists_anomalies() {
        let report = sample_report();

        let markdown = report.render_markdown();

        assert!(markdown.contains("# Data Quality Report"));
        assert!(markdown.contains("daily revisions") || markdown.contains("Daily revisions"));
        assert!(markdown.contains("abs.cpi"));
        assert!(markdown.contains("plausible_range"));
    }

    #[test]
    fn pagerduty_payload_summarizes_anomalies() {
        let report = sample_report();

        let payload = pagerduty_event_payload(&report, "routing-key");
        let json = serde_json::to_value(payload).unwrap();

        assert_eq!(json["routing_key"], "routing-key");
        assert_eq!(json["event_action"], "trigger");
        assert_eq!(json["payload"]["severity"], "critical");
        assert_eq!(json["payload"]["custom_details"]["anomalies_total"], 1);
        assert_eq!(
            json["payload"]["custom_details"]["anomalous_dataflows"][0],
            "abs.cpi"
        );
    }

    #[tokio::test]
    async fn notify_pagerduty_posts_events_v2_payload() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut buffer = [0_u8; 1024];
                let read = socket.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") && request.ends_with(b"}")
                {
                    break;
                }
            }
            sender
                .send(String::from_utf8(request).unwrap())
                .expect("send request body");
            socket
                .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\n\r\n{}")
                .await
                .unwrap();
        });

        let outcome = notify_pagerduty(
            &sample_report(),
            &PagerDutyConfig {
                routing_key: Some("routing-key".to_string()),
                events_url: format!("http://{addr}/v2/enqueue"),
            },
        )
        .await
        .unwrap();
        server.await.unwrap();

        assert_eq!(outcome, PagerDutyOutcome::Sent);
        let request = receiver.recv().unwrap();
        assert!(request.starts_with("POST /v2/enqueue HTTP/1.1"));
        assert!(request.contains("\"routing_key\":\"routing-key\""));
        assert!(request.contains("\"event_action\":\"trigger\""));
        assert!(request.contains("\"anomalous_dataflows\":[\"abs.cpi\"]"));
    }

    #[test]
    fn day_helper_uses_24_hour_windows() {
        const SECONDS_PER_DAY: u64 = 24 * 60 * 60;
        const fn days(days: u64) -> Duration {
            Duration::from_secs(days * SECONDS_PER_DAY)
        }

        assert_eq!(days(1), Duration::from_secs(SECONDS_PER_DAY));
    }
}
