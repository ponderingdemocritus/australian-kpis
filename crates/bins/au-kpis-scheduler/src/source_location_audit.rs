//! Source-location audit rule evaluation and reporting.

use std::{collections::BTreeMap, time::Duration};

use anyhow::Context;
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::Value;

const USER_AGENT: &str = concat!("au-kpis-source-location-audit/", env!("CARGO_PKG_VERSION"));
const HTTP_TIMEOUT: Duration = Duration::from_secs(20);

/// One source-specific location audit rule.
#[derive(Debug, Clone)]
pub struct SourceLocationRule {
    /// Source id in the source catalog.
    pub source_id: &'static str,
    /// Dataflow id protected by this rule.
    pub dataflow_id: &'static str,
    /// Currently configured or canonical source URL to check.
    pub current_url: &'static str,
    /// Source-specific semantic check.
    pub check: SourceLocationCheck,
}

impl SourceLocationRule {
    /// Build a source-location audit rule.
    #[must_use]
    pub const fn new(
        source_id: &'static str,
        dataflow_id: &'static str,
        current_url: &'static str,
        check: SourceLocationCheck,
    ) -> Self {
        Self {
            source_id,
            dataflow_id,
            current_url,
            check,
        }
    }
}

/// Source-specific location and freshness semantics.
#[derive(Debug, Clone)]
pub enum SourceLocationCheck {
    /// URL should be reachable with a successful HTTP status.
    Reachable {
        /// Human recommendation when the URL cannot be reached.
        recommendation: &'static str,
    },
    /// Page body should contain at least one expected semantic hint.
    ContainsAny {
        /// Text fragments accepted as evidence of the current page.
        needles: &'static [&'static str],
        /// Human recommendation when no hint is present.
        recommendation: &'static str,
    },
    /// Current URL is known to have a newer canonical replacement.
    CanonicalUrl {
        /// Expected canonical or effective URL.
        expected_url: &'static str,
        /// Human recommendation for the tracked issue.
        recommendation: &'static str,
    },
    /// Budget index must expose the latest expected budget year.
    BudgetYear {
        /// Budget year currently configured in the adapter/catalog.
        configured_year: &'static str,
        /// Latest budget year expected on the official index.
        latest_year: &'static str,
        /// Human recommendation for the tracked issue.
        recommendation: &'static str,
    },
    /// Directory listing must include current report filename patterns.
    DirectoryListing {
        /// Required body fragments that identify the current reports.
        required_patterns: &'static [&'static str],
        /// Human recommendation when patterns disappear.
        recommendation: &'static str,
    },
    /// Licensed feed dataflow where the public product page is the auditable URL.
    LicensedProduct {
        /// Human recommendation when the product page is unreachable.
        recommendation: &'static str,
    },
    /// World Bank B-READY API semantics for Australia availability.
    WorldBankBreadyApi {
        /// Human recommendation when values are unresolved.
        recommendation: &'static str,
    },
    /// Manual placeholder that must remain visible until replaced.
    ManualPlaceholder {
        /// Why this source cannot pass automatically.
        reason: &'static str,
        /// Human recommendation for the tracked issue.
        recommendation: &'static str,
    },
}

/// HTTP snapshot used by deterministic evaluator tests and the live runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceUrlSnapshot {
    /// Requested URL.
    pub url: String,
    /// Final URL after redirects.
    pub effective_url: String,
    /// HTTP status code, or `0` when the request could not be made.
    pub status: u16,
    /// Response body as text.
    pub body: String,
}

impl SourceUrlSnapshot {
    /// Build a source URL snapshot.
    #[must_use]
    pub fn new(url: &str, effective_url: &str, status: u16, body: &str) -> Self {
        Self {
            url: url.to_string(),
            effective_url: effective_url.to_string(),
            status,
            body: body.to_string(),
        }
    }
}

/// Overall source-location audit status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAuditStatus {
    /// All configured rules passed.
    Ok,
    /// At least one configured source appears to have moved or become stale.
    Drift,
    /// At least one configured source needs human review but is not proven stale.
    ManualReview,
    /// At least one configured rule failed due to tooling or HTTP errors.
    Error,
}

/// Finding severity for GitHub issue automation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAuditSeverity {
    /// Informational finding.
    Info,
    /// Source drift should be reviewed.
    Warning,
    /// Human review is needed before scoring or source replacement.
    ManualReview,
    /// Tooling or source access failed.
    Error,
}

/// One detected source-location issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceAuditFinding {
    /// Source id in the source catalog.
    pub source_id: String,
    /// Dataflow id protected by the finding.
    pub dataflow_id: String,
    /// Finding severity.
    pub severity: SourceAuditSeverity,
    /// Currently configured URL.
    pub current_url: String,
    /// Latest, canonical, or effective URL when known.
    pub latest_url: Option<String>,
    /// Human-readable evidence collected by the rule.
    pub evidence: String,
    /// Recommended operator action.
    pub recommendation: String,
}

/// Per-rule audit result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceAuditResult {
    /// Source id in the source catalog.
    pub source_id: String,
    /// Dataflow id protected by the rule.
    pub dataflow_id: String,
    /// Rule status.
    pub status: SourceAuditStatus,
    /// Current URL checked by the rule.
    pub current_url: String,
    /// Final URL observed after redirects.
    pub effective_url: Option<String>,
    /// HTTP status code, when a request was made.
    pub http_status: Option<u16>,
    /// Short rule evidence.
    pub evidence: String,
}

/// Generated source-location audit report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceLocationAuditReport {
    /// Report generation timestamp.
    pub generated_at: DateTime<Utc>,
    /// Overall report status.
    pub status: SourceAuditStatus,
    /// Number of rules checked.
    pub checked_total: usize,
    /// Number of findings in the report.
    pub findings_total: usize,
    /// Per-rule results.
    pub results: Vec<SourceAuditResult>,
    /// Findings requiring operational action.
    pub findings: Vec<SourceAuditFinding>,
}

impl SourceLocationAuditReport {
    /// True when the report contains operational findings.
    #[must_use]
    pub fn has_findings(&self) -> bool {
        self.findings_total > 0
    }

    /// Render the report as Markdown for retained workflow artifacts.
    #[must_use]
    pub fn render_markdown(&self) -> String {
        let mut markdown = String::new();
        markdown.push_str("# Source Location Audit Report\n\n");
        markdown.push_str(&format!("- Generated at: `{}`\n", self.generated_at));
        markdown.push_str(&format!("- Status: `{}`\n", self.status.as_str()));
        markdown.push_str(&format!("- Rules checked: `{}`\n", self.checked_total));
        markdown.push_str(&format!("- Findings: `{}`\n\n", self.findings_total));

        markdown.push_str("| Source | Dataflow | Status | HTTP | Effective URL |\n");
        markdown.push_str("|---|---|---:|---:|---|\n");
        for result in &self.results {
            let status = result
                .http_status
                .map_or_else(|| "n/a".to_string(), |value| value.to_string());
            let effective_url = result.effective_url.as_deref().unwrap_or("n/a");
            markdown.push_str(&format!(
                "| `{}` | `{}` | `{}` | `{}` | {} |\n",
                result.source_id,
                result.dataflow_id,
                result.status.as_str(),
                status,
                markdown_escape(effective_url)
            ));
        }

        markdown.push_str("\n## Findings\n\n");
        if !self.has_findings() {
            markdown.push_str("No source-location findings detected.\n");
            return markdown;
        }

        markdown.push_str("| Severity | Source | Dataflow | Current URL | Latest/effective URL | Evidence | Recommendation |\n");
        markdown.push_str("|---:|---|---|---|---|---|---|\n");
        for finding in &self.findings {
            markdown.push_str(&format!(
                "| `{}` | `{}` | `{}` | {} | {} | {} | {} |\n",
                finding.severity.as_str(),
                finding.source_id,
                finding.dataflow_id,
                markdown_escape(&finding.current_url),
                finding
                    .latest_url
                    .as_deref()
                    .map(markdown_escape)
                    .unwrap_or_else(|| "n/a".to_string()),
                markdown_escape(&finding.evidence),
                markdown_escape(&finding.recommendation)
            ));
        }

        markdown
    }
}

impl SourceAuditStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Drift => "drift",
            Self::ManualReview => "manual_review",
            Self::Error => "error",
        }
    }
}

impl SourceAuditSeverity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::ManualReview => "manual_review",
            Self::Error => "error",
        }
    }
}

/// Built-in rule catalog for implemented and APS-scoped source dataflows.
#[must_use]
pub fn default_source_location_rules() -> &'static [SourceLocationRule] {
    &DEFAULT_RULES
}

/// Run the source-location audit against live external URLs.
#[tracing::instrument(skip(rules))]
pub async fn run_source_location_audit(
    rules: &[SourceLocationRule],
    now: DateTime<Utc>,
) -> anyhow::Result<SourceLocationAuditReport> {
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .user_agent(USER_AGENT)
        .build()
        .context("build source-location audit HTTP client")?;
    let mut snapshots = Vec::with_capacity(rules.len());
    for rule in rules {
        snapshots.push(fetch_snapshot(&client, rule.current_url).await?);
    }
    Ok(evaluate_source_location_snapshots(rules, &snapshots, now))
}

/// Evaluate source-location rules from pre-fetched snapshots.
#[must_use]
pub fn evaluate_source_location_snapshots(
    rules: &[SourceLocationRule],
    snapshots: &[SourceUrlSnapshot],
    generated_at: DateTime<Utc>,
) -> SourceLocationAuditReport {
    let snapshots_by_url = snapshots
        .iter()
        .map(|snapshot| (snapshot.url.as_str(), snapshot))
        .collect::<BTreeMap<_, _>>();
    let mut results = Vec::with_capacity(rules.len());
    let mut findings = Vec::new();

    for rule in rules {
        let snapshot = snapshots_by_url.get(rule.current_url).copied();
        let evaluation = evaluate_rule(rule, snapshot);
        if let Some(finding) = evaluation.finding {
            findings.push(finding);
        }
        results.push(evaluation.result);
    }

    let status = aggregate_status(&results);
    let findings_total = findings.len();
    SourceLocationAuditReport {
        generated_at,
        status,
        checked_total: rules.len(),
        findings_total,
        results,
        findings,
    }
}

#[derive(Debug)]
struct RuleEvaluation {
    result: SourceAuditResult,
    finding: Option<SourceAuditFinding>,
}

async fn fetch_snapshot(client: &reqwest::Client, url: &str) -> anyhow::Result<SourceUrlSnapshot> {
    reqwest::Url::parse(url)
        .with_context(|| format!("source-location rule URL is invalid: {url}"))?;
    let response = match client.get(url).send().await {
        Ok(response) => response,
        Err(err) => {
            return Ok(SourceUrlSnapshot {
                url: url.to_string(),
                effective_url: url.to_string(),
                status: 0,
                body: format!("request error: {err}"),
            });
        }
    };
    let status = response.status().as_u16();
    let effective_url = response.url().to_string();
    let body = response
        .text()
        .await
        .unwrap_or_else(|err| format!("response body read error: {err}"));
    Ok(SourceUrlSnapshot {
        url: url.to_string(),
        effective_url,
        status,
        body,
    })
}

fn evaluate_rule(
    rule: &SourceLocationRule,
    snapshot: Option<&SourceUrlSnapshot>,
) -> RuleEvaluation {
    if let SourceLocationCheck::ManualPlaceholder {
        reason,
        recommendation,
    } = &rule.check
    {
        return finding_evaluation(
            rule,
            snapshot,
            SourceAuditStatus::ManualReview,
            SourceAuditSeverity::ManualReview,
            None,
            (*reason).to_string(),
            (*recommendation).to_string(),
        );
    }

    let Some(snapshot) = snapshot else {
        return finding_evaluation(
            rule,
            None,
            SourceAuditStatus::Error,
            SourceAuditSeverity::Error,
            None,
            "No HTTP snapshot was available for this rule.".to_string(),
            "Retry the source-location audit and inspect scheduler logs.".to_string(),
        );
    };

    match &rule.check {
        SourceLocationCheck::CanonicalUrl {
            expected_url,
            recommendation,
        } => evaluate_canonical_url(rule, snapshot, expected_url, recommendation),
        SourceLocationCheck::Reachable { recommendation }
        | SourceLocationCheck::LicensedProduct { recommendation } => {
            evaluate_reachable(rule, snapshot, recommendation)
        }
        SourceLocationCheck::ContainsAny {
            needles,
            recommendation,
        } => evaluate_contains_any(rule, snapshot, needles, recommendation),
        SourceLocationCheck::BudgetYear {
            configured_year,
            latest_year,
            recommendation,
        } => evaluate_budget_year(rule, snapshot, configured_year, latest_year, recommendation),
        SourceLocationCheck::DirectoryListing {
            required_patterns,
            recommendation,
        } => evaluate_directory_listing(rule, snapshot, required_patterns, recommendation),
        SourceLocationCheck::WorldBankBreadyApi { recommendation } => {
            evaluate_world_bank_bready(rule, snapshot, recommendation)
        }
        SourceLocationCheck::ManualPlaceholder { .. } => unreachable!("handled before snapshot"),
    }
}

fn evaluate_canonical_url(
    rule: &SourceLocationRule,
    snapshot: &SourceUrlSnapshot,
    expected_url: &str,
    recommendation: &str,
) -> RuleEvaluation {
    if snapshot.effective_url == expected_url && is_success(snapshot.status) {
        return ok_evaluation(
            rule,
            snapshot,
            format!("Canonical URL resolved to {expected_url}."),
        );
    }

    finding_evaluation(
        rule,
        Some(snapshot),
        SourceAuditStatus::Drift,
        SourceAuditSeverity::Warning,
        Some(expected_url.to_string()),
        format!(
            "Configured URL returned HTTP {} with effective URL `{}`; expected `{}`.",
            snapshot.status, snapshot.effective_url, expected_url
        ),
        recommendation.to_string(),
    )
}

fn evaluate_reachable(
    rule: &SourceLocationRule,
    snapshot: &SourceUrlSnapshot,
    recommendation: &str,
) -> RuleEvaluation {
    if is_success(snapshot.status) {
        return ok_evaluation(
            rule,
            snapshot,
            format!("URL reachable with HTTP {}.", snapshot.status),
        );
    }

    if is_soft_access_status(snapshot.status) {
        return finding_evaluation(
            rule,
            Some(snapshot),
            SourceAuditStatus::ManualReview,
            SourceAuditSeverity::ManualReview,
            Some(snapshot.effective_url.clone()),
            format!(
                "URL returned HTTP {}; access may be bot-filtered or temporarily unavailable.",
                snapshot.status
            ),
            recommendation.to_string(),
        );
    }

    finding_evaluation(
        rule,
        Some(snapshot),
        SourceAuditStatus::Drift,
        SourceAuditSeverity::Warning,
        Some(snapshot.effective_url.clone()),
        format!("URL returned HTTP {}.", snapshot.status),
        recommendation.to_string(),
    )
}

fn evaluate_contains_any(
    rule: &SourceLocationRule,
    snapshot: &SourceUrlSnapshot,
    needles: &[&str],
    recommendation: &str,
) -> RuleEvaluation {
    if !is_success(snapshot.status) {
        return evaluate_reachable(rule, snapshot, recommendation);
    }
    if let Some(needle) = needles
        .iter()
        .find(|needle| snapshot.body.contains(**needle))
    {
        return ok_evaluation(
            rule,
            snapshot,
            format!("Observed expected source hint `{needle}`."),
        );
    }

    finding_evaluation(
        rule,
        Some(snapshot),
        SourceAuditStatus::ManualReview,
        SourceAuditSeverity::ManualReview,
        Some(snapshot.effective_url.clone()),
        "Page was reachable but expected source hints were absent.".to_string(),
        recommendation.to_string(),
    )
}

fn evaluate_budget_year(
    rule: &SourceLocationRule,
    snapshot: &SourceUrlSnapshot,
    configured_year: &str,
    latest_year: &str,
    recommendation: &str,
) -> RuleEvaluation {
    if !is_success(snapshot.status) {
        return evaluate_reachable(rule, snapshot, recommendation);
    }
    if let Some(newer_year) = discover_newer_budget_year(&snapshot.body, configured_year) {
        let latest_url = discover_latest_year_url(snapshot, &newer_year);
        return finding_evaluation(
            rule,
            Some(snapshot),
            SourceAuditStatus::Drift,
            SourceAuditSeverity::Warning,
            latest_url,
            format!(
                "Official index references newer budget year {newer_year}, while configured URL uses {configured_year}."
            ),
            recommendation.to_string(),
        );
    }
    let latest_year_observed = snapshot.body.contains(latest_year);
    if !latest_year_observed {
        return finding_evaluation(
            rule,
            Some(snapshot),
            SourceAuditStatus::ManualReview,
            SourceAuditSeverity::ManualReview,
            Some(snapshot.effective_url.clone()),
            format!(
                "Could not confirm latest budget year {latest_year} from reachable page; configured year is {configured_year}."
            ),
            recommendation.to_string(),
        );
    }
    if configured_year == latest_year {
        return ok_evaluation(
            rule,
            snapshot,
            format!("Configured budget year {configured_year} is current."),
        );
    }
    let latest_url = discover_latest_year_url(snapshot, latest_year);
    finding_evaluation(
        rule,
        Some(snapshot),
        SourceAuditStatus::Drift,
        SourceAuditSeverity::Warning,
        latest_url,
        format!(
            "Official index references budget year {latest_year}, while configured URL uses {configured_year}."
        ),
        recommendation.to_string(),
    )
}

fn evaluate_directory_listing(
    rule: &SourceLocationRule,
    snapshot: &SourceUrlSnapshot,
    required_patterns: &[&str],
    recommendation: &str,
) -> RuleEvaluation {
    if !is_success(snapshot.status) {
        return evaluate_reachable(rule, snapshot, recommendation);
    }
    let missing = required_patterns
        .iter()
        .filter(|pattern| !snapshot.body.contains(**pattern))
        .copied()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return ok_evaluation(
            rule,
            snapshot,
            "Directory listing contains expected current report patterns.".to_string(),
        );
    }

    finding_evaluation(
        rule,
        Some(snapshot),
        SourceAuditStatus::Drift,
        SourceAuditSeverity::Warning,
        Some(snapshot.effective_url.clone()),
        format!(
            "Directory listing is missing expected patterns: {}.",
            missing.join(", ")
        ),
        recommendation.to_string(),
    )
}

fn evaluate_world_bank_bready(
    rule: &SourceLocationRule,
    snapshot: &SourceUrlSnapshot,
    recommendation: &str,
) -> RuleEvaluation {
    if !is_success(snapshot.status) {
        return evaluate_reachable(rule, snapshot, recommendation);
    }

    let parsed = serde_json::from_str::<Value>(&snapshot.body);
    let Ok(value) = parsed else {
        return finding_evaluation(
            rule,
            Some(snapshot),
            SourceAuditStatus::Error,
            SourceAuditSeverity::Error,
            Some(snapshot.effective_url.clone()),
            "World Bank B-READY API response was not valid JSON.".to_string(),
            recommendation.to_string(),
        );
    };

    let Some(latest) = latest_australia_bready_values(&value) else {
        return finding_evaluation(
            rule,
            Some(snapshot),
            SourceAuditStatus::ManualReview,
            SourceAuditSeverity::ManualReview,
            Some(snapshot.effective_url.clone()),
            "World Bank B-READY API response did not include Australia values.".to_string(),
            recommendation.to_string(),
        );
    };

    if latest.null_count > 0 {
        return finding_evaluation(
            rule,
            Some(snapshot),
            SourceAuditStatus::ManualReview,
            SourceAuditSeverity::ManualReview,
            Some(snapshot.effective_url.clone()),
            format!(
                "Observed {} null Australia B-READY values in latest period {}.",
                latest.null_count, latest.period
            ),
            recommendation.to_string(),
        );
    }

    ok_evaluation(
        rule,
        snapshot,
        format!(
            "World Bank B-READY API returned {} non-null Australia values in latest period {}.",
            latest.value_count, latest.period
        ),
    )
}

fn ok_evaluation(
    rule: &SourceLocationRule,
    snapshot: &SourceUrlSnapshot,
    evidence: String,
) -> RuleEvaluation {
    RuleEvaluation {
        result: SourceAuditResult {
            source_id: rule.source_id.to_string(),
            dataflow_id: rule.dataflow_id.to_string(),
            status: SourceAuditStatus::Ok,
            current_url: rule.current_url.to_string(),
            effective_url: Some(snapshot.effective_url.clone()),
            http_status: Some(snapshot.status),
            evidence,
        },
        finding: None,
    }
}

fn finding_evaluation(
    rule: &SourceLocationRule,
    snapshot: Option<&SourceUrlSnapshot>,
    status: SourceAuditStatus,
    severity: SourceAuditSeverity,
    latest_url: Option<String>,
    evidence: String,
    recommendation: String,
) -> RuleEvaluation {
    RuleEvaluation {
        result: SourceAuditResult {
            source_id: rule.source_id.to_string(),
            dataflow_id: rule.dataflow_id.to_string(),
            status,
            current_url: rule.current_url.to_string(),
            effective_url: snapshot.map(|snapshot| snapshot.effective_url.clone()),
            http_status: snapshot.map(|snapshot| snapshot.status),
            evidence: evidence.clone(),
        },
        finding: Some(SourceAuditFinding {
            source_id: rule.source_id.to_string(),
            dataflow_id: rule.dataflow_id.to_string(),
            severity,
            current_url: rule.current_url.to_string(),
            latest_url,
            evidence,
            recommendation,
        }),
    }
}

fn aggregate_status(results: &[SourceAuditResult]) -> SourceAuditStatus {
    if results
        .iter()
        .any(|result| result.status == SourceAuditStatus::Error)
    {
        SourceAuditStatus::Error
    } else if results
        .iter()
        .any(|result| result.status == SourceAuditStatus::Drift)
    {
        SourceAuditStatus::Drift
    } else if results
        .iter()
        .any(|result| result.status == SourceAuditStatus::ManualReview)
    {
        SourceAuditStatus::ManualReview
    } else {
        SourceAuditStatus::Ok
    }
}

fn is_success(status: u16) -> bool {
    StatusCode::from_u16(status).is_ok_and(|status| status.is_success())
}

fn is_soft_access_status(status: u16) -> bool {
    status == 0
        || matches!(
            StatusCode::from_u16(status),
            Ok(StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS)
        )
}

fn discover_latest_year_url(snapshot: &SourceUrlSnapshot, latest_year: &str) -> Option<String> {
    let latest_index = snapshot.body.find(latest_year)?;
    let prefix = &snapshot.body[..latest_index];
    let href_start = prefix
        .rfind("href=\"")
        .map(|index| index + "href=\"".len())?;
    let href_suffix = &snapshot.body[href_start..];
    let href_end = href_suffix.find('"')?;
    let href = &href_suffix[..href_end];
    resolve_url(&snapshot.effective_url, href).or_else(|| Some(href.to_string()))
}

fn discover_newer_budget_year(body: &str, configured_year: &str) -> Option<String> {
    let configured_start = budget_year_start(configured_year)?;
    let bytes = body.as_bytes();
    let mut newest: Option<(u16, String)> = None;

    for window in bytes.windows(7) {
        if !(window[0] == b'2'
            && window[1] == b'0'
            && window[2].is_ascii_digit()
            && window[3].is_ascii_digit()
            && window[4] == b'-'
            && window[5].is_ascii_digit()
            && window[6].is_ascii_digit())
        {
            continue;
        }
        let Ok(candidate) = std::str::from_utf8(window) else {
            continue;
        };
        let Some(candidate_start) = budget_year_start(candidate) else {
            continue;
        };
        if candidate_start > configured_start
            && newest
                .as_ref()
                .is_none_or(|(newest_start, _)| candidate_start > *newest_start)
        {
            newest = Some((candidate_start, candidate.to_string()));
        }
    }

    newest.map(|(_, year)| year)
}

fn budget_year_start(year: &str) -> Option<u16> {
    year.get(0..4)?.parse().ok()
}

fn resolve_url(base: &str, href: &str) -> Option<String> {
    let base = reqwest::Url::parse(base).ok()?;
    base.join(href).ok().map(|url| url.to_string())
}

#[derive(Debug, PartialEq, Eq)]
struct LatestAustraliaBreadyValues {
    period: String,
    value_count: usize,
    null_count: usize,
}

fn latest_australia_bready_values(value: &Value) -> Option<LatestAustraliaBreadyValues> {
    let mut values = Vec::new();
    collect_australia_bready_values(value, &mut values);
    let latest_period = values.iter().map(|(period, _)| period).max()?.clone();
    let latest_values = values
        .iter()
        .filter(|(period, _)| period == &latest_period)
        .collect::<Vec<_>>();
    let null_count = latest_values.iter().filter(|(_, is_null)| *is_null).count();

    Some(LatestAustraliaBreadyValues {
        period: latest_period,
        value_count: latest_values.len(),
        null_count,
    })
}

fn collect_australia_bready_values(value: &Value, values: &mut Vec<(String, bool)>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_australia_bready_values(item, values);
            }
        }
        Value::Object(map) => {
            let is_aus = map
                .get("countryiso3code")
                .and_then(Value::as_str)
                .is_some_and(|code| code == "AUS");
            if is_aus {
                if let Some(period) = map.get("date").and_then(period_string) {
                    values.push((period, map.get("value").is_none_or(Value::is_null)));
                }
            }
            for item in map.values() {
                collect_australia_bready_values(item, values);
            }
        }
        _ => {}
    }
}

fn period_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn markdown_escape(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

const DEFAULT_RULES: [SourceLocationRule; 30] = [
    SourceLocationRule::new(
        "abs",
        "abs.cpi",
        "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI?detail=allstubs",
        SourceLocationCheck::ContainsAny {
            needles: &["CPI", "Consumer Price Index"],
            recommendation: "Review the ABS CPI dataflow endpoint and update the ABS adapter source URL if it moved.",
        },
    ),
    SourceLocationRule::new(
        "abs",
        "abs.building_approvals",
        "https://www.abs.gov.au/statistics/industry/building-and-construction/building-approvals-australia/latest-release",
        SourceLocationCheck::ContainsAny {
            needles: &["Building Approvals", "latest-release"],
            recommendation: "Review the ABS Building Approvals latest-release page.",
        },
    ),
    SourceLocationRule::new(
        "abs",
        "abs.building_activity",
        "https://www.abs.gov.au/statistics/industry/building-and-construction/building-activity-australia/latest-release",
        SourceLocationCheck::ContainsAny {
            needles: &["Building Activity", "latest-release"],
            recommendation: "Review the ABS Building Activity latest-release page.",
        },
    ),
    SourceLocationRule::new(
        "abs",
        "abs.dwelling_completion_times",
        "https://www.abs.gov.au/articles/average-dwelling-completion-times",
        SourceLocationCheck::ContainsAny {
            needles: &["dwelling completion", "Average dwelling completion times"],
            recommendation: "Review the ABS dwelling completion times article location.",
        },
    ),
    SourceLocationRule::new(
        "rba",
        "rba.statistical_tables",
        "https://www.rba.gov.au/statistics/tables/",
        SourceLocationCheck::ContainsAny {
            needles: &["Statistical Tables", "csv"],
            recommendation: "Review the RBA statistical tables index and table URLs.",
        },
    ),
    SourceLocationRule::new(
        "apra",
        "apra.quarterly_statistics",
        "https://apra.gov.au/news-and-publications/quarterly-authorised-deposit-taking-institution-statistics",
        SourceLocationCheck::ContainsAny {
            needles: &[
                "Quarterly authorised deposit-taking institution statistics",
                "xlsx",
            ],
            recommendation: "Review the APRA quarterly ADI statistics release page.",
        },
    ),
    SourceLocationRule::new(
        "apra",
        "apra.super_asset_allocation",
        "https://www.apra.gov.au/news-and-publications/quarterly-superannuation-statistics",
        SourceLocationCheck::ContainsAny {
            needles: &["Quarterly superannuation statistics", "superannuation"],
            recommendation: "Review the APRA superannuation release semantics before scoring this dataflow.",
        },
    ),
    SourceLocationRule::new(
        "aemo",
        "aemo.dispatch",
        "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
        SourceLocationCheck::DirectoryListing {
            required_patterns: &["PUBLIC_DISPATCHIS_", ".zip"],
            recommendation: "Review the AEMO NEMWeb DispatchIS directory if current ZIP reports disappear.",
        },
    ),
    SourceLocationRule::new(
        "aemo",
        "aemo.generation_mix",
        "https://nemweb.com.au/Reports/Current/Next_Day_Actual_Gen/",
        SourceLocationCheck::DirectoryListing {
            required_patterns: &["PUBLIC_NEXT_DAY_ACTUAL_GEN_", ".zip"],
            recommendation: "Review AEMO NEMWeb Next Day Actual Gen directory if current ZIP reports disappear.",
        },
    ),
    SourceLocationRule::new(
        "aemo",
        "aemo.dispatchability_capacity",
        "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
        SourceLocationCheck::DirectoryListing {
            required_patterns: &["PUBLIC_DISPATCHIS_", ".zip"],
            recommendation: "Review AEMO dispatchability-capacity proxy source semantics.",
        },
    ),
    SourceLocationRule::new(
        "asx",
        "asx.market_statistics",
        "https://www.asx.com.au/about/market-statistics/historical-market-statistics",
        SourceLocationCheck::ContainsAny {
            needles: &["Historical market statistics", "market statistics"],
            recommendation: "Review the ASX historical market statistics page.",
        },
    ),
    SourceLocationRule::new(
        "asx",
        "asx.market_statistics",
        "https://www.asx.com.au/legals/terms-of-use",
        SourceLocationCheck::ContainsAny {
            needles: &["Terms of Use", "ASX"],
            recommendation: "Review the ASX Terms of Use location used for license attribution.",
        },
    ),
    SourceLocationRule::new(
        "asx",
        "asx.announcements",
        "https://www.asx.com.au/connectivity-and-data/information-services/company-news",
        SourceLocationCheck::LicensedProduct {
            recommendation: "Review the ASX company announcements product page; configured licensed feed URLs may remain empty.",
        },
    ),
    SourceLocationRule::new(
        "asx",
        "asx.eod",
        "https://www.asx.com.au/connectivity-and-data/information-services/reference-data",
        SourceLocationCheck::LicensedProduct {
            recommendation: "Review the ASX reference-data product page; configured licensed EOD feed URLs may remain empty.",
        },
    ),
    SourceLocationRule::new(
        "nhsac",
        "nhsac.housing_accord_progress",
        "https://nhsac.gov.au/publications",
        SourceLocationCheck::ContainsAny {
            needles: &["publications", "Housing Accord"],
            recommendation: "Review the NHSAC publications page for housing accord progress inputs.",
        },
    ),
    SourceLocationRule::new(
        "pc",
        "pc.productivity_bulletin",
        "https://www.pc.gov.au/ongoing/productivity-insights",
        SourceLocationCheck::ContainsAny {
            needles: &["Productivity", "Insights"],
            recommendation: "Review the Productivity Commission productivity insights page.",
        },
    ),
    SourceLocationRule::new(
        "worldbank",
        "worldbank.bready",
        "https://api.worldbank.org/v2/country/AUS/indicator/IC.BRE.BE.OS?format=json&source=2&per_page=100",
        SourceLocationCheck::WorldBankBreadyApi {
            recommendation: "Review World Bank B-READY Australia values before scoring this source.",
        },
    ),
    SourceLocationRule::new(
        "treasury",
        "treasury.budget_papers",
        "https://budget.gov.au/content/bp4/index.htm",
        SourceLocationCheck::BudgetYear {
            configured_year: "2026-27",
            latest_year: "2026-27",
            recommendation: "Review the Australian Government Budget Paper No. 4 page when a newer federal budget appears.",
        },
    ),
    SourceLocationRule::new(
        "state-budgets",
        "state_budgets.nsw_budget",
        "https://www.nsw.gov.au/business-and-economy/nsw-budget/2026-27-budget-papers",
        SourceLocationCheck::BudgetYear {
            configured_year: "2026-27",
            latest_year: "2026-27",
            recommendation: "Review the NSW budget source when a newer budget year appears.",
        },
    ),
    SourceLocationRule::new(
        "state-budgets",
        "state_budgets.vic_budget",
        "https://www.budget.vic.gov.au/budget-papers",
        SourceLocationCheck::BudgetYear {
            configured_year: "2026-27",
            latest_year: "2026-27",
            recommendation: "Review the Victorian budget papers index when a newer budget appears.",
        },
    ),
    SourceLocationRule::new(
        "state-budgets",
        "state_budgets.qld_budget",
        "https://budget.qld.gov.au/budget-papers/",
        SourceLocationCheck::BudgetYear {
            configured_year: "2026-27",
            latest_year: "2026-27",
            recommendation: "Review the Queensland budget papers index when a newer budget appears.",
        },
    ),
    SourceLocationRule::new(
        "state-planning",
        "state_planning.nsw_da_processing",
        "https://www.planning.nsw.gov.au/data-and-insights",
        SourceLocationCheck::ContainsAny {
            needles: &["data", "insights"],
            recommendation: "Review NSW Planning data-and-insights links for DA processing inputs.",
        },
    ),
    SourceLocationRule::new(
        "state-planning",
        "state_planning.vic_permit_activity",
        "https://www.planning.vic.gov.au/guides-and-resources/data-insights-and-analytics/planning-permit-activity-in-victoria",
        SourceLocationCheck::ContainsAny {
            needles: &["planning permit", "activity"],
            recommendation: "Review Victoria Planning permit activity source links.",
        },
    ),
    SourceLocationRule::new(
        "oxford",
        "oxford.gari",
        "https://oxfordinsights.com/ai-readiness/ai-readiness-index/",
        SourceLocationCheck::ContainsAny {
            needles: &["Government AI Readiness", "AI Readiness"],
            recommendation: "Review the Oxford Insights Government AI Readiness Index source page.",
        },
    ),
    SourceLocationRule::new(
        "naic",
        "naic.ai_adoption_tracker",
        "https://www.ai.gov.au/news-and-insights/reports/ai-adoption-tracker",
        SourceLocationCheck::ContainsAny {
            needles: &["AI adoption", "tracker"],
            recommendation: "Review the NAIC/industry AI adoption tracker source page.",
        },
    ),
    SourceLocationRule::new(
        "abs",
        "abs.ai_rd",
        "https://www.abs.gov.au/statistics/research",
        SourceLocationCheck::ContainsAny {
            needles: &["Research and Development", "business"],
            recommendation: "Review ABS R&D and AI-related data source pages.",
        },
    ),
    SourceLocationRule::new(
        "home-affairs",
        "home_affairs.skillselect_talent_proxy",
        "https://immi.homeaffairs.gov.au/visas/working-in-australia/skillselect/invitation-rounds",
        SourceLocationCheck::ContainsAny {
            needles: &["SkillSelect", "skilled"],
            recommendation: "Review the Home Affairs SkillSelect invitation-rounds source link.",
        },
    ),
    SourceLocationRule::new(
        "state-capital",
        "state_capital.vic_major_projects",
        "https://bigbuild.vic.gov.au/projects",
        SourceLocationCheck::ContainsAny {
            needles: &["projects", "Big Build"],
            recommendation: "Review Victoria Big Build major projects source links.",
        },
    ),
    SourceLocationRule::new(
        "state-capital",
        "state_capital.budget_capital_papers",
        "https://budget.gov.au/",
        SourceLocationCheck::ContainsAny {
            needles: &["Budget", "capital"],
            recommendation: "Review federal/state budget capital papers source semantics.",
        },
    ),
    SourceLocationRule::new(
        "compute",
        "compute.au_datacentre_capacity_mw",
        "https://example.test/compute-capacity",
        SourceLocationCheck::ManualPlaceholder {
            reason: "example.test is a placeholder source.",
            recommendation: "Replace compute.au_datacentre_capacity_mw with reviewed aemo.data_centre_demand IASR/ESOO demand-proxy semantics.",
        },
    ),
];
