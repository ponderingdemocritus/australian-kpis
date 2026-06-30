//! Source-location audit rule evaluation and reporting.

use std::{collections::BTreeMap, time::Duration};

use anyhow::Context;
use au_kpis_source_register::{
    AuditPolicy as RegisterAuditPolicy, SOURCE_REGISTER_VERSION, SourceRegisterError,
    load_source_register,
};
use chrono::{DateTime, NaiveDate, Utc};
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::Value;

const USER_AGENT: &str = concat!("au-kpis-source-location-audit/", env!("CARGO_PKG_VERSION"));
const HTTP_TIMEOUT: Duration = Duration::from_secs(20);

/// One source-specific location audit rule.
#[derive(Debug, Clone)]
pub struct SourceLocationRule {
    /// Source id in the source catalog.
    pub source_id: String,
    /// Dataflow id protected by this rule.
    pub dataflow_id: String,
    /// Currently configured or canonical source URL to check.
    pub current_url: String,
    /// Source-specific semantic check.
    pub check: SourceLocationCheck,
    /// Register source status when this rule is register-derived.
    pub source_status: Option<String>,
    /// Register audit policy kind when this rule is register-derived.
    pub audit_policy_kind: Option<String>,
}

impl SourceLocationRule {
    /// Build a source-location audit rule.
    #[must_use]
    pub fn new(
        source_id: impl Into<String>,
        dataflow_id: impl Into<String>,
        current_url: impl Into<String>,
        check: SourceLocationCheck,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            dataflow_id: dataflow_id.into(),
            current_url: current_url.into(),
            check,
            source_status: None,
            audit_policy_kind: None,
        }
    }

    /// Attach source-register metadata to an audit rule.
    #[must_use]
    pub fn with_register_metadata(
        mut self,
        source_status: impl Into<String>,
        audit_policy_kind: impl Into<String>,
    ) -> Self {
        self.source_status = Some(source_status.into());
        self.audit_policy_kind = Some(audit_policy_kind.into());
        self
    }
}

/// Source-specific location and freshness semantics.
#[derive(Debug, Clone)]
pub enum SourceLocationCheck {
    /// URL should be reachable with a successful HTTP status.
    Reachable {
        /// Human recommendation when the URL cannot be reached.
        recommendation: String,
    },
    /// Page body should contain at least one expected semantic hint.
    ContainsAny {
        /// Text fragments accepted as evidence of the current page.
        needles: Vec<String>,
        /// Human recommendation when no hint is present.
        recommendation: String,
    },
    /// Current URL is known to have a newer canonical replacement.
    CanonicalUrl {
        /// Expected canonical or effective URL.
        expected_url: String,
        /// Human recommendation for the tracked issue.
        recommendation: String,
    },
    /// Budget index must expose the latest expected budget year.
    BudgetYear {
        /// Budget year currently configured in the adapter/catalog.
        configured_year: String,
        /// Latest budget year expected on the official index.
        latest_year: String,
        /// Human recommendation for the tracked issue.
        recommendation: String,
    },
    /// Directory listing must include current report filename patterns.
    DirectoryListing {
        /// Required body fragments that identify the current reports.
        required_patterns: Vec<String>,
        /// Human recommendation when patterns disappear.
        recommendation: String,
    },
    /// Licensed feed dataflow where the public product page is the auditable URL.
    LicensedProduct {
        /// Human recommendation when the product page is unreachable.
        recommendation: String,
    },
    /// Official source that is known to block or challenge automated requests.
    BotFiltered {
        /// HTTP statuses accepted as evidence of bot filtering.
        expected_statuses: Vec<u16>,
        /// Body text that must appear if the source unexpectedly returns HTTP success.
        semantic_fallback: Option<String>,
        /// Human recommendation for preserving auditability.
        recommendation: String,
    },
    /// World Bank B-READY API semantics for Australia availability.
    WorldBankBreadyApi {
        /// Human recommendation when values are unresolved.
        recommendation: String,
    },
    /// Manual placeholder that must remain visible until replaced.
    ManualPlaceholder {
        /// Why this source cannot pass automatically.
        reason: String,
        /// Human recommendation for the tracked issue.
        recommendation: String,
    },
    /// Manual register-only source with no live URL audit and due-date checks.
    ManualRegisterOnly {
        /// Why this source is reviewed manually.
        reason: String,
        /// Last reviewed date from the register.
        reviewed_at: String,
        /// Next manual review due date from the register.
        manual_review_due_at: String,
        /// Human recommendation for the tracked issue.
        recommendation: String,
    },
}

impl SourceLocationCheck {
    fn requires_http_snapshot(&self) -> bool {
        !matches!(
            self,
            Self::ManualPlaceholder { .. } | Self::ManualRegisterOnly { .. }
        )
    }
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
    /// At least one configured source appears reachable only through challenged access.
    BotFiltered,
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
    /// Public source appears protected by bot filtering or access challenge.
    BotFiltered,
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
    /// Register source status when the rule is register-derived.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_status: Option<String>,
    /// Register audit policy kind when the rule is register-derived.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit_policy_kind: Option<String>,
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
    /// Source register version used by the scheduler audit.
    pub register_version: String,
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
        markdown.push_str(&format!("- Source register: `{}`\n", self.register_version));
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
            Self::BotFiltered => "bot_filtered",
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
            Self::BotFiltered => "bot_filtered",
            Self::Error => "error",
        }
    }
}

/// Register-backed rule catalog for implemented and APS-scoped source dataflows.
pub fn default_source_location_rules() -> Result<Vec<SourceLocationRule>, SourceRegisterError> {
    source_register_location_rules()
}

fn source_register_location_rules() -> Result<Vec<SourceLocationRule>, SourceRegisterError> {
    let register = load_source_register()?;
    let mut rules = Vec::new();

    for dataflow in register.dataflows {
        if !dataflow.status.emits_source_location_rules() {
            continue;
        }
        let source_status = source_status_name(dataflow.status);
        if let Some(rule) = source_register_rule(
            &dataflow.source_id,
            &dataflow.dataflow_id,
            &dataflow.canonical_url,
            &dataflow.audit_policy,
            source_status,
            dataflow.reviewed_at.as_deref(),
            dataflow.manual_review_due_at.as_deref(),
        ) {
            rules.push(rule);
        }
        for additional in &dataflow.additional_audit_policies {
            if let Some(rule) = source_register_rule(
                &dataflow.source_id,
                &dataflow.dataflow_id,
                &additional.url,
                &additional.policy,
                source_status,
                dataflow.reviewed_at.as_deref(),
                dataflow.manual_review_due_at.as_deref(),
            ) {
                rules.push(rule);
            }
        }
    }

    Ok(rules)
}

fn source_register_rule(
    source_id: &str,
    dataflow_id: &str,
    current_url: &str,
    policy: &RegisterAuditPolicy,
    source_status: &'static str,
    reviewed_at: Option<&str>,
    manual_review_due_at: Option<&str>,
) -> Option<SourceLocationRule> {
    let audit_policy_kind = audit_policy_kind(policy);
    let check = match policy {
        RegisterAuditPolicy::ContainsAny {
            needles,
            recommendation,
        } => SourceLocationCheck::ContainsAny {
            needles: needles.clone(),
            recommendation: recommendation.clone(),
        },
        RegisterAuditPolicy::DirectoryListing {
            required_patterns,
            recommendation,
        } => SourceLocationCheck::DirectoryListing {
            required_patterns: required_patterns.clone(),
            recommendation: recommendation.clone(),
        },
        RegisterAuditPolicy::BudgetYear {
            configured_year,
            latest_year,
            recommendation,
        } => SourceLocationCheck::BudgetYear {
            configured_year: configured_year.clone(),
            latest_year: latest_year.clone(),
            recommendation: recommendation.clone(),
        },
        RegisterAuditPolicy::LicensedProduct { recommendation } => {
            SourceLocationCheck::LicensedProduct {
                recommendation: recommendation.clone(),
            }
        }
        RegisterAuditPolicy::WorldBankBreadyApi { recommendation } => {
            SourceLocationCheck::WorldBankBreadyApi {
                recommendation: recommendation.clone(),
            }
        }
        RegisterAuditPolicy::ManualPlaceholder {
            reason,
            recommendation,
        } => SourceLocationCheck::ManualPlaceholder {
            reason: reason.clone(),
            recommendation: recommendation.clone(),
        },
        RegisterAuditPolicy::ManualRegisterOnly {
            reason,
            recommendation,
        } => SourceLocationCheck::ManualRegisterOnly {
            reason: reason.clone(),
            reviewed_at: reviewed_at.unwrap_or("").to_string(),
            manual_review_due_at: manual_review_due_at.unwrap_or("").to_string(),
            recommendation: recommendation.clone(),
        },
        RegisterAuditPolicy::BotFiltered {
            expected_statuses,
            semantic_fallback,
            recommendation,
        } => SourceLocationCheck::BotFiltered {
            expected_statuses: expected_statuses.clone(),
            semantic_fallback: semantic_fallback.clone(),
            recommendation: recommendation.clone(),
        },
    };

    Some(
        SourceLocationRule::new(source_id, dataflow_id, current_url, check)
            .with_register_metadata(source_status, audit_policy_kind),
    )
}

fn source_status_name(status: au_kpis_source_register::SourceStatus) -> &'static str {
    match status {
        au_kpis_source_register::SourceStatus::Active => "active",
        au_kpis_source_register::SourceStatus::ManualPending => "manual_pending",
        au_kpis_source_register::SourceStatus::VisibleUnscored => "visible_unscored",
        au_kpis_source_register::SourceStatus::CoverageGap => "coverage_gap",
        au_kpis_source_register::SourceStatus::LicensedFeed => "licensed_feed",
        au_kpis_source_register::SourceStatus::Placeholder => "placeholder",
        au_kpis_source_register::SourceStatus::Retired => "retired",
    }
}

fn audit_policy_kind(policy: &RegisterAuditPolicy) -> &'static str {
    match policy {
        RegisterAuditPolicy::ContainsAny { .. } => "contains_any",
        RegisterAuditPolicy::DirectoryListing { .. } => "directory_listing",
        RegisterAuditPolicy::BudgetYear { .. } => "budget_year",
        RegisterAuditPolicy::LicensedProduct { .. } => "licensed_product",
        RegisterAuditPolicy::WorldBankBreadyApi { .. } => "world_bank_bready_api",
        RegisterAuditPolicy::ManualPlaceholder { .. } => "manual_placeholder",
        RegisterAuditPolicy::ManualRegisterOnly { .. } => "manual_register_only",
        RegisterAuditPolicy::BotFiltered { .. } => "bot_filtered",
    }
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
        if rule.check.requires_http_snapshot() {
            snapshots.push(fetch_snapshot(&client, &rule.current_url).await?);
        }
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
        let snapshot = snapshots_by_url.get(rule.current_url.as_str()).copied();
        let evaluation = evaluate_rule(rule, snapshot, generated_at);
        if let Some(finding) = evaluation.finding {
            findings.push(finding);
        }
        results.push(evaluation.result);
    }

    let status = aggregate_status(&results);
    let findings_total = findings.len();
    SourceLocationAuditReport {
        generated_at,
        register_version: SOURCE_REGISTER_VERSION.to_string(),
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
    generated_at: DateTime<Utc>,
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

    if let SourceLocationCheck::ManualRegisterOnly {
        reason,
        reviewed_at,
        manual_review_due_at,
        recommendation,
    } = &rule.check
    {
        return evaluate_manual_register_only(
            rule,
            generated_at,
            reason,
            reviewed_at,
            manual_review_due_at,
            recommendation,
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
        SourceLocationCheck::BotFiltered {
            expected_statuses,
            semantic_fallback,
            recommendation,
        } => evaluate_bot_filtered(
            rule,
            snapshot,
            expected_statuses,
            semantic_fallback.as_deref(),
            recommendation,
        ),
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
        SourceLocationCheck::ManualRegisterOnly { .. } => {
            unreachable!("handled before snapshot")
        }
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

    if snapshot.status == 0 {
        return finding_evaluation(
            rule,
            Some(snapshot),
            SourceAuditStatus::Error,
            SourceAuditSeverity::Error,
            Some(snapshot.effective_url.clone()),
            "URL request failed before an HTTP response was received.".to_string(),
            recommendation.to_string(),
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

fn evaluate_manual_register_only(
    rule: &SourceLocationRule,
    generated_at: DateTime<Utc>,
    reason: &str,
    reviewed_at: &str,
    manual_review_due_at: &str,
    recommendation: &str,
) -> RuleEvaluation {
    let generated_date = generated_at.date_naive();
    let reviewed = match NaiveDate::parse_from_str(reviewed_at, "%Y-%m-%d") {
        Ok(value) => value,
        Err(err) => {
            return finding_evaluation(
                rule,
                None,
                SourceAuditStatus::Error,
                SourceAuditSeverity::Error,
                None,
                format!("Manual register reviewed_at `{reviewed_at}` is invalid: {err}."),
                recommendation.to_string(),
            );
        }
    };
    let due = match NaiveDate::parse_from_str(manual_review_due_at, "%Y-%m-%d") {
        Ok(value) => value,
        Err(err) => {
            return finding_evaluation(
                rule,
                None,
                SourceAuditStatus::Error,
                SourceAuditSeverity::Error,
                None,
                format!(
                    "Manual register manual_review_due_at `{manual_review_due_at}` is invalid: {err}."
                ),
                recommendation.to_string(),
            );
        }
    };

    if generated_date > due {
        return finding_evaluation(
            rule,
            None,
            SourceAuditStatus::ManualReview,
            SourceAuditSeverity::ManualReview,
            None,
            format!("{reason} Last reviewed {reviewed}; manual review was due {due}."),
            recommendation.to_string(),
        );
    }

    manual_ok_evaluation(
        rule,
        format!("{reason} Last reviewed {reviewed}; next manual review is due {due}."),
    )
}

fn evaluate_bot_filtered(
    rule: &SourceLocationRule,
    snapshot: &SourceUrlSnapshot,
    expected_statuses: &[u16],
    semantic_fallback: Option<&str>,
    recommendation: &str,
) -> RuleEvaluation {
    if is_success(snapshot.status) {
        let Some(semantic_fallback) = semantic_fallback else {
            return finding_evaluation(
                rule,
                Some(snapshot),
                SourceAuditStatus::ManualReview,
                SourceAuditSeverity::ManualReview,
                Some(snapshot.effective_url.clone()),
                format!(
                    "URL returned HTTP {} for a bot-filter policy without a semantic fallback.",
                    snapshot.status
                ),
                recommendation.to_string(),
            );
        };
        if !snapshot.body.contains(semantic_fallback) {
            return finding_evaluation(
                rule,
                Some(snapshot),
                SourceAuditStatus::ManualReview,
                SourceAuditSeverity::ManualReview,
                Some(snapshot.effective_url.clone()),
                format!(
                    "URL returned HTTP {} but body did not contain semantic fallback `{semantic_fallback}`.",
                    snapshot.status
                ),
                recommendation.to_string(),
            );
        }
        return ok_evaluation(
            rule,
            snapshot,
            format!(
                "URL reachable with HTTP {} despite bot-filter policy.",
                snapshot.status
            ),
        );
    }

    if snapshot.status == 0 {
        return finding_evaluation(
            rule,
            Some(snapshot),
            SourceAuditStatus::Error,
            SourceAuditSeverity::Error,
            Some(snapshot.effective_url.clone()),
            "URL request failed before an HTTP response was received.".to_string(),
            recommendation.to_string(),
        );
    }

    if expected_statuses.contains(&snapshot.status) || is_soft_access_status(snapshot.status) {
        return finding_evaluation(
            rule,
            Some(snapshot),
            SourceAuditStatus::BotFiltered,
            SourceAuditSeverity::BotFiltered,
            Some(snapshot.effective_url.clone()),
            format!(
                "URL returned HTTP {}; source appears bot-filtered or access-challenged.",
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
        format!(
            "URL returned HTTP {}, which is outside expected bot-filter statuses.",
            snapshot.status
        ),
        recommendation.to_string(),
    )
}

fn evaluate_contains_any(
    rule: &SourceLocationRule,
    snapshot: &SourceUrlSnapshot,
    needles: &[String],
    recommendation: &str,
) -> RuleEvaluation {
    if !is_success(snapshot.status) {
        return evaluate_reachable(rule, snapshot, recommendation);
    }
    if let Some(needle) = needles
        .iter()
        .find(|needle| snapshot.body.contains(needle.as_str()))
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
    if let Some((newer_year, latest_url)) =
        discover_newer_budget_year_link(snapshot, configured_year)
    {
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
    required_patterns: &[String],
    recommendation: &str,
) -> RuleEvaluation {
    if !is_success(snapshot.status) {
        return evaluate_reachable(rule, snapshot, recommendation);
    }
    let missing = required_patterns
        .iter()
        .filter(|pattern| !snapshot.body.contains(pattern.as_str()))
        .map(String::as_str)
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

fn manual_ok_evaluation(rule: &SourceLocationRule, evidence: String) -> RuleEvaluation {
    RuleEvaluation {
        result: SourceAuditResult {
            source_id: rule.source_id.to_string(),
            dataflow_id: rule.dataflow_id.to_string(),
            status: SourceAuditStatus::Ok,
            source_status: rule.source_status.as_deref().map(str::to_string),
            audit_policy_kind: rule.audit_policy_kind.as_deref().map(str::to_string),
            current_url: rule.current_url.to_string(),
            effective_url: None,
            http_status: None,
            evidence,
        },
        finding: None,
    }
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
            source_status: rule.source_status.as_deref().map(str::to_string),
            audit_policy_kind: rule.audit_policy_kind.as_deref().map(str::to_string),
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
            source_status: rule.source_status.as_deref().map(str::to_string),
            audit_policy_kind: rule.audit_policy_kind.as_deref().map(str::to_string),
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
        .any(|result| result.status == SourceAuditStatus::BotFiltered)
    {
        SourceAuditStatus::BotFiltered
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
    matches!(
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

fn discover_newer_budget_year_link(
    snapshot: &SourceUrlSnapshot,
    configured_year: &str,
) -> Option<(String, Option<String>)> {
    let configured_start = budget_year_start(configured_year)?;
    let bytes = snapshot.body.as_bytes();
    let mut newest: Option<(u16, String, Option<String>)> = None;

    for (index, window) in bytes.windows(7).enumerate() {
        let Ok(candidate) = std::str::from_utf8(window) else {
            continue;
        };
        let Some(candidate_start) = parse_budget_year_start(candidate) else {
            continue;
        };
        if !is_budget_year_boundary(bytes, index) {
            continue;
        };
        let Some(latest_url) =
            discover_anchor_url_containing(&snapshot.effective_url, &snapshot.body, index)
        else {
            continue;
        };
        if candidate_start > configured_start
            && newest
                .as_ref()
                .is_none_or(|(newest_start, _, _)| candidate_start > *newest_start)
        {
            newest = Some((candidate_start, candidate.to_string(), latest_url));
        }
    }

    newest.map(|(_, year, latest_url)| (year, latest_url))
}

fn discover_anchor_url_containing(
    base_url: &str,
    body: &str,
    index: usize,
) -> Option<Option<String>> {
    let prefix = body.get(..index)?;
    let suffix = body.get(index..)?;
    let open_anchor = prefix.rfind("<a ")?;
    let close_before = prefix.rfind("</a>");
    if close_before.is_some_and(|close| close > open_anchor) {
        return None;
    }
    suffix.find("</a>")?;
    let anchor_prefix = &body[open_anchor..index];
    let href_start = anchor_prefix
        .find("href=\"")
        .map(|href_index| open_anchor + href_index + "href=\"".len())?;
    let href_suffix = &body[href_start..];
    let href_end = href_suffix.find('"')?;
    let href = &href_suffix[..href_end];
    Some(resolve_url(base_url, href).or_else(|| Some(href.to_string())))
}

fn budget_year_start(year: &str) -> Option<u16> {
    parse_budget_year_start(year)
}

fn parse_budget_year_start(year: &str) -> Option<u16> {
    let bytes = year.as_bytes();
    if bytes.len() != 7
        || bytes[0] != b'2'
        || bytes[1] != b'0'
        || !bytes[2].is_ascii_digit()
        || !bytes[3].is_ascii_digit()
        || bytes[4] != b'-'
        || !bytes[5].is_ascii_digit()
        || !bytes[6].is_ascii_digit()
    {
        return None;
    }
    let start = year.get(0..4)?.parse::<u16>().ok()?;
    let suffix = year.get(5..7)?.parse::<u16>().ok()?;
    (suffix == (start + 1) % 100).then_some(start)
}

fn is_budget_year_boundary(bytes: &[u8], index: usize) -> bool {
    let before_ok = index == 0 || !bytes[index - 1].is_ascii_alphanumeric();
    let after_index = index + 7;
    let after_ok = after_index >= bytes.len() || !bytes[after_index].is_ascii_alphanumeric();
    before_ok && after_ok
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
