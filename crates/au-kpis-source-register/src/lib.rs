//! Versioned source governance register.
//!
//! The register is the reviewed source of truth for scoped dataflows,
//! source-location audit policy, manual review status, cadence, provenance,
//! and replacement notes.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Checked-in source register config.
pub const SOURCE_REGISTER_V1_TOML: &str = include_str!("../config/source-register.v1.toml");
/// Checked-in source register version.
pub const SOURCE_REGISTER_VERSION: &str = "source-register.v1";

/// Load and validate the checked-in source register.
pub fn load_source_register() -> Result<SourceRegister, SourceRegisterError> {
    parse_source_register(SOURCE_REGISTER_V1_TOML)
}

/// Parse and validate a TOML source register.
pub fn parse_source_register(raw: &str) -> Result<SourceRegister, SourceRegisterError> {
    let register: SourceRegister = toml::from_str(raw)?;
    validate_source_register(&register)?;
    Ok(register)
}

/// Validate source-register invariants.
pub fn validate_source_register(register: &SourceRegister) -> Result<(), SourceRegisterError> {
    require_text("version", &register.version)?;
    if register.version != SOURCE_REGISTER_VERSION {
        return Err(SourceRegisterError::InvalidRegister(format!(
            "register version must be `{SOURCE_REGISTER_VERSION}`"
        )));
    }
    if register.dataflows.is_empty() {
        return Err(SourceRegisterError::InvalidRegister(
            "register must contain at least one dataflow".into(),
        ));
    }

    let mut seen = BTreeSet::new();
    for dataflow in &register.dataflows {
        validate_dataflow(dataflow)?;
        if !seen.insert(dataflow.dataflow_id.as_str()) {
            return Err(SourceRegisterError::InvalidRegister(format!(
                "duplicate dataflow id `{}`",
                dataflow.dataflow_id
            )));
        }
    }
    Ok(())
}

fn validate_dataflow(dataflow: &SourceRegisterDataflow) -> Result<(), SourceRegisterError> {
    require_text("source_id", &dataflow.source_id)?;
    require_text("dataflow_id", &dataflow.dataflow_id)?;
    require_text("canonical_url", &dataflow.canonical_url)?;
    validate_http_url(
        "canonical_url",
        &dataflow.canonical_url,
        &dataflow.dataflow_id,
    )?;
    require_text("license", &dataflow.license)?;
    require_text("attribution", &dataflow.attribution)?;
    require_text("cadence", &dataflow.cadence)?;
    require_text("review_frequency", &dataflow.review_frequency)?;
    require_text("source_scope", &dataflow.source_scope)?;
    require_text_list(
        "provenance_requirements",
        &dataflow.provenance_requirements,
        &dataflow.dataflow_id,
    )?;
    require_text_list(
        "validation_requirements",
        &dataflow.validation_requirements,
        &dataflow.dataflow_id,
    )?;

    if matches!(
        dataflow.status,
        SourceStatus::ManualPending | SourceStatus::VisibleUnscored
    ) || matches!(
        dataflow.audit_policy,
        AuditPolicy::ManualRegisterOnly { .. }
    ) {
        require_optional_text(
            "retrieved_at",
            dataflow.retrieved_at.as_deref(),
            &dataflow.dataflow_id,
        )?;
        require_optional_text(
            "reviewed_by",
            dataflow.reviewed_by.as_deref(),
            &dataflow.dataflow_id,
        )?;
        require_optional_text(
            "reviewed_at",
            dataflow.reviewed_at.as_deref(),
            &dataflow.dataflow_id,
        )?;
        require_optional_text(
            "manual_review_due_at",
            dataflow.manual_review_due_at.as_deref(),
            &dataflow.dataflow_id,
        )?;
    }

    if matches!(dataflow.status, SourceStatus::Placeholder) {
        require_optional_text(
            "replacement_candidate",
            dataflow.replacement_candidate.as_deref(),
            &dataflow.dataflow_id,
        )?;
    }

    if matches!(
        dataflow.audit_policy,
        AuditPolicy::ManualRegisterOnly { .. }
    ) && !matches!(
        dataflow.status,
        SourceStatus::ManualPending
            | SourceStatus::VisibleUnscored
            | SourceStatus::CoverageGap
            | SourceStatus::Placeholder
    ) {
        return Err(SourceRegisterError::InvalidRegister(format!(
            "`{}` uses manual_register_only but status is {:?}",
            dataflow.dataflow_id, dataflow.status
        )));
    }

    validate_optional_iso_date(
        "retrieved_at",
        dataflow.retrieved_at.as_deref(),
        &dataflow.dataflow_id,
    )?;
    validate_optional_iso_date(
        "reviewed_at",
        dataflow.reviewed_at.as_deref(),
        &dataflow.dataflow_id,
    )?;
    validate_optional_iso_date(
        "manual_review_due_at",
        dataflow.manual_review_due_at.as_deref(),
        &dataflow.dataflow_id,
    )?;

    for additional in &dataflow.additional_audit_policies {
        validate_http_url(
            "additional_audit_policies.url",
            &additional.url,
            &dataflow.dataflow_id,
        )?;
    }

    Ok(())
}

fn require_text(field: &str, value: &str) -> Result<(), SourceRegisterError> {
    if value.trim().is_empty() {
        Err(SourceRegisterError::InvalidRegister(format!(
            "`{field}` must not be empty"
        )))
    } else {
        Ok(())
    }
}

fn require_optional_text(
    field: &str,
    value: Option<&str>,
    dataflow_id: &str,
) -> Result<(), SourceRegisterError> {
    if value.is_some_and(|value| !value.trim().is_empty()) {
        Ok(())
    } else {
        Err(SourceRegisterError::InvalidRegister(format!(
            "`{dataflow_id}` requires `{field}`"
        )))
    }
}

fn require_text_list(
    field: &str,
    values: &[String],
    dataflow_id: &str,
) -> Result<(), SourceRegisterError> {
    if values.is_empty() || values.iter().any(|value| value.trim().is_empty()) {
        Err(SourceRegisterError::InvalidRegister(format!(
            "`{dataflow_id}` requires non-empty `{field}`"
        )))
    } else {
        Ok(())
    }
}

fn validate_http_url(
    field: &str,
    value: &str,
    dataflow_id: &str,
) -> Result<(), SourceRegisterError> {
    let parsed = url::Url::parse(value).map_err(|err| {
        SourceRegisterError::InvalidRegister(format!(
            "`{dataflow_id}` has invalid `{field}` URL: {err}"
        ))
    })?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(SourceRegisterError::InvalidRegister(format!(
            "`{dataflow_id}` `{field}` must be an absolute HTTP(S) URL"
        )));
    }
    Ok(())
}

fn validate_optional_iso_date(
    field: &str,
    value: Option<&str>,
    dataflow_id: &str,
) -> Result<(), SourceRegisterError> {
    let Some(value) = value else {
        return Ok(());
    };
    if is_valid_iso_date(value) {
        Ok(())
    } else {
        Err(SourceRegisterError::InvalidRegister(format!(
            "`{dataflow_id}` `{field}` must use YYYY-MM-DD"
        )))
    }
}

fn is_valid_iso_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return false;
    }
    let year = parse_fixed_digits(&bytes[0..4]);
    let month = parse_fixed_digits(&bytes[5..7]);
    let day = parse_fixed_digits(&bytes[8..10]);
    let (Some(year), Some(month), Some(day)) = (year, month, day) else {
        return false;
    };
    if month == 0 || month > 12 || day == 0 {
        return false;
    }
    day <= days_in_month(year, month)
}

fn parse_fixed_digits(bytes: &[u8]) -> Option<u32> {
    bytes.iter().try_fold(0_u32, |acc, byte| {
        byte.is_ascii_digit()
            .then(|| acc * 10 + u32::from(byte - b'0'))
    })
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: u32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// Source register parse/validation error.
#[derive(Debug, Error)]
pub enum SourceRegisterError {
    /// TOML deserialization failed.
    #[error("parse source register TOML: {0}")]
    Toml(#[from] toml::de::Error),
    /// Source register content violates an invariant.
    #[error("invalid source register: {0}")]
    InvalidRegister(String),
}

/// Versioned source register.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRegister {
    /// Register version id.
    pub version: String,
    /// Registered dataflows.
    pub dataflows: Vec<SourceRegisterDataflow>,
}

/// One governed source/dataflow entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRegisterDataflow {
    /// Source id.
    pub source_id: String,
    /// Dataflow id.
    pub dataflow_id: String,
    /// Governance status.
    pub status: SourceStatus,
    /// Ownership area.
    pub owner_area: OwnerArea,
    /// Canonical audit/citation URL.
    pub canonical_url: String,
    /// License id or source-specific license note.
    pub license: String,
    /// Attribution text.
    pub attribution: String,
    /// Expected cadence.
    pub cadence: String,
    /// Review cadence for source-governance checks.
    pub review_frequency: String,
    /// Scope authority such as spec anchor, issue id, or scorecard config.
    pub source_scope: String,
    /// Source provenance requirements.
    #[serde(default)]
    pub provenance_requirements: Vec<String>,
    /// Source validation requirements.
    #[serde(default)]
    pub validation_requirements: Vec<String>,
    /// Optional expected missing reason.
    #[serde(default)]
    pub expected_missing_reason: Option<String>,
    /// Optional retrieval date for manual/curated sources.
    #[serde(default)]
    pub retrieved_at: Option<String>,
    /// Optional reviewer id for manual/curated sources.
    #[serde(default)]
    pub reviewed_by: Option<String>,
    /// Optional review date for manual/curated sources.
    #[serde(default)]
    pub reviewed_at: Option<String>,
    /// Optional next review due date for manual/curated sources.
    #[serde(default)]
    pub manual_review_due_at: Option<String>,
    /// Optional replacement candidate for placeholders or coverage gaps.
    #[serde(default)]
    pub replacement_candidate: Option<String>,
    /// Primary audit policy.
    pub audit_policy: AuditPolicy,
    /// Additional audit policies for the same dataflow.
    #[serde(default)]
    pub additional_audit_policies: Vec<AdditionalAuditPolicy>,
}

/// Source status in the governance register.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceStatus {
    /// Active machine-readable or parseable source.
    Active,
    /// Manual or curated source is pending review.
    ManualPending,
    /// Visible context source excluded from scoring.
    VisibleUnscored,
    /// Source scope intentionally does not cover the needed input.
    CoverageGap,
    /// Licensed feed represented by public product/license pages.
    LicensedFeed,
    /// Placeholder source awaiting replacement.
    Placeholder,
    /// Retired source retained for audit history.
    Retired,
}

/// Owner area for a source-register entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerArea {
    /// Source adapter.
    Adapter,
    /// Scorecard input.
    Scorecard,
    /// Curated/manual source.
    Curated,
    /// Licensed feed.
    Licensed,
    /// Experimental source.
    Experimental,
}

/// Source-location audit policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuditPolicy {
    /// Page body must contain one of the configured hints.
    ContainsAny {
        /// Expected semantic hints.
        needles: Vec<String>,
        /// Recommended action if missing.
        recommendation: String,
    },
    /// Directory body must contain all required filename patterns.
    DirectoryListing {
        /// Required filename patterns.
        required_patterns: Vec<String>,
        /// Recommended action if missing.
        recommendation: String,
    },
    /// Budget index must not expose a newer budget year than configured.
    BudgetYear {
        /// Configured budget year.
        configured_year: String,
        /// Latest expected budget year.
        latest_year: String,
        /// Recommended action if stale.
        recommendation: String,
    },
    /// Public product page for licensed feed should be reachable.
    LicensedProduct {
        /// Recommended action if unreachable.
        recommendation: String,
    },
    /// World Bank B-READY Australia API semantic check.
    WorldBankBreadyApi {
        /// Recommended action when values are unresolved.
        recommendation: String,
    },
    /// Manual placeholder source that intentionally always needs review.
    ManualPlaceholder {
        /// Reason this cannot pass automatically.
        reason: String,
        /// Recommended replacement action.
        recommendation: String,
    },
    /// Manual register-only source with due-date validation but no live URL audit.
    ManualRegisterOnly {
        /// Reason this is manually reviewed.
        reason: String,
        /// Recommended action.
        recommendation: String,
    },
    /// Official source protected by bot filtering or access challenge.
    BotFiltered {
        /// Expected HTTP status codes for blocked bot access.
        expected_statuses: Vec<u16>,
        /// Optional machine-readable fallback description.
        #[serde(default)]
        semantic_fallback: Option<String>,
        /// Recommended action.
        recommendation: String,
    },
}

impl AuditPolicy {
    /// True when this policy should emit a source-location audit rule.
    #[must_use]
    pub const fn emits_source_location_rule(&self) -> bool {
        true
    }
}

/// Additional source-location audit policy for a dataflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdditionalAuditPolicy {
    /// URL checked by this additional policy.
    pub url: String,
    /// Audit policy for the additional URL.
    #[serde(flatten)]
    pub policy: AuditPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_source_register_loads() {
        let register = load_source_register().expect("load checked-in register");

        assert_eq!(register.version, "source-register.v1");
        assert!(
            register
                .dataflows
                .iter()
                .any(|dataflow| dataflow.dataflow_id == "abs.cpi")
        );
        assert!(
            register
                .dataflows
                .iter()
                .any(|dataflow| dataflow.dataflow_id == "curated.oversight_strength")
        );
    }

    #[test]
    fn duplicate_dataflow_ids_are_rejected() {
        let raw = r#"
version = "source-register.v1"

[[dataflows]]
source_id = "abs"
dataflow_id = "abs.cpi"
status = "active"
owner_area = "adapter"
canonical_url = "https://example.test/a"
license = "CC-BY-4.0"
attribution = "Source: ABS"
cadence = "quarterly"
review_frequency = "weekly"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]

[dataflows.audit_policy]
kind = "contains_any"
needles = ["CPI"]
recommendation = "Review."

[[dataflows]]
source_id = "abs"
dataflow_id = "abs.cpi"
status = "active"
owner_area = "adapter"
canonical_url = "https://example.test/b"
license = "CC-BY-4.0"
attribution = "Source: ABS"
cadence = "quarterly"
review_frequency = "weekly"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]

[dataflows.audit_policy]
kind = "contains_any"
needles = ["CPI"]
recommendation = "Review."
"#;

        let err = parse_source_register(raw).expect_err("duplicate id should fail");
        assert!(err.to_string().contains("duplicate dataflow id `abs.cpi`"));
    }

    #[test]
    fn manual_entries_require_review_due_date() {
        let raw = r#"
version = "source-register.v1"

[[dataflows]]
source_id = "curated"
dataflow_id = "curated.oversight_strength"
status = "manual_pending"
owner_area = "curated"
canonical_url = "https://www.oaic.gov.au/"
license = "Manual review required"
attribution = "Curated input"
cadence = "annual"
review_frequency = "annual"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]
retrieved_at = "2026-06-22"
reviewed_by = "aps-curation"
reviewed_at = "2026-06-22"

[dataflows.audit_policy]
kind = "manual_register_only"
reason = "Manual review"
recommendation = "Review source."
"#;

        let err = parse_source_register(raw).expect_err("missing due date should fail");
        assert!(err.to_string().contains("manual_review_due_at"));
    }

    #[test]
    fn malformed_register_urls_are_rejected() {
        let raw = valid_register_fixture().replace(
            "canonical_url = \"https://example.test/a\"",
            "canonical_url = \"not a url\"",
        );

        let err = parse_source_register(&raw).expect_err("bad URL should fail");
        assert!(err.to_string().contains("canonical_url"));
    }

    #[test]
    fn malformed_register_dates_are_rejected() {
        let raw = valid_register_fixture().replace(
            "manual_review_due_at = \"2027-06-22\"",
            "manual_review_due_at = \"2027/06/22\"",
        );

        let err = parse_source_register(&raw).expect_err("bad date should fail");
        assert!(err.to_string().contains("manual_review_due_at"));
    }

    #[test]
    fn missing_requirement_lists_are_rejected() {
        let raw = valid_register_fixture()
            .lines()
            .filter(|line| {
                !line.starts_with("provenance_requirements =")
                    && !line.starts_with("validation_requirements =")
            })
            .collect::<Vec<_>>()
            .join("\n");

        let err = parse_source_register(&raw).expect_err("missing requirements should fail");
        assert!(err.to_string().contains("provenance_requirements"));
    }

    #[test]
    fn manual_register_only_requires_review_dates_for_all_statuses() {
        let raw = valid_register_fixture()
            .replace("status = \"manual_pending\"", "status = \"placeholder\"")
            .lines()
            .filter(|line| {
                !line.starts_with("retrieved_at =")
                    && !line.starts_with("reviewed_at =")
                    && !line.starts_with("manual_review_due_at =")
            })
            .collect::<Vec<_>>()
            .join("\n");

        let err = parse_source_register(&raw).expect_err("missing dates should fail");
        assert!(err.to_string().contains("retrieved_at"));
    }

    fn valid_register_fixture() -> String {
        r#"
version = "source-register.v1"

[[dataflows]]
source_id = "curated"
dataflow_id = "curated.oversight_strength"
status = "manual_pending"
owner_area = "curated"
canonical_url = "https://example.test/a"
license = "Manual review required"
attribution = "Curated input"
cadence = "annual"
review_frequency = "annual"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]
retrieved_at = "2026-06-22"
reviewed_by = "aps-curation"
reviewed_at = "2026-06-22"
manual_review_due_at = "2027-06-22"

[dataflows.audit_policy]
kind = "manual_register_only"
reason = "Manual review"
recommendation = "Review source."
"#
        .to_string()
    }
}
