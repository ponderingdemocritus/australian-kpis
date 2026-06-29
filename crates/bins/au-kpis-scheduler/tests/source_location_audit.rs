use au_kpis_scheduler::source_location_audit::{
    SourceAuditSeverity, SourceAuditStatus, SourceLocationCheck, SourceLocationRule,
    SourceUrlSnapshot, evaluate_source_location_snapshots,
};
use chrono::{TimeZone, Utc};

fn generated_at() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 29, 6, 0, 0)
        .single()
        .expect("valid timestamp")
}

#[test]
fn source_location_asx_legacy_terms_url_maps_to_current_terms_location() {
    let rules = [SourceLocationRule::new(
        "asx",
        "asx.market_statistics",
        "https://www.asx.com.au/terms-of-use",
        SourceLocationCheck::CanonicalUrl {
            expected_url: "https://www.asx.com.au/legals/terms-of-use",
            recommendation: "Update ASX license attribution to https://www.asx.com.au/legals/terms-of-use.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.asx.com.au/terms-of-use",
        "https://www.asx.com.au/terms-of-use",
        404,
        "not found",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Drift);
    assert_eq!(report.findings.len(), 1);
    let finding = &report.findings[0];
    assert_eq!(finding.source_id, "asx");
    assert_eq!(finding.dataflow_id, "asx.market_statistics");
    assert_eq!(
        finding.latest_url.as_deref(),
        Some("https://www.asx.com.au/legals/terms-of-use")
    );
    assert!(finding.recommendation.contains("/legals/terms-of-use"));
}

#[test]
fn source_location_nsw_budget_rule_flags_configured_older_budget_year() {
    let rules = [SourceLocationRule::new(
        "state-budgets",
        "state_budgets.nsw_budget",
        "https://www.nsw.gov.au/business-and-economy/nsw-budget/2025-26-budget-papers",
        SourceLocationCheck::BudgetYear {
            configured_year: "2025-26",
            latest_year: "2026-27",
            recommendation: "Review and update the NSW budget source to the current 2026-27 budget papers.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.nsw.gov.au/business-and-economy/nsw-budget/2025-26-budget-papers",
        "https://www.nsw.gov.au/business-and-economy/nsw-budget",
        200,
        r#"<a href="/business-and-economy/nsw-budget/2026-27-budget-papers">2026-27 Budget Paper No. 1 Budget Statement</a>"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Drift);
    let finding = report
        .findings
        .iter()
        .find(|finding| finding.dataflow_id == "state_budgets.nsw_budget")
        .expect("nsw drift finding");
    assert_eq!(finding.severity, SourceAuditSeverity::Warning);
    assert_eq!(
        finding.latest_url.as_deref(),
        Some("https://www.nsw.gov.au/business-and-economy/nsw-budget/2026-27-budget-papers")
    );
    assert!(finding.evidence.contains("2026-27"));
}

#[test]
fn source_location_world_bank_bready_null_australia_values_are_unresolved_manual_review() {
    let rules = [SourceLocationRule::new(
        "worldbank",
        "worldbank.bready",
        "https://api.worldbank.org/v2/country/AUS/indicator/BREADY?format=json",
        SourceLocationCheck::WorldBankBreadyApi {
            recommendation: "Review World Bank B-READY Australia availability before scoring this source.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://api.worldbank.org/v2/country/AUS/indicator/BREADY?format=json",
        "https://api.worldbank.org/v2/country/AUS/indicator/BREADY?format=json",
        200,
        r#"[{"page":1,"pages":1},[{"countryiso3code":"AUS","date":"2025","value":null},{"countryiso3code":"AUS","date":"2024","value":null}]]"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::ManualReview);
    assert_eq!(report.findings.len(), 1);
    assert_eq!(
        report.findings[0].severity,
        SourceAuditSeverity::ManualReview
    );
    assert!(report.findings[0].evidence.contains("null Australia"));
}

#[test]
fn source_location_aemo_directory_with_expected_zip_patterns_passes() {
    let rules = [SourceLocationRule::new(
        "aemo",
        "aemo.dispatch",
        "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
        SourceLocationCheck::DirectoryListing {
            required_patterns: &["PUBLIC_DISPATCHIS_", ".zip"],
            recommendation: "Review AEMO NEMWeb DispatchIS directory if current ZIP reports disappear.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
        "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
        200,
        r#"<a href="PUBLIC_DISPATCHIS_202606290000_0000000000000000.zip">PUBLIC_DISPATCHIS_202606290000_0000000000000000.zip</a>"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Ok);
    assert!(report.findings.is_empty());
}

#[test]
fn source_location_aemo_next_day_actual_gen_directory_passes() {
    let rules = [SourceLocationRule::new(
        "aemo",
        "aemo.generation_mix",
        "https://nemweb.com.au/Reports/Current/Next_Day_Actual_Gen/",
        SourceLocationCheck::DirectoryListing {
            required_patterns: &["PUBLIC_NEXT_DAY_ACTUAL_GEN_", ".zip"],
            recommendation: "Review AEMO NEMWeb Next Day Actual Gen directory if current ZIP reports disappear.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://nemweb.com.au/Reports/Current/Next_Day_Actual_Gen/",
        "https://nemweb.com.au/Reports/Current/Next_Day_Actual_Gen/",
        200,
        r#"<a href="PUBLIC_NEXT_DAY_ACTUAL_GEN_20260629_0000000000000000.zip">PUBLIC_NEXT_DAY_ACTUAL_GEN_20260629_0000000000000000.zip</a>"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Ok);
    assert!(report.findings.is_empty());
}

#[test]
fn source_location_federal_budget_2026_27_bp4_is_current() {
    let rules = [SourceLocationRule::new(
        "treasury",
        "treasury.budget_papers",
        "https://budget.gov.au/content/bp4/index.htm",
        SourceLocationCheck::BudgetYear {
            configured_year: "2026-27",
            latest_year: "2026-27",
            recommendation: "Review the Australian Government Budget Paper No. 4 page when a newer federal budget appears.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://budget.gov.au/content/bp4/index.htm",
        "https://budget.gov.au/content/bp4/index.htm",
        200,
        r#"<title>Budget Paper No. 4: Agency Resourcing | Budget 2026-27</title>"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Ok);
    assert!(report.findings.is_empty());
}

#[test]
fn source_location_manual_placeholder_source_always_creates_review_finding() {
    let rules = [SourceLocationRule::new(
        "compute",
        "compute.au_datacentre_capacity_mw",
        "https://example.test/compute-capacity",
        SourceLocationCheck::ManualPlaceholder {
            reason: "example.test is a placeholder source.",
            recommendation: "Replace compute.au_datacentre_capacity_mw with a reviewed primary source.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://example.test/compute-capacity",
        "https://example.test/compute-capacity",
        0,
        "",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::ManualReview);
    assert_eq!(report.findings.len(), 1);
    assert_eq!(
        report.findings[0].dataflow_id,
        "compute.au_datacentre_capacity_mw"
    );
}

#[test]
fn source_location_report_markdown_and_json_expose_stable_fields() {
    let rules = [SourceLocationRule::new(
        "compute",
        "compute.au_datacentre_capacity_mw",
        "https://example.test/compute-capacity",
        SourceLocationCheck::ManualPlaceholder {
            reason: "example.test is a placeholder source.",
            recommendation: "Replace compute.au_datacentre_capacity_mw with a reviewed primary source.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://example.test/compute-capacity",
        "https://example.test/compute-capacity",
        0,
        "",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());
    let markdown = report.render_markdown();
    let json = serde_json::to_value(&report).expect("serialize report");

    assert!(markdown.contains("# Source Location Audit Report"));
    assert!(markdown.contains("compute.au_datacentre_capacity_mw"));
    assert_eq!(json["status"], "manual_review");
    assert_eq!(json["findings_total"], 1);
    assert!(json["findings"][0]["recommendation"].is_string());
}
