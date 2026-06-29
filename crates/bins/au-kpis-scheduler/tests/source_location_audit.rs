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
fn source_location_world_bank_bready_invalid_json_is_tool_error() {
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
        "not json",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Error);
    assert_eq!(report.findings[0].severity, SourceAuditSeverity::Error);
    assert!(report.findings[0].evidence.contains("not valid JSON"));
}

#[test]
fn source_location_world_bank_bready_non_null_australia_values_pass() {
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
        r#"[{"page":1,"pages":1},[{"countryiso3code":"AUS","date":"2025","value":77.5},{"countryiso3code":"NZL","date":"2025","value":null}]]"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Ok);
    assert!(report.findings.is_empty());
}

#[test]
fn source_location_world_bank_bready_ignores_older_null_australia_values() {
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
        r#"[{"page":1,"pages":1},[{"countryiso3code":"AUS","date":"2024","value":null},{"countryiso3code":"AUS","date":"2025","value":77.5}]]"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Ok);
    assert!(report.findings.is_empty());
    assert!(report.results[0].evidence.contains("latest period 2025"));
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
fn source_location_directory_missing_pattern_flags_drift() {
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
        "<html>empty directory</html>",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Drift);
    assert_eq!(report.findings[0].severity, SourceAuditSeverity::Warning);
    assert!(
        report.findings[0]
            .evidence
            .contains("missing expected patterns")
    );
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
fn source_location_current_budget_rule_flags_newer_budget_year() {
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
        r#"<a href="/content/bp4/index.htm">Budget Paper No. 4 2027-28</a>"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Drift);
    assert_eq!(report.findings[0].severity, SourceAuditSeverity::Warning);
    assert!(report.findings[0].evidence.contains("2027-28"));
}

#[test]
fn source_location_budget_year_unconfirmed_latest_is_manual_review() {
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
        "https://www.nsw.gov.au/business-and-economy/nsw-budget/2025-26-budget-papers",
        200,
        "<html>Budget papers archive</html>",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::ManualReview);
    assert_eq!(
        report.findings[0].severity,
        SourceAuditSeverity::ManualReview
    );
    assert!(report.findings[0].evidence.contains("Could not confirm"));
}

#[test]
fn source_location_reachable_soft_access_is_manual_review() {
    let rules = [SourceLocationRule::new(
        "rba",
        "rba.statistical_tables",
        "https://www.rba.gov.au/statistics/tables/",
        SourceLocationCheck::Reachable {
            recommendation: "Review the RBA statistical tables index and table URLs.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.rba.gov.au/statistics/tables/",
        "https://www.rba.gov.au/statistics/tables/",
        403,
        "forbidden",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::ManualReview);
    assert_eq!(
        report.findings[0].severity,
        SourceAuditSeverity::ManualReview
    );
    assert!(report.findings[0].evidence.contains("bot-filtered"));
}

#[test]
fn source_location_reachable_hard_failure_is_drift() {
    let rules = [SourceLocationRule::new(
        "home-affairs",
        "home_affairs.skillselect_talent_proxy",
        "https://immi.homeaffairs.gov.au/visas/working-in-australia/skillselect/invitation-rounds",
        SourceLocationCheck::Reachable {
            recommendation: "Review the Home Affairs SkillSelect invitation-rounds source link.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://immi.homeaffairs.gov.au/visas/working-in-australia/skillselect/invitation-rounds",
        "https://immi.homeaffairs.gov.au/visas/working-in-australia/skillselect/invitation-rounds",
        404,
        "not found",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Drift);
    assert_eq!(report.findings[0].severity, SourceAuditSeverity::Warning);
    assert!(report.findings[0].evidence.contains("HTTP 404"));
}

#[test]
fn source_location_contains_any_without_expected_hint_is_manual_review() {
    let rules = [SourceLocationRule::new(
        "naic",
        "naic.ai_adoption_tracker",
        "https://www.ai.gov.au/news-and-insights/reports/ai-adoption-tracker",
        SourceLocationCheck::ContainsAny {
            needles: &["AI adoption", "tracker"],
            recommendation: "Review the NAIC/industry AI adoption tracker source page.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.ai.gov.au/news-and-insights/reports/ai-adoption-tracker",
        "https://www.ai.gov.au/news-and-insights/reports/ai-adoption-tracker",
        200,
        "<html>Reports</html>",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::ManualReview);
    assert!(report.findings[0].evidence.contains("hints were absent"));
}

#[test]
fn source_location_missing_snapshot_is_error() {
    let rules = [SourceLocationRule::new(
        "asx",
        "asx.market_statistics",
        "https://www.asx.com.au/legals/terms-of-use",
        SourceLocationCheck::ContainsAny {
            needles: &["Terms of Use", "ASX"],
            recommendation: "Review the ASX Terms of Use location used for license attribution.",
        },
    )];

    let report = evaluate_source_location_snapshots(&rules, &[], generated_at());

    assert_eq!(report.status, SourceAuditStatus::Error);
    assert_eq!(report.findings[0].severity, SourceAuditSeverity::Error);
    assert_eq!(report.results[0].http_status, None);
}

#[test]
fn source_location_canonical_url_current_location_passes() {
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
        "https://www.asx.com.au/legals/terms-of-use",
        200,
        "ASX Terms of Use",
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

#[test]
fn source_location_passing_report_markdown_says_no_findings() {
    let rules = [SourceLocationRule::new(
        "aemo",
        "aemo.dispatch",
        "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
        SourceLocationCheck::DirectoryListing {
            required_patterns: &["PUBLIC_DISPATCHIS_", ".zip"],
            recommendation: "Review the AEMO NEMWeb DispatchIS directory if current ZIP reports disappear.",
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
        "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
        200,
        r#"<a href="PUBLIC_DISPATCHIS_202606290000_0000000000000000.zip">PUBLIC_DISPATCHIS_202606290000_0000000000000000.zip</a>"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());
    let markdown = report.render_markdown();

    assert_eq!(report.status, SourceAuditStatus::Ok);
    assert!(markdown.contains("No source-location findings detected."));
}
