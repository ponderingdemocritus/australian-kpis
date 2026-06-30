use au_kpis_scheduler::source_location_audit::{
    SourceAuditSeverity, SourceAuditStatus, SourceLocationCheck, SourceLocationRule,
    SourceUrlSnapshot, default_source_location_rules, evaluate_source_location_snapshots,
};
use chrono::{TimeZone, Utc};

fn generated_at() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 29, 6, 0, 0)
        .single()
        .expect("valid timestamp")
}

fn s(value: &str) -> String {
    value.to_string()
}

fn sv(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

#[test]
fn source_location_asx_legacy_terms_url_maps_to_current_terms_location() {
    let rules = [SourceLocationRule::new(
        "asx",
        "asx.market_statistics",
        "https://www.asx.com.au/terms-of-use",
        SourceLocationCheck::CanonicalUrl {
            expected_url: s("https://www.asx.com.au/legals/terms-of-use"),
            recommendation: s(
                "Update ASX license attribution to https://www.asx.com.au/legals/terms-of-use.",
            ),
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
            configured_year: s("2025-26"),
            latest_year: s("2026-27"),
            recommendation: s(
                "Review and update the NSW budget source to the current 2026-27 budget papers.",
            ),
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
            recommendation: s(
                "Review World Bank B-READY Australia availability before scoring this source.",
            ),
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
            recommendation: s(
                "Review World Bank B-READY Australia availability before scoring this source.",
            ),
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
            recommendation: s(
                "Review World Bank B-READY Australia availability before scoring this source.",
            ),
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
            recommendation: s(
                "Review World Bank B-READY Australia availability before scoring this source.",
            ),
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
            required_patterns: sv(&["PUBLIC_DISPATCHIS_", ".zip"]),
            recommendation: s(
                "Review AEMO NEMWeb DispatchIS directory if current ZIP reports disappear.",
            ),
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
            required_patterns: sv(&["PUBLIC_NEXT_DAY_ACTUAL_GEN_", ".zip"]),
            recommendation: s(
                "Review AEMO NEMWeb Next Day Actual Gen directory if current ZIP reports disappear.",
            ),
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
            required_patterns: sv(&["PUBLIC_NEXT_DAY_ACTUAL_GEN_", ".zip"]),
            recommendation: s(
                "Review AEMO NEMWeb Next Day Actual Gen directory if current ZIP reports disappear.",
            ),
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
            configured_year: s("2026-27"),
            latest_year: s("2026-27"),
            recommendation: s(
                "Review the Australian Government Budget Paper No. 4 page when a newer federal budget appears.",
            ),
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
            configured_year: s("2026-27"),
            latest_year: s("2026-27"),
            recommendation: s(
                "Review the Australian Government Budget Paper No. 4 page when a newer federal budget appears.",
            ),
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
            configured_year: s("2025-26"),
            latest_year: s("2026-27"),
            recommendation: s(
                "Review and update the NSW budget source to the current 2026-27 budget papers.",
            ),
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
fn source_location_budget_year_ignores_iso_dates_when_current_year_is_present() {
    let rules = [SourceLocationRule::new(
        "treasury",
        "treasury.budget_papers",
        "https://budget.gov.au/content/bp4/index.htm",
        SourceLocationCheck::BudgetYear {
            configured_year: s("2026-27"),
            latest_year: s("2026-27"),
            recommendation: s(
                "Review the Australian Government Budget Paper No. 4 page when a newer federal budget appears.",
            ),
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://budget.gov.au/content/bp4/index.htm",
        "https://budget.gov.au/content/bp4/index.htm",
        200,
        r#"<time datetime="2027-06-15">15 June 2027</time><title>Budget 2026-27</title>"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Ok);
    assert!(report.findings.is_empty());
}

#[test]
fn source_location_budget_year_ignores_forward_estimate_years_when_current_year_is_present() {
    let rules = [SourceLocationRule::new(
        "treasury",
        "treasury.budget_papers",
        "https://budget.gov.au/content/bp4/index.htm",
        SourceLocationCheck::BudgetYear {
            configured_year: s("2026-27"),
            latest_year: s("2026-27"),
            recommendation: s(
                "Review the Australian Government Budget Paper No. 4 page when a newer federal budget appears.",
            ),
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://budget.gov.au/content/bp4/index.htm",
        "https://budget.gov.au/content/bp4/index.htm",
        200,
        r#"<title>Budget 2026-27</title><p>Forward estimates include 2027-28 and 2028-29.</p>"#,
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Ok);
    assert!(report.findings.is_empty());
}

#[test]
fn source_location_reachable_soft_access_is_manual_review() {
    let rules = [SourceLocationRule::new(
        "rba",
        "rba.statistical_tables",
        "https://www.rba.gov.au/statistics/tables/",
        SourceLocationCheck::Reachable {
            recommendation: s("Review the RBA statistical tables index and table URLs."),
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
fn source_location_bot_filtered_expected_status_is_not_drift() {
    let rules = [SourceLocationRule::new(
        "rba",
        "rba.statistical_tables",
        "https://www.rba.gov.au/statistics/tables/",
        SourceLocationCheck::BotFiltered {
            expected_statuses: vec![403],
            semantic_fallback: Some(s("Statistical Tables")),
            recommendation: s(
                "Use reviewed direct CSV/XLS table artifacts if the RBA index is bot-filtered.",
            ),
        },
    )
    .with_register_metadata("active", "bot_filtered")];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.rba.gov.au/statistics/tables/",
        "https://www.rba.gov.au/statistics/tables/",
        403,
        "forbidden",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::BotFiltered);
    assert_eq!(
        report.findings[0].severity,
        SourceAuditSeverity::BotFiltered
    );
    assert_eq!(report.results[0].source_status.as_deref(), Some("active"));
    assert_eq!(
        report.results[0].audit_policy_kind.as_deref(),
        Some("bot_filtered")
    );
    assert!(report.findings[0].evidence.contains("access-challenged"));
}

#[test]
fn source_location_bot_filtered_expected_rate_limit_status_is_not_drift() {
    let rules = [SourceLocationRule::new(
        "rba",
        "rba.statistical_tables",
        "https://www.rba.gov.au/statistics/tables/",
        SourceLocationCheck::BotFiltered {
            expected_statuses: vec![403, 429],
            semantic_fallback: Some(s("Statistical Tables")),
            recommendation: s(
                "Use reviewed direct CSV/XLS table artifacts if the RBA index is bot-filtered.",
            ),
        },
    )
    .with_register_metadata("active", "bot_filtered")];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.rba.gov.au/statistics/tables/",
        "https://www.rba.gov.au/statistics/tables/",
        429,
        "rate limited",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::BotFiltered);
    assert_eq!(
        report.findings[0].severity,
        SourceAuditSeverity::BotFiltered
    );
    assert!(report.findings[0].evidence.contains("access-challenged"));
}

#[test]
fn source_location_bot_filtered_success_without_semantic_hint_needs_review() {
    let rules = [SourceLocationRule::new(
        "rba",
        "rba.statistical_tables",
        "https://www.rba.gov.au/statistics/tables/",
        SourceLocationCheck::BotFiltered {
            expected_statuses: vec![403],
            semantic_fallback: Some(s("Statistical Tables")),
            recommendation: s(
                "Use reviewed direct CSV/XLS table artifacts if the RBA index is bot-filtered.",
            ),
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.rba.gov.au/statistics/tables/",
        "https://www.rba.gov.au/statistics/tables/",
        200,
        "Access challenge",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::ManualReview);
    assert_eq!(
        report.findings[0].severity,
        SourceAuditSeverity::ManualReview
    );
    assert!(
        report.findings[0]
            .evidence
            .contains("did not contain semantic fallback")
    );
}

#[test]
fn source_location_bot_filtered_success_without_semantic_fallback_needs_review() {
    let rules = [SourceLocationRule::new(
        "rba",
        "rba.statistical_tables",
        "https://www.rba.gov.au/statistics/tables/",
        SourceLocationCheck::BotFiltered {
            expected_statuses: vec![403],
            semantic_fallback: None,
            recommendation: s(
                "Use reviewed direct CSV/XLS table artifacts if the RBA index is bot-filtered.",
            ),
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.rba.gov.au/statistics/tables/",
        "https://www.rba.gov.au/statistics/tables/",
        200,
        "Access challenge",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::ManualReview);
    assert_eq!(
        report.findings[0].severity,
        SourceAuditSeverity::ManualReview
    );
    assert!(
        report.findings[0]
            .evidence
            .contains("without a semantic fallback")
    );
}

#[test]
fn source_location_bot_filtered_unexpected_status_is_drift() {
    let rules = [SourceLocationRule::new(
        "state-planning",
        "state_planning.vic_permit_activity",
        "https://www.planning.vic.gov.au/guides-and-resources/data-insights-and-analytics/planning-permit-activity-in-victoria",
        SourceLocationCheck::BotFiltered {
            expected_statuses: vec![403],
            semantic_fallback: Some(s("Planning permit activity")),
            recommendation: s("Review Victoria Planning permit activity source links."),
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.planning.vic.gov.au/guides-and-resources/data-insights-and-analytics/planning-permit-activity-in-victoria",
        "https://www.planning.vic.gov.au/guides-and-resources/data-insights-and-analytics/planning-permit-activity-in-victoria",
        404,
        "not found",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Drift);
    assert_eq!(report.findings[0].severity, SourceAuditSeverity::Warning);
    assert!(report.findings[0].evidence.contains("outside expected"));
}

#[test]
fn source_location_bot_filtered_unlisted_soft_access_status_is_drift() {
    let rules = [SourceLocationRule::new(
        "rba",
        "rba.statistical_tables",
        "https://www.rba.gov.au/statistics/tables/",
        SourceLocationCheck::BotFiltered {
            expected_statuses: vec![403],
            semantic_fallback: Some(s("Statistical Tables")),
            recommendation: s(
                "Use reviewed direct CSV/XLS table artifacts if the RBA index is bot-filtered.",
            ),
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.rba.gov.au/statistics/tables/",
        "https://www.rba.gov.au/statistics/tables/",
        429,
        "rate limited",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Drift);
    assert_eq!(report.findings[0].severity, SourceAuditSeverity::Warning);
    assert!(report.findings[0].evidence.contains("outside expected"));
}

#[test]
fn source_location_request_failure_is_tool_error() {
    let rules = [SourceLocationRule::new(
        "rba",
        "rba.statistical_tables",
        "https://www.rba.gov.au/statistics/tables/",
        SourceLocationCheck::Reachable {
            recommendation: s("Review the RBA statistical tables index and table URLs."),
        },
    )];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.rba.gov.au/statistics/tables/",
        "https://www.rba.gov.au/statistics/tables/",
        0,
        "request error: dns error",
    )];

    let report = evaluate_source_location_snapshots(&rules, &snapshots, generated_at());

    assert_eq!(report.status, SourceAuditStatus::Error);
    assert_eq!(report.findings[0].severity, SourceAuditSeverity::Error);
    assert!(report.findings[0].evidence.contains("request failed"));
}

#[test]
fn source_location_reachable_hard_failure_is_drift() {
    let rules = [SourceLocationRule::new(
        "home-affairs",
        "home_affairs.skillselect_talent_proxy",
        "https://immi.homeaffairs.gov.au/visas/working-in-australia/skillselect/invitation-rounds",
        SourceLocationCheck::Reachable {
            recommendation: s("Review the Home Affairs SkillSelect invitation-rounds source link."),
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
            needles: sv(&["AI adoption", "tracker"]),
            recommendation: s("Review the NAIC/industry AI adoption tracker source page."),
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
fn source_location_overdue_manual_entry_with_live_policy_requires_review() {
    let rules = [SourceLocationRule::new(
        "apra",
        "apra.super_asset_allocation",
        "https://www.apra.gov.au/superannuation-statistics",
        SourceLocationCheck::ContainsAny {
            needles: sv(&["Superannuation statistics"]),
            recommendation: s("Review APRA superannuation asset allocation source evidence."),
        },
    )
    .with_register_metadata("manual_pending", "contains_any")
    .with_manual_review_metadata("2026-01-01", "2026-05-01")];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.apra.gov.au/superannuation-statistics",
        "https://www.apra.gov.au/superannuation-statistics",
        200,
        "Superannuation statistics",
    )];

    let report = evaluate_source_location_snapshots(
        &rules,
        &snapshots,
        Utc.with_ymd_and_hms(2026, 6, 30, 0, 0, 0).unwrap(),
    );

    assert_eq!(report.status, SourceAuditStatus::ManualReview);
    assert_eq!(
        report.findings[0].severity,
        SourceAuditSeverity::ManualReview
    );
    assert!(
        report.findings[0]
            .evidence
            .contains("manual register review was due")
    );
}

#[test]
fn source_location_overdue_manual_entry_preserves_drift_and_review_due_evidence() {
    let rules = [SourceLocationRule::new(
        "apra",
        "apra.super_asset_allocation",
        "https://www.apra.gov.au/superannuation-statistics",
        SourceLocationCheck::Reachable {
            recommendation: s("Review APRA superannuation asset allocation source evidence."),
        },
    )
    .with_register_metadata("manual_pending", "contains_any")
    .with_manual_review_metadata("2026-01-01", "2026-05-01")];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.apra.gov.au/superannuation-statistics",
        "https://www.apra.gov.au/superannuation-statistics",
        404,
        "not found",
    )];

    let report = evaluate_source_location_snapshots(
        &rules,
        &snapshots,
        Utc.with_ymd_and_hms(2026, 6, 30, 0, 0, 0).unwrap(),
    );

    assert_eq!(report.status, SourceAuditStatus::Drift);
    assert_eq!(report.findings[0].severity, SourceAuditSeverity::Warning);
    assert!(report.findings[0].evidence.contains("HTTP 404"));
    assert!(
        report.findings[0]
            .evidence
            .contains("Manual register review was due")
    );
}

#[test]
fn source_location_current_manual_entry_with_live_policy_passes() {
    let rules = [SourceLocationRule::new(
        "apra",
        "apra.super_asset_allocation",
        "https://www.apra.gov.au/superannuation-statistics",
        SourceLocationCheck::ContainsAny {
            needles: sv(&["Superannuation statistics"]),
            recommendation: s("Review APRA superannuation asset allocation source evidence."),
        },
    )
    .with_register_metadata("manual_pending", "contains_any")
    .with_manual_review_metadata("2026-01-01", "2026-12-01")];
    let snapshots = [SourceUrlSnapshot::new(
        "https://www.apra.gov.au/superannuation-statistics",
        "https://www.apra.gov.au/superannuation-statistics",
        200,
        "Superannuation statistics",
    )];

    let report = evaluate_source_location_snapshots(
        &rules,
        &snapshots,
        Utc.with_ymd_and_hms(2026, 6, 30, 0, 0, 0).unwrap(),
    );

    assert_eq!(report.status, SourceAuditStatus::Ok);
    assert!(report.findings.is_empty());
    assert!(
        report.results[0]
            .evidence
            .contains("Observed expected source hint")
    );
}

#[test]
fn source_location_missing_snapshot_is_error() {
    let rules = [SourceLocationRule::new(
        "asx",
        "asx.market_statistics",
        "https://www.asx.com.au/legals/terms-of-use",
        SourceLocationCheck::ContainsAny {
            needles: sv(&["Terms of Use", "ASX"]),
            recommendation: s("Review the ASX Terms of Use location used for license attribution."),
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
            expected_url: s("https://www.asx.com.au/legals/terms-of-use"),
            recommendation: s(
                "Update ASX license attribution to https://www.asx.com.au/legals/terms-of-use.",
            ),
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
            reason: s("example.test is a placeholder source."),
            recommendation: s(
                "Replace compute.au_datacentre_capacity_mw with a reviewed primary source.",
            ),
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
fn source_location_manual_register_only_current_review_passes_without_http() {
    let rules = [SourceLocationRule::new(
        "curated",
        "curated.oversight_strength",
        "https://www.anao.gov.au/work-program",
        SourceLocationCheck::ManualRegisterOnly {
            reason: s("Curated oversight-strength input is manually reviewed."),
            reviewed_at: s("2026-06-22"),
            manual_review_due_at: s("2026-12-22"),
            recommendation: s("Review oversight source taxonomy before scoring."),
        },
    )];

    let report = evaluate_source_location_snapshots(&rules, &[], generated_at());

    assert_eq!(report.status, SourceAuditStatus::Ok);
    assert!(report.findings.is_empty());
    assert_eq!(report.results[0].http_status, None);
    assert!(report.results[0].evidence.contains("next manual review"));
}

#[test]
fn source_location_manual_register_only_overdue_creates_review_finding() {
    let rules = [SourceLocationRule::new(
        "curated",
        "curated.oversight_strength",
        "https://www.anao.gov.au/work-program",
        SourceLocationCheck::ManualRegisterOnly {
            reason: s("Curated oversight-strength input is manually reviewed."),
            reviewed_at: s("2025-06-22"),
            manual_review_due_at: s("2026-01-01"),
            recommendation: s("Review oversight source taxonomy before scoring."),
        },
    )];

    let report = evaluate_source_location_snapshots(&rules, &[], generated_at());

    assert_eq!(report.status, SourceAuditStatus::ManualReview);
    assert_eq!(
        report.findings[0].severity,
        SourceAuditSeverity::ManualReview
    );
    assert!(
        report.findings[0]
            .evidence
            .contains("manual review was due")
    );
}

#[test]
fn source_location_report_markdown_and_json_expose_stable_fields() {
    let rules = [SourceLocationRule::new(
        "compute",
        "compute.au_datacentre_capacity_mw",
        "https://example.test/compute-capacity",
        SourceLocationCheck::ManualPlaceholder {
            reason: s("example.test is a placeholder source."),
            recommendation: s(
                "Replace compute.au_datacentre_capacity_mw with a reviewed primary source.",
            ),
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
    assert!(markdown.contains("Source register: `source-register.v1`"));
    assert!(markdown.contains("compute.au_datacentre_capacity_mw"));
    assert_eq!(json["register_version"], "source-register.v1");
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
            required_patterns: sv(&["PUBLIC_DISPATCHIS_", ".zip"]),
            recommendation: s(
                "Review the AEMO NEMWeb DispatchIS directory if current ZIP reports disappear.",
            ),
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

#[test]
fn source_location_default_rules_use_registered_source_ids() {
    const REGISTERED_SOURCE_IDS: &[&str] = &[
        "abs",
        "aemo",
        "ai-readiness",
        "apra",
        "asx",
        "compute",
        "curated",
        "nhsac",
        "pc",
        "rba",
        "state-budgets",
        "state-planning",
        "state_capital",
        "treasury",
        "worldbank",
    ];

    let rules = default_source_location_rules().expect("load default source-location rules");
    for rule in &rules {
        assert!(
            REGISTERED_SOURCE_IDS.contains(&rule.source_id.as_str()),
            "unregistered source id {} for {}",
            rule.source_id,
            rule.dataflow_id
        );
    }
}

#[test]
fn source_location_ai_rules_use_published_aps_provenance_urls() {
    let rules = default_source_location_rules().expect("load default source-location rules");
    let oxford = rules
        .iter()
        .find(|rule| rule.dataflow_id == "oxford.gari")
        .expect("oxford.gari rule");
    let abs_ai_rd = rules
        .iter()
        .find(|rule| rule.dataflow_id == "abs.ai_rd")
        .expect("abs.ai_rd rule");

    assert_eq!(
        oxford.current_url,
        "https://oxfordinsights.com/ai-readiness/government-ai-readiness-index-2025/"
    );
    assert_eq!(
        abs_ai_rd.current_url,
        "https://www.abs.gov.au/media-centre/media-releases/ai-now-fastest-growing-area-business-rd"
    );
}

#[test]
fn source_location_state_capital_rules_use_adapter_index_url() {
    const STATE_CAPITAL_INDEX_URL: &str =
        "https://www.audit.vic.gov.au/report/major-projects-performance-reporting-2025";

    let rules = default_source_location_rules().expect("load default source-location rules");
    let state_capital_rules = rules
        .iter()
        .filter(|rule| rule.source_id == "state_capital")
        .collect::<Vec<_>>();

    assert_eq!(state_capital_rules.len(), 2);
    for rule in state_capital_rules {
        assert_eq!(rule.current_url, STATE_CAPITAL_INDEX_URL);
    }
}

#[test]
fn source_location_nsw_budget_rule_uses_unversioned_landing_page() {
    let rules = default_source_location_rules().expect("load default source-location rules");
    let nsw_rule = rules
        .iter()
        .find(|rule| rule.dataflow_id == "state_budgets.nsw_budget")
        .expect("nsw budget rule");

    assert_eq!(
        nsw_rule.current_url,
        "https://www.nsw.gov.au/business-and-economy/nsw-budget"
    );
}
