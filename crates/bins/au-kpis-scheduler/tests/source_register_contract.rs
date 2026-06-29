use std::collections::BTreeSet;

use au_kpis_scheduler::source_location_audit::default_source_location_rules;
use au_kpis_scorecard::load_aps_v1_config;
use au_kpis_source_register::{AuditPolicy, SourceStatus, load_source_register};

#[test]
fn source_register_dataflow_ids_are_unique() {
    let register = load_source_register().expect("load source register");
    let mut seen = BTreeSet::new();

    for dataflow in &register.dataflows {
        assert!(
            seen.insert(dataflow.dataflow_id.as_str()),
            "duplicate source register dataflow id `{}`",
            dataflow.dataflow_id
        );
    }
}

#[test]
fn scheduler_default_rules_are_derived_from_source_register() {
    let register = load_source_register().expect("load source register");
    let registered_audited = register
        .dataflows
        .iter()
        .filter(|dataflow| dataflow.audit_policy.emits_source_location_rule())
        .map(|dataflow| dataflow.dataflow_id.as_str())
        .collect::<BTreeSet<_>>();
    let scheduler_rules = default_source_location_rules()
        .iter()
        .map(|rule| rule.dataflow_id)
        .collect::<BTreeSet<_>>();

    assert_eq!(
        registered_audited, scheduler_rules,
        "scheduler source-location rules must match register-backed audit policies"
    );
}

#[test]
fn aps_source_dataflows_are_all_registered() {
    let register = load_source_register().expect("load source register");
    let registered = register
        .dataflows
        .iter()
        .map(|dataflow| dataflow.dataflow_id.as_str())
        .collect::<BTreeSet<_>>();
    let aps = load_aps_v1_config().expect("load APS config");

    for indicator in aps.indicators {
        assert!(
            registered.contains(indicator.source_dataflow_id.as_str()),
            "APS indicator `{}` references unregistered source dataflow `{}`",
            indicator.indicator_id,
            indicator.source_dataflow_id
        );
    }
}

#[test]
fn manual_and_visible_unscored_register_entries_have_review_due_dates() {
    let register = load_source_register().expect("load source register");

    for dataflow in register.dataflows.iter().filter(|dataflow| {
        matches!(
            dataflow.status,
            SourceStatus::ManualPending | SourceStatus::VisibleUnscored
        )
    }) {
        assert!(
            dataflow.manual_review_due_at.is_some(),
            "`{}` is manual/visible-unscored and needs manual_review_due_at",
            dataflow.dataflow_id
        );
    }
}

#[test]
fn curated_aps_entries_are_explicit_manual_register_policies() {
    let register = load_source_register().expect("load source register");
    let curated = register
        .dataflows
        .iter()
        .filter(|dataflow| dataflow.dataflow_id.starts_with("curated."))
        .collect::<Vec<_>>();

    assert!(!curated.is_empty(), "expected curated APS source entries");
    for dataflow in curated {
        assert!(
            matches!(
                dataflow.audit_policy,
                AuditPolicy::ManualRegisterOnly { .. }
            ),
            "`{}` should be explicit manual_register_only policy",
            dataflow.dataflow_id
        );
    }
}
