use std::collections::BTreeSet;

use au_kpis_adapter::SourceAdapter;
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
    let mut registered_audited = BTreeSet::new();
    for dataflow in &register.dataflows {
        if dataflow.audit_policy.emits_source_location_rule() {
            registered_audited.insert((
                dataflow.source_id.as_str(),
                dataflow.dataflow_id.as_str(),
                dataflow.canonical_url.as_str(),
                audit_policy_kind(&dataflow.audit_policy),
            ));
        }
        for additional in &dataflow.additional_audit_policies {
            if additional.policy.emits_source_location_rule() {
                registered_audited.insert((
                    dataflow.source_id.as_str(),
                    dataflow.dataflow_id.as_str(),
                    additional.url.as_str(),
                    audit_policy_kind(&additional.policy),
                ));
            }
        }
    }
    let scheduler_rules = default_source_location_rules()
        .expect("load default source-location rules")
        .iter()
        .map(|rule| {
            (
                rule.source_id,
                rule.dataflow_id,
                rule.current_url,
                rule.audit_policy_kind
                    .expect("register-derived rule must expose audit policy kind"),
            )
        })
        .collect::<BTreeSet<_>>();

    assert_eq!(
        registered_audited, scheduler_rules,
        "scheduler source-location rules must match register-backed audit policies"
    );
}

#[test]
fn manual_placeholder_register_entries_use_placeholder_status() {
    let register = load_source_register().expect("load source register");

    for dataflow in register
        .dataflows
        .iter()
        .filter(|dataflow| matches!(dataflow.audit_policy, AuditPolicy::ManualPlaceholder { .. }))
    {
        assert!(
            matches!(dataflow.status, SourceStatus::Placeholder),
            "`{}` uses manual_placeholder but status is {:?}",
            dataflow.dataflow_id,
            dataflow.status
        );
    }
}

fn audit_policy_kind(policy: &AuditPolicy) -> &'static str {
    match policy {
        AuditPolicy::ContainsAny { .. } => "contains_any",
        AuditPolicy::DirectoryListing { .. } => "directory_listing",
        AuditPolicy::BudgetYear { .. } => "budget_year",
        AuditPolicy::LicensedProduct { .. } => "licensed_product",
        AuditPolicy::WorldBankBreadyApi { .. } => "world_bank_bready_api",
        AuditPolicy::ManualPlaceholder { .. } => "manual_placeholder",
        AuditPolicy::ManualRegisterOnly { .. } => "manual_register_only",
        AuditPolicy::BotFiltered { .. } => "bot_filtered",
    }
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
fn adapter_manifest_dataflows_are_registered_with_matching_source_ids() {
    let register = load_source_register().expect("load source register");
    let registered = register
        .dataflows
        .iter()
        .map(|dataflow| registered_dataflow_key(&dataflow.source_id, &dataflow.dataflow_id))
        .collect::<BTreeSet<_>>();

    for adapter in implemented_adapters() {
        assert_adapter_dataflows_registered(adapter.as_ref(), &registered);
    }
}

fn implemented_adapters() -> Vec<Box<dyn SourceAdapter>> {
    vec![
        Box::new(au_kpis_adapter_abs::AbsAdapter::default()),
        Box::new(au_kpis_adapter_aemo::AemoAdapter::default()),
        Box::new(au_kpis_adapter_ai_readiness::AiReadinessAdapter::default()),
        Box::new(au_kpis_adapter_apra::ApraAdapter::default()),
        Box::new(au_kpis_adapter_asx::AsxAdapter::default()),
        Box::new(au_kpis_adapter_nhsac::NhsacAdapter::default()),
        Box::new(au_kpis_adapter_pc::PcAdapter::default()),
        Box::new(au_kpis_adapter_rba::RbaAdapter::default()),
        Box::new(au_kpis_adapter_state_budgets::StateBudgetsAdapter::default()),
        Box::new(au_kpis_adapter_state_capital::StateCapitalAdapter::default()),
        Box::new(au_kpis_adapter_state_planning::StatePlanningAdapter::default()),
        Box::new(au_kpis_adapter_treasury::TreasuryAdapter::default()),
        Box::new(au_kpis_adapter_worldbank::WorldbankAdapter::default()),
    ]
}

fn assert_adapter_dataflows_registered(adapter: &dyn SourceAdapter, registered: &BTreeSet<String>) {
    let manifest = adapter.manifest();
    let manifest_dataflow_ids = manifest
        .dataflows
        .iter()
        .map(|dataflow_id| dataflow_id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let metadata = adapter.dataflow_metadata();
    let metadata_dataflow_ids = metadata
        .iter()
        .map(|dataflow| dataflow.id.as_str().to_owned())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        manifest_dataflow_ids, metadata_dataflow_ids,
        "adapter `{}` manifest dataflows must match SourceAdapter::dataflow_metadata ids",
        manifest.source_id
    );

    for dataflow_id in &manifest.dataflows {
        let key = registered_dataflow_key(&manifest.source_id, dataflow_id);
        assert!(
            registered.contains(&key),
            "adapter `{}` manifest references unregistered source/dataflow `{}`",
            manifest.source_id,
            key
        );
    }

    for dataflow in metadata {
        assert_eq!(
            dataflow.source_id, manifest.source_id,
            "adapter `{}` metadata dataflow `{}` must use the manifest source id",
            manifest.source_id, dataflow.id
        );
        let key = registered_dataflow_key(&dataflow.source_id, &dataflow.id);
        assert!(
            registered.contains(&key),
            "adapter `{}` metadata references unregistered source/dataflow `{}`",
            manifest.source_id,
            key
        );
    }
}

fn registered_dataflow_key(source_id: impl AsRef<str>, dataflow_id: impl AsRef<str>) -> String {
    format!("{}/{}", source_id.as_ref(), dataflow_id.as_ref())
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
