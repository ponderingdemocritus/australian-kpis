use std::{collections::BTreeSet, fs, path::Path};

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
                dataflow.source_id.clone(),
                dataflow.dataflow_id.clone(),
                dataflow.canonical_url.clone(),
                audit_policy_kind(&dataflow.audit_policy).to_string(),
            ));
        }
        for additional in &dataflow.additional_audit_policies {
            if additional.policy.emits_source_location_rule() {
                registered_audited.insert((
                    dataflow.source_id.clone(),
                    dataflow.dataflow_id.clone(),
                    additional.url.clone(),
                    audit_policy_kind(&additional.policy).to_string(),
                ));
            }
        }
    }
    let scheduler_rules = default_source_location_rules()
        .expect("load default source-location rules")
        .iter()
        .map(|rule| {
            (
                rule.source_id.clone(),
                rule.dataflow_id.clone(),
                rule.current_url.clone(),
                rule.audit_policy_kind
                    .as_deref()
                    .expect("register-derived rule must expose audit policy kind")
                    .to_string(),
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
        assert_adapter_dataflows_registered(adapter.adapter.as_ref(), &registered);
    }
}

#[test]
fn every_workspace_adapter_crate_is_covered_by_register_contract() {
    let workspace_adapters = workspace_adapter_package_names();
    let covered_adapters = implemented_adapters()
        .iter()
        .map(|adapter| adapter.package_name.to_owned())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        workspace_adapters, covered_adapters,
        "every crates/adapters package must be covered by implemented_adapters()"
    );
}

struct ImplementedAdapter {
    package_name: &'static str,
    adapter: Box<dyn SourceAdapter>,
}

fn implemented_adapters() -> Vec<ImplementedAdapter> {
    vec![
        ImplementedAdapter {
            package_name: "au-kpis-adapter-abs",
            adapter: Box::new(au_kpis_adapter_abs::AbsAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-aemo",
            adapter: Box::new(au_kpis_adapter_aemo::AemoAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-ai-readiness",
            adapter: Box::new(au_kpis_adapter_ai_readiness::AiReadinessAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-apra",
            adapter: Box::new(au_kpis_adapter_apra::ApraAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-asx",
            adapter: Box::new(au_kpis_adapter_asx::AsxAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-nhsac",
            adapter: Box::new(au_kpis_adapter_nhsac::NhsacAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-pc",
            adapter: Box::new(au_kpis_adapter_pc::PcAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-rba",
            adapter: Box::new(au_kpis_adapter_rba::RbaAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-state-budgets",
            adapter: Box::new(au_kpis_adapter_state_budgets::StateBudgetsAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-state-capital",
            adapter: Box::new(au_kpis_adapter_state_capital::StateCapitalAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-state-planning",
            adapter: Box::new(au_kpis_adapter_state_planning::StatePlanningAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-treasury",
            adapter: Box::new(au_kpis_adapter_treasury::TreasuryAdapter::default()),
        },
        ImplementedAdapter {
            package_name: "au-kpis-adapter-worldbank",
            adapter: Box::new(au_kpis_adapter_worldbank::WorldbankAdapter::default()),
        },
    ]
}

fn workspace_adapter_package_names() -> BTreeSet<String> {
    let adapters_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("crates/adapters");
    fs::read_dir(adapters_dir)
        .expect("read crates/adapters")
        .map(|entry| entry.expect("read adapter entry").path().join("Cargo.toml"))
        .filter(|path| path.exists())
        .map(|path| {
            let manifest = fs::read_to_string(&path).expect("read adapter Cargo.toml");
            manifest
                .lines()
                .find_map(|line| line.trim().strip_prefix("name = \""))
                .and_then(|value| value.strip_suffix('"'))
                .expect("adapter Cargo.toml declares package name")
                .to_owned()
        })
        .collect()
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
