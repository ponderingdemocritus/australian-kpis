use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_state_budgets::{NswBudgetAdapter, NswBudgetPublication, NswBudgetRevision};
use chrono::{TimeZone, Utc};

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

fn publication(
    budget_year: &'static str,
    last_updated: &'static str,
    source_url: &'static str,
) -> NswBudgetPublication {
    NswBudgetPublication {
        budget_year: budget_year.into(),
        paper: "Budget Paper No. 1".into(),
        paper_slug: "bp1-budget-statement".into(),
        title: "Budget Statement".into(),
        source_url: source_url.into(),
        last_updated: Some(last_updated.into()),
    }
}

fn fixture_publications() -> Vec<NswBudgetPublication> {
    vec![
        publication(
            "2024-25",
            "2024-06-18",
            "https://www.budget.nsw.gov.au/sites/default/files/2024-06/bp1-budget-statement-nsw-budget-2024-25.pdf",
        ),
        publication(
            "2025-26",
            "2025-06-24",
            "https://www.budget.nsw.gov.au/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf",
        ),
    ]
}

#[test]
fn discoverable_jobs_apply_nsw_revision_and_license_metadata() {
    let current = fixture_publications();
    let known_revisions = BTreeMap::from([(
        "NSW:bp1-budget-statement:2024-25".to_string(),
        NswBudgetRevision::new("2024-06-18", Some("2024-06-18")),
    )]);
    let jobs = NswBudgetAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];
    assert_eq!(job.source_id.as_str(), "state-budgets");
    assert_eq!(job.dataflow_id.as_str(), "state_budgets.nsw_budget");
    assert_eq!(job.trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(job.metadata["jurisdiction"], "NSW");
    assert_eq!(job.metadata["budget_year"], "2025-26");
    assert_eq!(job.metadata["artifact_date"], "2025-06-24");
    assert_eq!(job.metadata["artifact_format"], "pdf");
    assert_eq!(
        job.metadata["license"],
        "Creative Commons Attribution 3.0 Australia Licence"
    );
    assert_eq!(
        job.metadata["license_url"],
        "https://creativecommons.org/licenses/by/3.0/au/"
    );
    assert_eq!(job.metadata["attribution"], "Source: NSW Treasury");
    assert_eq!(
        job.metadata["schema_drift_policy"],
        "hash-pdf-table-candidates"
    );
    assert_eq!(
        job.metadata["revision_key"],
        "NSW:bp1-budget-statement:2025-26"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_returns_hand_curated_nsw_budget_publications() {
    let adapter = NswBudgetAdapter::builder()
        .publications(fixture_publications())
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover NSW budget PDFs");

    assert_eq!(jobs.len(), 2);
    assert!(
        jobs.iter()
            .all(|job| job.source_id.as_str() == "state-budgets")
    );
    assert!(
        jobs.iter()
            .all(|job| job.dataflow_id.as_str() == "state_budgets.nsw_budget")
    );
    assert!(
        jobs.iter()
            .all(|job| job.trace_parent.as_deref() == Some(TRACE_PARENT))
    );
    assert_eq!(jobs[0].metadata["budget_year"], "2024-25");
    assert_eq!(jobs[1].metadata["budget_year"], "2025-26");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_honours_requested_dataflow_scope() {
    let adapter = NswBudgetAdapter::builder()
        .publications(fixture_publications())
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap())
        .with_requested_dataflow_id(au_kpis_domain::DataflowId::new("abs.cpi").unwrap());

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover requested dataflow");

    assert!(jobs.is_empty());
}

#[test]
fn manifest_declares_nsw_rate_limit_and_dataflow_metadata() {
    let adapter = NswBudgetAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "state-budgets");
    assert_eq!(manifest.rate_limit.max_requests, 20);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![au_kpis_domain::DataflowId::new("state_budgets.nsw_budget").unwrap()]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 1);
    assert_eq!(dataflows[0].id.as_str(), "state_budgets.nsw_budget");
    assert_eq!(dataflows[0].source_id.as_str(), "state-budgets");
    assert_eq!(dataflows[0].frequency, au_kpis_domain::Frequency::Annual);
    assert_eq!(
        dataflows[0].license,
        au_kpis_domain::License::Other("Creative Commons Attribution 3.0 Australia Licence".into())
    );
    assert_eq!(dataflows[0].attribution, "Source: NSW Treasury");
    assert_eq!(
        dataflows[0].source_url,
        "https://www.budget.nsw.gov.au/2025-26/budget-papers"
    );
}
