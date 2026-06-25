use std::{collections::BTreeMap, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveryCtx, SourceAdapter};
use au_kpis_adapter_state_budgets::{
    NswBudgetAdapter, NswBudgetPublication, NswBudgetRevision, QldBudgetAdapter,
    QldBudgetPublication, QldBudgetRevision, StateBudgetsAdapter, VicBudgetAdapter,
    VicBudgetPublication, VicBudgetRevision,
};
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
async fn default_nsw_discovery_uses_current_official_nsw_budget_urls() {
    let adapter = NswBudgetAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 6, 19, 0, 0, 0).unwrap());

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover default NSW budget");

    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0].source_url,
        "https://www.nsw.gov.au/sites/default/files/noindex/2026-03/bp1-budget-statement-nsw-budget-2025-26.pdf"
    );
    assert_eq!(
        jobs[0].metadata["source_index_url"],
        "https://www.nsw.gov.au/business-and-economy/nsw-budget/2025-26-budget-papers"
    );
    assert_eq!(jobs[0].metadata["artifact_date"], "2026-03-20");
    assert_eq!(jobs[0].metadata["budget_year"], "2025-26");

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(
        dataflows[0].source_url,
        "https://www.nsw.gov.au/business-and-economy/nsw-budget/2025-26-budget-papers"
    );
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
        "https://www.nsw.gov.au/business-and-economy/nsw-budget/2025-26-budget-papers"
    );
}

fn vic_publication(
    budget_year: &'static str,
    last_updated: &'static str,
    source_url: &'static str,
) -> VicBudgetPublication {
    VicBudgetPublication {
        budget_year: budget_year.into(),
        paper: "Budget Paper No. 5".into(),
        paper_slug: "bp5-statement-of-finances".into(),
        title: "Statement of Finances".into(),
        source_url: source_url.into(),
        last_updated: Some(last_updated.into()),
    }
}

fn vic_fixture_publications() -> Vec<VicBudgetPublication> {
    vec![
        vic_publication(
            "2025-26",
            "2025-05-17",
            "https://s3.ap-southeast-2.amazonaws.com/vicbudgetfiles2025.26vicbudget/2025-26+State+Budget+-+Statement+of+Finances.pdf",
        ),
        vic_publication(
            "2026-27",
            "2026-05-05",
            "https://s3.ap-southeast-2.amazonaws.com/vicbudgetfiles2026.27vicbudget/2026-27+State+Budget+-+Statement+of+Finances.pdf",
        ),
    ]
}

fn qld_publication(
    budget_year: &'static str,
    last_updated: &'static str,
    source_url: &'static str,
) -> QldBudgetPublication {
    QldBudgetPublication {
        budget_year: budget_year.into(),
        paper: "Budget Paper No. 2".into(),
        paper_slug: "bp2-budget-strategy-outlook".into(),
        title: "Budget Strategy and Outlook".into(),
        source_url: source_url.into(),
        last_updated: Some(last_updated.into()),
    }
}

fn qld_fixture_publications() -> Vec<QldBudgetPublication> {
    vec![qld_publication(
        "2025-26",
        "2025-06-24",
        "https://budget.qld.gov.au/files/Budget-2025-26-BP2-Budget-Strategy-Outlook.pdf",
    )]
}

#[test]
fn discoverable_jobs_apply_vic_revision_and_license_metadata() {
    let current = vic_fixture_publications();
    let known_revisions = BTreeMap::from([(
        "VIC:bp5-statement-of-finances:2025-26".to_string(),
        VicBudgetRevision::new("2025-05-17", Some("2025-05-17")),
    )]);
    let jobs = VicBudgetAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];
    assert_eq!(job.source_id.as_str(), "state-budgets");
    assert_eq!(job.dataflow_id.as_str(), "state_budgets.vic_budget");
    assert_eq!(job.trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(job.metadata["jurisdiction"], "VIC");
    assert_eq!(job.metadata["budget_year"], "2026-27");
    assert_eq!(job.metadata["artifact_date"], "2026-05-05");
    assert_eq!(job.metadata["artifact_format"], "pdf");
    assert_eq!(
        job.metadata["license"],
        "Creative Commons Attribution 4.0 International licence"
    );
    assert_eq!(
        job.metadata["license_url"],
        "https://creativecommons.org/licenses/by/4.0/"
    );
    assert_eq!(
        job.metadata["attribution"],
        "© Copyright State Government of Victoria"
    );
    assert_eq!(
        job.metadata["schema_drift_policy"],
        "hash-pdf-table-candidates"
    );
    assert_eq!(
        job.metadata["revision_key"],
        "VIC:bp5-statement-of-finances:2026-27"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_returns_hand_curated_vic_budget_publications() {
    let adapter = VicBudgetAdapter::builder()
        .publications(vic_fixture_publications())
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover VIC budget PDFs");

    assert_eq!(jobs.len(), 2);
    assert!(
        jobs.iter()
            .all(|job| job.source_id.as_str() == "state-budgets")
    );
    assert!(
        jobs.iter()
            .all(|job| job.dataflow_id.as_str() == "state_budgets.vic_budget")
    );
    assert!(
        jobs.iter()
            .all(|job| job.trace_parent.as_deref() == Some(TRACE_PARENT))
    );
    assert_eq!(jobs[0].metadata["budget_year"], "2025-26");
    assert_eq!(jobs[1].metadata["budget_year"], "2026-27");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vic_discover_honours_requested_dataflow_scope() {
    let adapter = VicBudgetAdapter::builder()
        .publications(vic_fixture_publications())
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap())
        .with_requested_dataflow_id(
            au_kpis_domain::DataflowId::new("state_budgets.nsw_budget").unwrap(),
        );

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover requested dataflow");

    assert!(jobs.is_empty());
}

#[test]
fn manifest_declares_vic_rate_limit_and_dataflow_metadata() {
    let adapter = VicBudgetAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "state-budgets");
    assert_eq!(manifest.rate_limit.max_requests, 20);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![au_kpis_domain::DataflowId::new("state_budgets.vic_budget").unwrap()]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 1);
    assert_eq!(dataflows[0].id.as_str(), "state_budgets.vic_budget");
    assert_eq!(dataflows[0].source_id.as_str(), "state-budgets");
    assert_eq!(dataflows[0].frequency, au_kpis_domain::Frequency::Annual);
    assert_eq!(dataflows[0].license, au_kpis_domain::License::CcBy40);
    assert_eq!(
        dataflows[0].attribution,
        "© Copyright State Government of Victoria"
    );
    assert_eq!(
        dataflows[0].source_url,
        "https://www.budget.vic.gov.au/budget-papers"
    );
}

#[test]
fn discoverable_jobs_apply_qld_revision_and_license_metadata() {
    let current = qld_fixture_publications();
    let known_revisions = BTreeMap::from([(
        "QLD:bp2-budget-strategy-outlook:2024-25".to_string(),
        QldBudgetRevision::new("2024-06-11", Some("2024-06-11")),
    )]);
    let jobs = QldBudgetAdapter::discoverable_jobs_with_started_at(
        &current,
        &known_revisions,
        Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
        Some(TRACE_PARENT),
    );

    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];
    assert_eq!(job.source_id.as_str(), "state-budgets");
    assert_eq!(job.dataflow_id.as_str(), "state_budgets.qld_budget");
    assert_eq!(job.trace_parent.as_deref(), Some(TRACE_PARENT));
    assert_eq!(job.metadata["jurisdiction"], "QLD");
    assert_eq!(job.metadata["budget_year"], "2025-26");
    assert_eq!(job.metadata["artifact_date"], "2025-06-24");
    assert_eq!(job.metadata["artifact_format"], "pdf");
    assert_eq!(job.metadata["license"], "Queensland Treasury copyright");
    assert_eq!(
        job.metadata["license_url"],
        "https://www.treasury.qld.gov.au/legal/copyright/"
    );
    assert_eq!(
        job.metadata["attribution"],
        "© The State of Queensland 2025 (Queensland Treasury)"
    );
    assert_eq!(
        job.metadata["schema_drift_policy"],
        "hash-pdf-table-candidates"
    );
    assert_eq!(
        job.metadata["revision_key"],
        "QLD:bp2-budget-strategy-outlook:2025-26"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_returns_hand_curated_qld_budget_publications() {
    let adapter = QldBudgetAdapter::builder()
        .publications(qld_fixture_publications())
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap())
        .with_trace_parent(TRACE_PARENT);

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover QLD budget PDFs");

    assert_eq!(jobs.len(), 1);
    assert!(
        jobs.iter()
            .all(|job| job.source_id.as_str() == "state-budgets")
    );
    assert!(
        jobs.iter()
            .all(|job| job.dataflow_id.as_str() == "state_budgets.qld_budget")
    );
    assert!(
        jobs.iter()
            .all(|job| job.trace_parent.as_deref() == Some(TRACE_PARENT))
    );
    assert_eq!(jobs[0].metadata["budget_year"], "2025-26");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_qld_discovery_uses_current_official_qld_budget_url() {
    let adapter = QldBudgetAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 6, 25, 0, 0, 0).unwrap());

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover default QLD budget");

    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0].source_url,
        "https://budget.qld.gov.au/files/2026-27-budget-bp2-budget-strategy-outlook.pdf"
    );
    assert_eq!(
        jobs[0].metadata["source_index_url"],
        "https://budget.qld.gov.au/budget-papers/"
    );
    assert_eq!(jobs[0].metadata["artifact_date"], "2026-06-24");
    assert_eq!(jobs[0].metadata["budget_year"], "2026-27");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qld_discover_honours_requested_dataflow_scope() {
    let adapter = QldBudgetAdapter::builder()
        .publications(qld_fixture_publications())
        .build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap())
        .with_requested_dataflow_id(
            au_kpis_domain::DataflowId::new("state_budgets.vic_budget").unwrap(),
        );

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover requested dataflow");

    assert!(jobs.is_empty());
}

#[test]
fn manifest_declares_qld_rate_limit_and_dataflow_metadata() {
    let adapter = QldBudgetAdapter::default();
    let manifest = adapter.manifest();

    assert_eq!(manifest.source_id.as_str(), "state-budgets");
    assert_eq!(manifest.rate_limit.max_requests, 20);
    assert_eq!(manifest.rate_limit.per, Duration::from_secs(60));
    assert_eq!(
        manifest.dataflows,
        vec![au_kpis_domain::DataflowId::new("state_budgets.qld_budget").unwrap()]
    );

    let dataflows = adapter.dataflow_metadata();
    assert_eq!(dataflows.len(), 1);
    assert_eq!(dataflows[0].id.as_str(), "state_budgets.qld_budget");
    assert_eq!(dataflows[0].source_id.as_str(), "state-budgets");
    assert_eq!(dataflows[0].frequency, au_kpis_domain::Frequency::Annual);
    assert_eq!(
        dataflows[0].license,
        au_kpis_domain::License::Other("Queensland Treasury copyright".into())
    );
    assert_eq!(
        dataflows[0].attribution,
        "© The State of Queensland 2025 (Queensland Treasury)"
    );
    assert_eq!(
        dataflows[0].source_url,
        "https://budget.qld.gov.au/budget-papers/"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn combined_state_budgets_adapter_routes_discovery_by_dataflow() {
    let adapter = StateBudgetsAdapter::new(
        NswBudgetAdapter::builder()
            .publications(fixture_publications())
            .build(),
        VicBudgetAdapter::builder()
            .publications(vic_fixture_publications())
            .build(),
        QldBudgetAdapter::builder()
            .publications(qld_fixture_publications())
            .build(),
    );
    let manifest = adapter.manifest();
    assert_eq!(
        manifest.dataflows,
        vec![
            au_kpis_domain::DataflowId::new("state_budgets.nsw_budget").unwrap(),
            au_kpis_domain::DataflowId::new("state_budgets.vic_budget").unwrap(),
            au_kpis_domain::DataflowId::new("state_budgets.qld_budget").unwrap(),
        ]
    );

    let http = AdapterHttpClient::new(manifest.rate_limit);
    let ctx = DiscoveryCtx::new(http, Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap())
        .with_requested_dataflow_id(
            au_kpis_domain::DataflowId::new("state_budgets.qld_budget").unwrap(),
        );

    let jobs = adapter
        .discover(&ctx)
        .await
        .expect("discover requested QLD state budget");

    assert_eq!(jobs.len(), 1);
    assert!(
        jobs.iter()
            .all(|job| job.dataflow_id.as_str() == "state_budgets.qld_budget")
    );
}
