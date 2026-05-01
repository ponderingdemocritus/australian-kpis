use std::{fs, path::Path};

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("testing crate lives under crates/testing")
}

#[test]
fn benchmark_scaffold_matches_issue_contract() {
    let root = repo_root();

    assert!(
        root.join("crates/au-kpis-domain/benches/observation_json.rs")
            .is_file(),
        "au-kpis-domain should provide the first placeholder criterion bench"
    );
    assert!(
        root.join("benches/baselines/README.md").is_file(),
        "repo should document committed benchmark baseline storage"
    );
    assert!(
        root.join("apps/bench/smoke.js").is_file(),
        "k6 smoke scaffold should live under apps/bench"
    );

    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");
    assert!(
        pr_workflow.contains(
            "cargo bench -p au-kpis-domain --bench observation_json -- --save-baseline pr"
        ),
        "PR workflow should run the criterion bench and save the PR baseline"
    );
    assert!(
        pr_workflow.contains("critcmp main pr --threshold 5"),
        "PR workflow should run advisory critcmp comparison"
    );
}

#[test]
fn smoke_workflow_builds_server_before_readiness_polling() {
    let root = repo_root();
    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");

    assert!(
        pr_workflow.contains("cargo build -p au-kpis-api-http --example contract_server --locked"),
        "smoke workflow should build the contract server before starting it"
    );
    assert!(
        pr_workflow
            .contains("./target/debug/examples/contract_server > target/smoke/server.log 2>&1 &"),
        "smoke workflow should start the prebuilt contract server binary"
    );
}

#[test]
fn contract_workflow_builds_server_before_readiness_polling() {
    let root = repo_root();
    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");

    assert!(
        pr_workflow.contains("cargo build -p au-kpis-api-http --example contract_server --locked"),
        "contract workflow should build the contract server before starting it"
    );
    assert!(
        pr_workflow.contains(
            "./target/debug/examples/contract_server > target/contract/server.log 2>&1 &"
        ),
        "contract workflow should start the prebuilt contract server binary"
    );
    assert!(
        pr_workflow.contains("AU_KPIS_CONTRACT_ADDR=\"127.0.0.1:0\"")
            && pr_workflow.contains("AU_KPIS_CONTRACT_ADDR_FILE=\"target/contract/server.addr\""),
        "contract workflow should let the server bind port 0 and report the selected address"
    );
}

#[test]
fn bench_workflow_skips_missing_base_benchmark() {
    let root = repo_root();
    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");

    assert!(
        pr_workflow.contains("if [ -f crates/au-kpis-domain/benches/observation_json.rs ]; then"),
        "bench workflow should guard the main baseline when the base branch lacks the bench target"
    );
    assert!(
        pr_workflow.contains(
            "Base branch has no observation_json bench yet; skipping main benchmark baseline."
        ),
        "bench workflow should explain why the first baseline capture is skipped"
    );
}
