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
            "cargo bench -p au-kpis-domain --bench observation_json --locked -- --save-baseline"
        ),
        "PR workflow should run the observation JSON criterion bench"
    );
    assert!(
        pr_workflow.contains(
            "cargo bench -p au-kpis-adapter-abs --bench sdmx_parse --locked -- --save-baseline"
        ),
        "PR workflow should run the ABS SDMX parse criterion bench"
    );
    assert!(
        pr_workflow.contains(
            "cargo bench -p au-kpis-loader --bench copy_upsert --locked -- --save-baseline"
        ),
        "PR workflow should run the loader COPY criterion bench"
    );
    assert!(
        pr_workflow.contains(
            "cargo bench -p au-kpis-api-http --bench observations_handler --locked -- --save-baseline"
        ),
        "PR workflow should run the API observations handler criterion bench"
    );
    assert!(
        pr_workflow.contains("critcmp main pr --threshold 5"),
        "PR workflow should run blocking critcmp comparison"
    );
}

#[test]
fn issue_37_benchmark_contract_is_wired() {
    let root = repo_root();

    for bench in [
        "crates/adapters/abs/benches/sdmx_parse.rs",
        "crates/au-kpis-loader/benches/copy_upsert.rs",
        "crates/au-kpis-api-http/benches/observations_handler.rs",
    ] {
        assert!(
            root.join(bench).is_file(),
            "issue #37 should provide benchmark target `{bench}`"
        );
    }

    let baseline = fs::read_to_string(root.join("benches/baselines/issue-37.md"))
        .expect("issue #37 benchmark baseline summary should be committed");
    for expected in [
        "SDMX parse bench",
        ">500k observations/s",
        "Loader COPY bench",
        "10k rows <500 ms",
        "API handler overhead",
        "<5 ms above DB",
    ] {
        assert!(
            baseline.contains(expected),
            "baseline summary should document `{expected}`"
        );
    }

    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");
    assert!(
        pr_workflow.contains("merge_group:"),
        "benchmark regression gate should run for merge queue batches"
    );
    assert!(
        pr_workflow.contains("name: Bench Regression"),
        "benchmark job should be blocking, not advisory"
    );
    assert!(
        !pr_workflow.contains("Bench Advisory"),
        "benchmark job should no longer be named advisory"
    );
    assert!(
        !pr_workflow.contains("continue-on-error: true\n    permissions:\n      contents: read"),
        "benchmark job should not be configured as continue-on-error"
    );
    assert!(
        pr_workflow.contains("run_benchmarks pr"),
        "benchmark workflow should save a PR baseline for every committed criterion bench"
    );
    assert!(
        pr_workflow.contains("critcmp main pr --threshold 5"),
        "benchmark workflow should block on >5% regressions"
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
fn bench_workflow_runs_workspace_baselines_without_first_bench_skip() {
    let root = repo_root();
    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");

    assert!(
        pr_workflow.contains("run_benchmarks main"),
        "bench workflow should capture a main baseline"
    );
    assert!(
        !pr_workflow.contains("skipping advisory critcmp comparison"),
        "bench workflow should not skip regression comparison now that benchmarks are blocking"
    );
}

#[test]
fn issue_50_parquet_stream_benchmark_contract_is_wired() {
    let root = repo_root();

    assert!(
        root.join("crates/au-kpis-api-http/benches/parquet_stream.rs")
            .is_file(),
        "issue #50 should provide a dedicated 1M-row Parquet Criterion bench"
    );
    assert!(
        root.join("crates/au-kpis-api-http/tests/parquet_memory.rs")
            .is_file(),
        "issue #50 should provide a DHAT memory-budget test"
    );

    let api_manifest = fs::read_to_string(root.join("crates/au-kpis-api-http/Cargo.toml"))
        .expect("read api-http manifest");
    assert!(
        api_manifest.contains("dhat-heap = [\"dep:dhat\"]"),
        "api-http should expose a dhat-heap feature for the memory profile"
    );
    assert!(
        api_manifest.contains("name = \"parquet_stream\""),
        "api-http manifest should register the parquet_stream Criterion bench"
    );

    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");
    assert!(
        pr_workflow.contains(
            "cargo bench -p au-kpis-api-http --bench parquet_stream --locked -- --save-baseline"
        ),
        "benchmark workflow should run the 1M-row Parquet Criterion bench"
    );
    assert!(
        pr_workflow.contains("cargo test -p au-kpis-api-http")
            && pr_workflow.contains("--features dhat-heap")
            && pr_workflow.contains("--test parquet_memory"),
        "CI should enforce the 1M-row Parquet DHAT memory budget"
    );
    assert!(
        pr_workflow.contains("critcmp main pr --threshold 5"),
        "merge-queue benchmark regression threshold should remain 5%"
    );

    let baseline = fs::read_to_string(root.join("benches/baselines/issue-50.md"))
        .expect("issue #50 benchmark baseline summary should be committed");
    for expected in ["Parquet 1M-row stream", "<30 s", "<100 MB", "dhat"] {
        assert!(
            baseline.contains(expected),
            "baseline summary should document `{expected}`"
        );
    }
}
