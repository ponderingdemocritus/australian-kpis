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
fn issue_38_k6_smoke_contract_is_wired() {
    let root = repo_root();
    let smoke_script =
        fs::read_to_string(root.join("apps/bench/smoke.js")).expect("read k6 smoke script");
    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");

    for expected in [
        "duration: '30s'",
        "http_req_duration: ['p(95)<200']",
        "http_req_failed: ['rate<0.01']",
        "/v1/health",
        "/v1/openapi.json",
        "/v1/dataflows?source=abs&frequency=quarterly",
        "/v1/dataflows/abs.cpi",
        "/v1/dataflows/abs.cpi/codelists/region",
        "/v1/observations?dataflow=abs.cpi&dimensions[region]=AUS&limit=5",
        "/v1/series/abs.cpi/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/v1/search?q=price%20index",
        "sleep(2)",
    ] {
        assert!(
            smoke_script.contains(expected),
            "k6 smoke script should cover `{expected}`"
        );
    }

    assert!(
        pr_workflow.contains("name: Smoke (k6)"),
        "smoke workflow should be the k6 PR and merge-queue gate"
    );
    assert!(
        pr_workflow.contains(
            "docker compose -f infra/compose/docker-compose.yml up -d --build api influxdb"
        ),
        "PR smoke workflow should run against the docker-compose API stack with InfluxDB"
    );
    assert!(
        pr_workflow.contains("< apps/web/e2e/fixtures/explorer.sql"),
        "PR smoke workflow should seed real endpoint data before k6 runs"
    );
    assert!(
        pr_workflow.contains("grafana/setup-k6-action"),
        "smoke workflow should install k6 explicitly"
    );
    assert!(
        pr_workflow.contains("k6 run --out \"${K6_OUT}\"")
            && pr_workflow.contains("influxdb=http://127.0.0.1:8086/k6"),
        "smoke workflow should publish k6 results to InfluxDB for Grafana trending"
    );
    assert!(
        pr_workflow.contains("AU_KPIS_STAGING_BASE_URL")
            && pr_workflow.contains("github.event_name == 'merge_group'"),
        "merge-queue smoke flow should target the configured staging API"
    );

    let compose = fs::read_to_string(root.join("infra/compose/docker-compose.yml"))
        .expect("read compose file");
    assert!(
        compose.contains("influxdb:") && compose.contains("INFLUXDB_DB: k6"),
        "compose stack should include an InfluxDB v1 database for k6 metrics"
    );
    assert!(
        root.join("infra/observability/grafana/provisioning/datasources/k6-influxdb.yml")
            .is_file(),
        "Grafana should provision a k6 InfluxDB datasource"
    );
    assert!(
        root.join("infra/observability/grafana/dashboards/k6-smoke.json")
            .is_file(),
        "Grafana should provision a k6 smoke trend dashboard"
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
