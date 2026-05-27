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
    assert!(
        !pr_workflow.contains("exit \"${critcmp_status}\""),
        "benchmark workflow should not exit before the confidence-bound regression guard runs"
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
            "docker compose -f infra/compose/docker-compose.yml up -d --wait --wait-timeout 120 postgres redis minio pdf-extractor influxdb"
        ),
        "PR smoke workflow should start and wait for compose dependencies with InfluxDB before migrating"
    );
    assert!(
        pr_workflow
            .contains("docker compose -f infra/compose/docker-compose.yml up -d --build api"),
        "PR smoke workflow should start the docker-compose API stack after seeding"
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
fn issue_49_k6_nightly_load_contract_is_wired() {
    let root = repo_root();

    let sustained =
        fs::read_to_string(root.join("apps/bench/sustained.js")).expect("read sustained script");
    let burst = fs::read_to_string(root.join("apps/bench/burst.js")).expect("read burst script");
    let nightly = fs::read_to_string(root.join(".github/workflows/k6-nightly.yml"))
        .expect("read k6 nightly workflow");
    let bench_docs =
        fs::read_to_string(root.join("apps/bench/README.md")).expect("read bench docs");
    let observability_docs =
        fs::read_to_string(root.join("docs/observability.md")).expect("read observability docs");

    for expected in [
        "vus: 100",
        "duration: '10m'",
        "http_req_duration: ['p(95)<500', 'p(99)<1500']",
        "http_req_failed: ['rate<0.001']",
        "singleSeriesRequest",
        "bulkObservationsRequest",
        "catalogRequest",
    ] {
        assert!(
            sustained.contains(expected),
            "sustained scenario should contain `{expected}`"
        );
    }

    for expected in [
        "stages:",
        "target: 2000",
        "duration: '2m'",
        "rateLimitResponses",
        "serverErrorResponses",
        "rate_limit_ratio",
        "server_error_ratio",
        "rate_limit_seen",
    ] {
        assert!(
            burst.contains(expected),
            "burst scenario should contain `{expected}`"
        );
    }

    for expected in [
        "cron: \"0 2 * * *\"",
        "AU_KPIS_STAGING_BASE_URL",
        "grafana/setup-k6-action",
        "apps/bench/sustained.js",
        "apps/bench/burst.js",
        "K6_OUT",
        "influxdb=",
        "perf:regression",
        "actions/github-script",
        "k6 load comparison",
    ] {
        assert!(
            nightly.contains(expected),
            "nightly k6 workflow should contain `{expected}`"
        );
    }

    assert!(
        root.join("infra/observability/grafana/dashboards/k6-load.json")
            .is_file(),
        "Grafana should provision a sustained/burst k6 load dashboard"
    );
    assert!(
        bench_docs.contains("sustained.js")
            && bench_docs.contains("burst.js")
            && bench_docs.contains("perf:regression"),
        "benchmark docs should describe sustained, burst, and PR comparison runs"
    );
    assert!(
        observability_docs.contains("k6 sustained and burst")
            && observability_docs.contains("k6-load.json"),
        "observability docs should document historical k6 load trending"
    );
}

#[test]
fn smoke_workflow_migrates_before_starting_api() {
    let root = repo_root();
    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");
    let smoke_job = pr_workflow
        .find("  smoke:")
        .expect("PR workflow should define a smoke job");
    let smoke_workflow = &pr_workflow[smoke_job..];

    let dependencies = smoke_workflow
        .find("Start compose smoke dependencies")
        .expect("smoke workflow should start dependencies before migrations");
    let migrations = smoke_workflow
        .find("Apply migrations")
        .expect("smoke workflow should apply migrations");
    let seed = smoke_workflow
        .find("Seed smoke fixture data")
        .expect("smoke workflow should seed fixture data");
    let api = smoke_workflow
        .find("Start local smoke API")
        .expect("smoke workflow should start the API after seeding");

    assert!(
        dependencies < migrations && migrations < seed && seed < api,
        "smoke workflow should start dependencies, migrate, seed, then start the API"
    );
    assert!(
        smoke_workflow
            .contains("--wait --wait-timeout 120 postgres redis minio pdf-extractor influxdb"),
        "smoke workflow should wait for dependency health checks before migrating"
    );
    assert!(
        smoke_workflow.contains("SELECT 1"),
        "smoke workflow should wait for a stable SQL round trip before migrating"
    );
}

#[test]
fn issue_39_schemathesis_contract_is_wired() {
    let root = repo_root();

    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");
    let contract_config = fs::read_to_string(root.join("tests/contract/schemathesis.toml"))
        .expect("read schemathesis PR config");
    let deep_config = fs::read_to_string(root.join("tests/contract/schemathesis.deep.toml"))
        .expect("read schemathesis deep-fuzz config");
    let nightly_workflow = fs::read_to_string(root.join(".github/workflows/contract-nightly.yml"))
        .expect("read nightly contract workflow");
    let ci_docs = fs::read_to_string(root.join("docs/ci.md")).expect("read CI docs");
    let testing_docs = fs::read_to_string(root.join("docs/testing.md")).expect("read testing docs");

    assert!(
        pr_workflow
            .contains("docker compose -f infra/compose/docker-compose.yml up -d --build api"),
        "PR contract workflow should fuzz the docker-compose API stack"
    );
    assert!(
        pr_workflow.contains("< apps/web/e2e/fixtures/explorer.sql"),
        "PR contract workflow should seed representative API data before fuzzing"
    );
    assert!(
        pr_workflow.contains("schemathesis --config-file tests/contract/schemathesis.toml run"),
        "PR contract workflow should run the committed schemathesis config"
    );
    assert!(
        pr_workflow.contains("--report-junit-path target/contract/schemathesis.xml"),
        "PR contract workflow should emit a JUnit report artifact"
    );
    assert!(
        pr_workflow.contains("merge_group:") && pr_workflow.contains("- contract"),
        "contract checks should remain blocking in merge queue batches through CI OK"
    );

    for expected in [
        "generation.max-examples = 8",
        "request-timeout = 5.0",
        "hooks = \"tests/contract/hooks.py\"",
        "stateful.enabled = false",
    ] {
        assert!(
            contract_config.contains(expected),
            "PR schemathesis config should contain `{expected}`"
        );
    }
    for expected in [
        "include-path = \"/v1/dataflows/{id}/codelists/{dim}\"",
        "parameters = { \"path.id\" = \"abs.cpi\", \"path.dim\" = \"region\" }",
        "include-path = \"/v1/observations\"",
        "parameters = { \"query.dataflow\" = \"abs.cpi\", \"query.format\" = \"json\" }",
        "include-path = \"/v1/series/{dataflow}/{series_key}\"",
        "parameters = { \"path.dataflow\" = \"abs.cpi\", \"path.series_key\" = \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\" }",
    ] {
        assert!(
            contract_config.contains(expected),
            "PR schemathesis config should provide realistic seeded data via `{expected}`"
        );
    }
    for expected in [
        "parameters = { \"path.id\" = \"$AU_KPIS_CONTRACT_DATAFLOW\", \"path.dim\" = \"$AU_KPIS_CONTRACT_DIMENSION\" }",
        "parameters = { \"query.dataflow\" = \"$AU_KPIS_CONTRACT_DATAFLOW\", \"query.format\" = \"json\" }",
        "parameters = { \"path.dataflow\" = \"$AU_KPIS_CONTRACT_DATAFLOW\", \"path.series_key\" = \"$AU_KPIS_CONTRACT_SERIES_KEY\" }",
    ] {
        assert!(
            deep_config.contains(expected),
            "deep-fuzz schemathesis config should provide staging-overridable seeded data via `{expected}`"
        );
    }
    assert!(
        !contract_config.contains("include-path = \"/v1/health\""),
        "PR schemathesis config should not narrow coverage to only the health endpoint"
    );

    for expected in [
        "generation.max-examples = 256",
        "request-timeout = 10.0",
        "hooks = \"tests/contract/hooks.py\"",
        "stateful.enabled = false",
    ] {
        assert!(
            deep_config.contains(expected),
            "deep-fuzz schemathesis config should contain `{expected}`"
        );
    }

    for expected in [
        "cron: \"0 4 * * *\"",
        "AU_KPIS_STAGING_BASE_URL",
        "tests/contract/schemathesis.deep.toml",
        "tests/contract/hooks.py",
        "AU_KPIS_CONTRACT_SERIES_KEY",
        "schemathesis --config-file tests/contract/schemathesis.deep.toml run",
        "--url \"${AU_KPIS_STAGING_BASE_URL}\"",
    ] {
        assert!(
            nightly_workflow.contains(expected),
            "nightly contract workflow should contain `{expected}`"
        );
    }

    assert!(
        ci_docs.contains("Nightly schemathesis deep fuzzing")
            && testing_docs.contains("schemathesis.deep.toml"),
        "CI and testing docs should document PR and nightly contract fuzzing"
    );
}

#[test]
fn contract_workflow_uses_compose_api_before_fuzzing() {
    let root = repo_root();
    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read pr workflow");
    let contract_job = pr_workflow
        .find("  contract:")
        .expect("PR workflow should define a contract job");
    let contract_workflow = &pr_workflow[contract_job..];

    let compose_stack = contract_workflow
        .find("Start compose contract stack")
        .expect("contract workflow should start the compose API stack");
    let readiness = contract_workflow
        .find("Wait for API readiness")
        .expect("contract workflow should poll API readiness");
    let schemathesis = contract_workflow
        .find("name: Run Schemathesis")
        .expect("contract workflow should run schemathesis");

    assert!(
        compose_stack < readiness && readiness < schemathesis,
        "contract workflow should start compose, wait for readiness, then fuzz"
    );
    assert!(
        !pr_workflow.contains("contract_server"),
        "contract workflow should use the real compose API instead of the legacy contract server"
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
