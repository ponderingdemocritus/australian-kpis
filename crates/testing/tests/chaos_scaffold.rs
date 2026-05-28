use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("testing crate lives under crates/testing")
}

#[test]
fn issue_63_chaos_suite_contract_is_wired() {
    let root = repo_root();
    let workflow =
        fs::read_to_string(root.join(".github/workflows/chaos-weekly.yml")).expect("read workflow");
    let docs = fs::read_to_string(root.join("docs/chaos.md")).expect("read chaos docs");
    let run_script = root.join("tests/chaos/run.sh");
    assert!(run_script.is_file(), "chaos suite runner should exist");
    assert_executable(&run_script);

    for expected in [
        "cron: \"0 5 * * 0\"",
        "workflow_dispatch",
        "environment: staging",
        "tests/chaos/run.sh",
        "chaos-results",
        "target/chaos",
        "GITHUB_STEP_SUMMARY",
    ] {
        assert!(
            workflow.contains(expected),
            "weekly chaos workflow should contain `{expected}`"
        );
    }

    for scenario in [
        "kill-ingestion-mid-load",
        "sever-db-connection",
        "fill-queue-capacity",
        "source-5xx-circuit-breaker",
        "vacuum-heavy-writes",
    ] {
        let script = root.join(format!("tests/chaos/{scenario}.sh"));
        assert!(
            script.is_file(),
            "scenario script `{scenario}` should exist"
        );
        assert_executable(&script);
        assert!(
            fs::read_to_string(&script)
                .expect("read scenario script")
                .contains("record_result"),
            "scenario `{scenario}` should write a machine-readable result"
        );
        assert!(
            docs.contains(scenario),
            "docs/chaos.md should document scenario `{scenario}`"
        );
        assert!(
            workflow.contains(scenario)
                || fs::read_to_string(&run_script)
                    .expect("read chaos runner")
                    .contains(scenario),
            "runner or workflow should invoke scenario `{scenario}`"
        );
    }

    for invariant in [
        "no duplicates/no gaps",
        "reconnection",
        "backpressure",
        "circuit breaker opens and recovers",
        "no deadlocks",
        "chaos-results",
    ] {
        assert!(
            docs.contains(invariant),
            "docs/chaos.md should explain `{invariant}`"
        );
    }
}

#[test]
fn issue_63_chaos_suite_dry_run_surfaces_reviewable_results() {
    let root = repo_root();
    let temp_dir = temp_fixture_dir("chaos-results");

    let output = Command::new("bash")
        .current_dir(root)
        .arg("tests/chaos/run.sh")
        .arg("--dry-run")
        .arg("--results-dir")
        .arg(&temp_dir)
        .output()
        .expect("run chaos dry-run");
    assert!(
        output.status.success(),
        "chaos dry-run should pass: stdout=\n{}\nstderr=\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let summary = fs::read_to_string(temp_dir.join("summary.md")).expect("read summary");
    let jsonl = fs::read_to_string(temp_dir.join("results.jsonl")).expect("read result jsonl");

    for scenario in [
        "kill-ingestion-mid-load",
        "sever-db-connection",
        "fill-queue-capacity",
        "source-5xx-circuit-breaker",
        "vacuum-heavy-writes",
    ] {
        assert!(
            summary.contains(scenario),
            "summary should include `{scenario}`"
        );
        assert!(jsonl.contains(&format!(r#""scenario":"{scenario}""#)));
    }
    assert_eq!(
        jsonl
            .lines()
            .filter(|line| line.contains(r#""status":"dry-run""#))
            .count(),
        5,
        "dry-run should record one result per scenario"
    );

    let _ = fs::remove_dir_all(temp_dir);
}

fn assert_executable(path: &Path) {
    let mode = fs::metadata(path)
        .unwrap_or_else(|err| panic!("read metadata for {}: {err}", path.display()))
        .permissions()
        .mode();
    assert_ne!(mode & 0o111, 0, "{} should be executable", path.display());
}

fn temp_fixture_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}
