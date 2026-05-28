use std::{
    fs,
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
fn issue_65_cargo_mutants_weekly_contract_is_wired() {
    let root = repo_root();
    let workflow = fs::read_to_string(root.join(".github/workflows/mutation-weekly.yml"))
        .expect("read mutation workflow");
    let report_script = root.join("tools/ci/mutation_report.py");
    assert!(
        report_script.is_file(),
        "issue #65 should provide a cargo-mutants report script"
    );
    let ci_docs = fs::read_to_string(root.join("docs/ci.md")).expect("read CI docs");
    let testing_docs = fs::read_to_string(root.join("docs/testing.md")).expect("read testing docs");

    for expected in [
        "cron: \"0 6 * * 0\"",
        "cargo install cargo-mutants --version 26.0.0 --locked",
        "cargo mutants --workspace",
        "--test-tool nextest",
        "--test-workspace true",
        "--minimum-test-timeout 120",
        "MUTATION_MIN_SCORE: \"70\"",
        "tools/ci/mutation_report.py",
        "actions/upload-artifact@v4",
        "cargo-mutants-report",
        "retention-days: 30",
        "actions/github-script@v8",
        "add test",
        "surviving cargo-mutants",
    ] {
        assert!(
            workflow.contains(expected),
            "weekly cargo-mutants workflow should contain `{expected}`"
        );
    }

    assert!(
        ci_docs.contains("Weekly cargo-mutants")
            && ci_docs.contains("mutation score")
            && ci_docs.contains("add test"),
        "CI docs should document the weekly mutation workflow and follow-up issues"
    );
    assert!(
        testing_docs.contains("cargo mutants --workspace")
            && testing_docs.contains("70%")
            && testing_docs.contains("cargo-mutants-report"),
        "testing docs should document local mutation testing and retained reports"
    );
}

#[test]
fn mutation_report_scores_and_lists_surviving_mutants() {
    let root = repo_root();
    let temp_dir = temp_fixture_dir("mutation-report");
    let mutants_out = temp_dir.join("mutants.out");
    fs::create_dir_all(&mutants_out).expect("create mutants output fixture");
    fs::create_dir_all(mutants_out.join("diff")).expect("create diff fixture dir");
    fs::write(
        mutants_out.join("diff/survivor.diff"),
        "--- original\n+++ mutant\n",
    )
    .expect("write survivor diff");
    fs::write(
        mutants_out.join("outcomes.json"),
        r#"{
  "outcomes": [
    {
      "scenario": {
        "Mutant": {
          "package": "au-kpis-error",
          "file": "crates/au-kpis-error/src/lib.rs",
          "function": { "function_name": "ErrorClass::is_retryable" },
          "span": { "start": { "line": 68, "column": 9 } },
          "replacement": "true"
        }
      },
      "summary": "CaughtMutant"
    },
    {
      "scenario": {
        "Mutant": {
          "package": "au-kpis-error",
          "file": "crates/au-kpis-error/src/lib.rs",
          "function": { "function_name": "Classify::retry_after" },
          "span": { "start": { "line": 85, "column": 9 } },
          "replacement": "Some(Default::default())"
        }
      },
      "summary": "CaughtMutant"
    },
    {
      "scenario": {
        "Mutant": {
          "package": "au-kpis-error",
          "file": "crates/au-kpis-error/src/lib.rs",
          "function": { "function_name": "CoreError::class" },
          "span": { "start": { "line": 122, "column": 9 } },
          "replacement": "Default::default()"
        }
      },
      "summary": "CaughtMutant"
    },
    {
      "scenario": {
        "Mutant": {
          "package": "au-kpis-error",
          "file": "crates/au-kpis-error/src/lib.rs",
          "function": { "function_name": "Classify::retry_after" },
          "span": { "start": { "line": 85, "column": 9 } },
          "replacement": "None"
        }
      },
      "summary": "MissedMutant",
      "diff_path": "diff/survivor.diff"
    },
    {
      "scenario": {
        "Mutant": {
          "package": "au-kpis-error",
          "file": "crates/au-kpis-error/src/lib.rs",
          "function": { "function_name": "unviable" },
          "span": { "start": { "line": 1, "column": 1 } },
          "replacement": "Default::default()"
        }
      },
      "summary": "Unviable"
    }
  ]
}"#,
    )
    .expect("write outcomes fixture");

    let markdown = temp_dir.join("report.md");
    let json = temp_dir.join("report.json");
    let issue = temp_dir.join("issue.md");
    let status = Command::new("python3")
        .current_dir(root)
        .arg("tools/ci/mutation_report.py")
        .arg("--out-dir")
        .arg(&mutants_out)
        .arg("--min-score")
        .arg("70")
        .arg("--markdown")
        .arg(&markdown)
        .arg("--json")
        .arg(&json)
        .arg("--issue-body")
        .arg(&issue)
        .status()
        .expect("run mutation report script");

    assert!(status.success(), "75% score should meet a 70% threshold");
    let markdown = fs::read_to_string(markdown).expect("read markdown report");
    assert!(markdown.contains("Mutation score: 75.00%"));
    assert!(markdown.contains("Surviving Mutants"));
    assert!(markdown.contains("crates/au-kpis-error/src/lib.rs:85"));

    let json = fs::read_to_string(json).expect("read json report");
    assert!(json.contains(r#""score": 75.0"#));
    assert!(json.contains(r#""missed": 1"#));

    let issue = fs::read_to_string(issue).expect("read issue body");
    assert!(issue.contains("Follow-up add test work"));
    assert!(issue.contains("Classify::retry_after"));

    let failing = Command::new("python3")
        .current_dir(root)
        .arg("tools/ci/mutation_report.py")
        .arg("--out-dir")
        .arg(&mutants_out)
        .arg("--min-score")
        .arg("80")
        .arg("--markdown")
        .arg(temp_dir.join("failing.md"))
        .arg("--json")
        .arg(temp_dir.join("failing.json"))
        .arg("--issue-body")
        .arg(temp_dir.join("failing-issue.md"))
        .status()
        .expect("run failing mutation report script");
    assert!(!failing.success(), "75% score should fail an 80% threshold");

    let _ = fs::remove_dir_all(temp_dir);
}

fn temp_fixture_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}
