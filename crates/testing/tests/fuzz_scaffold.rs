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
fn issue_64_cargo_fuzz_nightly_contract_is_wired() {
    let root = repo_root();
    let workflow = fs::read_to_string(root.join(".github/workflows/fuzz-nightly.yml"))
        .expect("read fuzz workflow");
    let fuzz_manifest =
        fs::read_to_string(root.join("fuzz/Cargo.toml")).expect("read cargo-fuzz manifest");
    let ci_docs = fs::read_to_string(root.join("docs/ci.md")).expect("read CI docs");
    let testing_docs = fs::read_to_string(root.join("docs/testing.md")).expect("read testing docs");

    for expected in [
        "cron: \"0 3 * * *\"",
        "cargo install cargo-fuzz --version 0.13.1 --locked",
        "FUZZ_TARGET_SECONDS: \"1800\"",
        "targets=(sdmx_json xls csv pdf_response)",
        "fuzz run \"${target}\"",
        "-max_total_time=${FUZZ_TARGET_SECONDS}",
        "-artifact_prefix=fuzz/artifacts/",
        "actions/upload-artifact@v4",
        "cargo-fuzz-artifacts",
        "retention-days: 30",
        "actions/github-script@v8",
        "release blocker",
        "bug",
        "type:test",
    ] {
        assert!(
            workflow.contains(expected),
            "nightly cargo-fuzz workflow should contain `{expected}`"
        );
    }

    for target in ["sdmx_json", "xls", "csv", "pdf_response"] {
        assert!(
            fuzz_manifest.contains(&format!("name = \"{target}\"")),
            "cargo-fuzz manifest should define target `{target}`"
        );
        assert!(
            root.join(format!("fuzz/fuzz_targets/{target}.rs"))
                .is_file(),
            "cargo-fuzz target `{target}` should have a harness file"
        );
    }

    assert!(
        root.join("tools/ci/seed_fuzz_corpora.py").is_file(),
        "issue #64 should provide a corpus seeding script"
    );
    assert!(
        ci_docs.contains("Nightly cargo-fuzz")
            && ci_docs.contains("30 minutes per target")
            && ci_docs.contains("cargo-fuzz-artifacts"),
        "CI docs should document the scheduled fuzzing workflow and retained artifacts"
    );
    assert!(
        testing_docs.contains("fuzz run sdmx_json")
            && testing_docs.contains("fuzz run xls")
            && testing_docs.contains("fuzz run csv")
            && testing_docs.contains("fuzz run pdf_response"),
        "testing docs should document local cargo-fuzz target execution"
    );
}

#[test]
fn issue_64_seed_script_materializes_parser_corpora() {
    let root = repo_root();
    let temp_dir = temp_fixture_dir("fuzz-corpus");
    let fuzz_dir = temp_dir.join("fuzz");

    let status = Command::new("python3")
        .current_dir(root)
        .arg("tools/ci/seed_fuzz_corpora.py")
        .arg("--repo-root")
        .arg(root)
        .arg("--fuzz-dir")
        .arg(&fuzz_dir)
        .status()
        .expect("run fuzz corpus seed script");
    assert!(status.success(), "fuzz corpus seed script should succeed");

    for seeded in [
        "corpus/sdmx_json/cpi_sdmx.json",
        "corpus/xls/a1_balance_sheet_weekly.xlsx",
        "corpus/xls/centralised.xlsx",
        "corpus/csv/g1_consumer_price_inflation.csv",
        "corpus/pdf_response/sidecar-response.json",
    ] {
        let path = fuzz_dir.join(seeded);
        assert!(path.is_file(), "expected seeded corpus file `{seeded}`");
        assert!(
            fs::metadata(&path).expect("read seeded metadata").len() > 0,
            "seeded corpus file `{seeded}` should not be empty"
        );
    }

    let pdf_response =
        fs::read_to_string(fuzz_dir.join("corpus/pdf_response/sidecar-response.json"))
            .expect("read pdf response corpus");
    assert!(pdf_response.contains("pdfplumber+camelot"));
    assert!(pdf_response.contains("artifacts/fixtures/bp4-agency-resourcing.pdf"));

    let _ = fs::remove_dir_all(temp_dir);
}

fn temp_fixture_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}
