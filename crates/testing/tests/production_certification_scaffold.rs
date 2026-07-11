use std::{fs, os::unix::fs::PermissionsExt, path::Path};

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("testing crate lives under crates/testing")
}

#[test]
fn production_v1_certification_contract_is_wired() {
    let root = root();
    for path in [
        "tools/release/build-scale-report.sh",
        "tools/release/verify-restore.sh",
        "tools/observability/drill-all-alerts.sh",
        "tools/release/build-security-report.sh",
        "tools/release/build-soak-report.sh",
    ] {
        let path = root.join(path);
        assert!(path.is_file(), "missing evidence tool {}", path.display());
        assert_ne!(fs::metadata(&path).unwrap().permissions().mode() & 0o111, 0);
    }
    for artifact in [
        "release-scale-report",
        "release-restore-report",
        "release-chaos-report",
        "release-security-report",
        "release-soak-report",
    ] {
        assert!(
            tree_text(root, &[".github/workflows", "tools", "docs"]).contains(artifact),
            "certification wiring should retain `{artifact}`"
        );
    }
    for path in [
        "docs/runbooks/on-call.md",
        "docs/runbooks/deploy-rollback.md",
        "docs/runbooks/source-pause-replay.md",
        "docs/runbooks/webhook-dlq.md",
        "docs/runbooks/database-object-restore.md",
    ] {
        assert!(root.join(path).is_file(), "missing runbook `{path}`");
    }
}

#[test]
fn scale_seed_and_operator_controls_are_real_commands() {
    let root = root();
    let seed = fs::read_to_string(root.join("crates/testing/src/bin/au-kpis-scale-seed.rs"))
        .expect("read scale seed");
    for expected in [
        "50_000_000",
        "REQUIRED_ACTIVE_DATAFLOWS",
        "REVISION_INTERVAL",
        "cadence_seconds",
        "chunks_compressed",
        "dataset_digest",
    ] {
        assert!(
            seed.contains(expected),
            "scale seed should contain `{expected}`"
        );
    }

    let cli = fs::read_to_string(root.join("crates/bins/au-kpis-cli/src/main.rs"))
        .expect("read admin CLI");
    for expected in [
        "SourceCommand::Pause",
        "SourceCommand::Resume",
        "RetryDlq",
        "ArtifactCommand::Reparse",
        "GenerationCommand::Inspect",
        "ManualInputCommand::Load",
    ] {
        assert!(
            cli.contains(expected),
            "admin CLI should contain `{expected}`"
        );
    }
}

fn tree_text(root: &Path, paths: &[&str]) -> String {
    let mut output = String::new();
    for relative in paths {
        collect(root.join(relative).as_path(), &mut output);
    }
    output
}

fn collect(path: &Path, output: &mut String) {
    if path.is_file() {
        output.push_str(&fs::read_to_string(path).unwrap_or_default());
        return;
    }
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            collect(&entry.path(), output);
        }
    }
}
