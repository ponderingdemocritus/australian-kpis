use std::{fs, path::Path};

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("testing crate lives under crates/testing")
}

#[test]
fn issue_60_data_quality_scaffold_is_wired() {
    let root = repo_root();

    for path in [
        "crates/bins/au-kpis-scheduler/src/data_quality.rs",
        "crates/bins/au-kpis-scheduler/src/lib.rs",
        ".github/workflows/data-quality.yml",
        "docs/data-quality.md",
    ] {
        assert!(
            root.join(path).is_file(),
            "issue #60 should provide `{path}`"
        );
    }

    let scheduler = fs::read_to_string(root.join("crates/bins/au-kpis-scheduler/src/main.rs"))
        .expect("read scheduler main");
    for expected in [
        "DataQuality",
        "run_data_quality_command",
        "AU_KPIS_PAGERDUTY_ROUTING_KEY",
        "AU_KPIS_DATA_QUALITY_REPORT_PATH",
    ] {
        assert!(
            scheduler.contains(expected),
            "scheduler should expose data-quality execution via `{expected}`"
        );
    }

    let data_quality =
        fs::read_to_string(root.join("crates/bins/au-kpis-scheduler/src/data_quality.rs"))
            .expect("read data quality module");
    for expected in [
        "abs.cpi",
        "rba.statistical_tables",
        "DataQualityRule",
        "cardinality",
        "recency",
        "plausible_range",
        "revision_volume",
        "PagerDuty",
    ] {
        assert!(
            data_quality.contains(expected),
            "data-quality module should implement `{expected}`"
        );
    }

    let workflow =
        fs::read_to_string(root.join(".github/workflows/data-quality.yml")).expect("read workflow");
    for expected in [
        "name: Data Quality",
        "0 * * * *",
        "data-quality",
        "PAGERDUTY_ROUTING_KEY",
        "data-quality-report",
        "actions/upload-artifact",
    ] {
        assert!(
            workflow.contains(expected),
            "data-quality workflow should contain `{expected}`"
        );
    }

    let docs =
        fs::read_to_string(root.join("docs/data-quality.md")).expect("read data quality docs");
    for expected in [
        "Per-dataflow rules",
        "plausible range",
        "cardinality",
        "recency",
        "PagerDuty",
        "daily report",
    ] {
        assert!(
            docs.contains(expected),
            "data-quality docs should document `{expected}`"
        );
    }
}
