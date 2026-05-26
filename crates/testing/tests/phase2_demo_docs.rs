use std::{fs, path::Path};

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("testing crate lives under crates/testing")
}

#[test]
fn phase_2_demo_docs_match_issue_40_contract() {
    let root = repo_root();
    let demo_path = root.join("docs/demos/phase-2.md");
    let recording_path = root.join("docs/demos/phase-2.cast");
    let seed_path = root.join("docs/demos/phase-2-seed.sql");

    assert!(
        demo_path.is_file(),
        "issue #40 should add docs/demos/phase-2.md"
    );
    assert!(
        recording_path.is_file(),
        "issue #40 should include an asciinema recording"
    );
    assert!(
        seed_path.is_file(),
        "issue #40 should include reproducible ABS CPI seed data for a clean clone"
    );

    let demo = fs::read_to_string(demo_path).expect("read phase 2 demo doc");
    for expected in [
        "Setup the local stack",
        "Ingest ABS CPI",
        "Query the API and SDK",
        "Chart in the Explorer",
        "under 15 minutes",
        "phase-2.cast",
        "phase-2-seed.sql",
    ] {
        assert!(
            demo.contains(expected),
            "phase 2 demo should document `{expected}`"
        );
    }

    let recording = fs::read_to_string(recording_path).expect("read phase 2 demo recording");
    assert!(
        recording.starts_with(r#"{"version":2,"#),
        "asciinema recording should use v2 JSON-lines format"
    );
    assert!(
        recording.contains("au-kpis-ingestion -- --once --source abs --dataflow cpi"),
        "recording should demonstrate the ABS CPI ingestion command"
    );

    let seed = fs::read_to_string(seed_path).expect("read phase 2 demo seed SQL");
    for expected in [
        "INSERT INTO sources",
        "INSERT INTO dataflows",
        "INSERT INTO dimensions",
        "ON CONFLICT",
    ] {
        assert!(
            seed.contains(expected),
            "seed SQL should provide idempotent reference data with `{expected}`"
        );
    }

    let readme = fs::read_to_string(root.join("README.md")).expect("read README");
    assert!(
        readme.contains("## Getting started") && readme.contains("docs/demos/phase-2.md"),
        "README Getting started should link to the Phase 2 demo"
    );
}
