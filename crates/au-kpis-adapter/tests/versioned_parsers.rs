use au_kpis_adapter::{
    AdapterError, ArtifactDateRange, ExpectedSchemaHash, ParserVersion, SchemaHashDrift,
    select_parser_version, validate_schema_hash,
};
use au_kpis_domain::{DataflowId, SourceId};
use chrono::NaiveDate;

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).expect("fixture date is valid")
}

fn parse_v1() -> &'static str {
    "legacy parser"
}

fn parse_v2() -> &'static str {
    "current parser"
}

#[test]
fn selects_parse_v1_or_parse_v2_by_artifact_date_range() {
    let versions = [
        ParserVersion::new("parse_v1", ArtifactDateRange::before(date(2025, 7, 1))),
        ParserVersion::new("parse_v2", ArtifactDateRange::from(date(2025, 7, 1))),
    ];

    let legacy = select_parser_version(&versions, date(2024, 6, 30)).expect("select v1");
    let current = select_parser_version(&versions, date(2025, 7, 1)).expect("select v2");

    assert_eq!(legacy.name(), "parse_v1");
    assert_eq!(parse_v1(), "legacy parser");
    assert_eq!(current.name(), "parse_v2");
    assert_eq!(parse_v2(), "current parser");
}

#[test]
fn rejects_overlapping_parser_date_ranges() {
    let versions = [
        ParserVersion::new("parse_v1", ArtifactDateRange::from(date(2024, 1, 1))),
        ParserVersion::new("parse_v2", ArtifactDateRange::from(date(2025, 1, 1))),
    ];

    let err = select_parser_version(&versions, date(2025, 7, 1))
        .expect_err("overlapping ranges should be a config error");

    assert!(matches!(err, AdapterError::Validation(message) if message.contains("overlap")));
}

#[test]
fn reports_format_drift_when_no_parser_version_covers_artifact_date() {
    let versions = [ParserVersion::new(
        "parse_v2",
        ArtifactDateRange::from(date(2025, 7, 1)),
    )];

    let err = select_parser_version(&versions, date(2024, 6, 30))
        .expect_err("uncovered historical artifact date should fail");

    assert!(
        matches!(err, AdapterError::FormatDrift(message) if message.contains("no parser version"))
    );
}

#[test]
fn schema_hash_match_accepts_expected_source_shape() {
    let expected = ExpectedSchemaHash::new(
        SourceId::new("treasury").expect("valid source id"),
        DataflowId::new("treasury.budget_papers").expect("valid dataflow id"),
        "parse_v2",
        "bp4-agency-resourcing",
        "abc123",
    )
    .expect("valid expectation");

    validate_schema_hash(&expected, "abc123").expect("matching hash should pass");
}

#[test]
fn schema_hash_mismatch_returns_structured_drift_error() {
    let expected = ExpectedSchemaHash::new(
        SourceId::new("treasury").expect("valid source id"),
        DataflowId::new("treasury.budget_papers").expect("valid dataflow id"),
        "parse_v2",
        "bp4-agency-resourcing",
        "abc123",
    )
    .expect("valid expectation");

    let err = validate_schema_hash(&expected, "def456").expect_err("hash drift should fail");

    let AdapterError::SchemaHashDrift(drift) = err else {
        panic!("expected schema hash drift error");
    };

    assert_eq!(
        *drift,
        SchemaHashDrift {
            source_id: SourceId::new("treasury").expect("valid source id"),
            dataflow_id: DataflowId::new("treasury.budget_papers").expect("valid dataflow id"),
            parser_version: "parse_v2".to_string(),
            schema_key: "bp4-agency-resourcing".to_string(),
            expected_hash: "abc123".to_string(),
            actual_hash: "def456".to_string(),
        }
    );
}

#[test]
fn schema_hash_expectation_rejects_empty_fields() {
    let err = ExpectedSchemaHash::new(
        SourceId::new("treasury").expect("valid source id"),
        DataflowId::new("treasury.budget_papers").expect("valid dataflow id"),
        "",
        "bp4-agency-resourcing",
        "abc123",
    )
    .expect_err("empty parser version should fail");

    assert!(matches!(err, AdapterError::Validation(message) if message.contains("parser")));
}
