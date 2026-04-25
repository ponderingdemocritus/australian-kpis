use std::process::Command;

use assert_cmd::cargo::cargo_bin;
use au_kpis_openapi::{ApiDoc, emit};
use jsonschema::draft202012;
use serde_json::Value;
use utoipa::OpenApi;

fn emitted_spec() -> Value {
    serde_json::from_str(&emit().expect("emitted spec should be JSON")).expect("valid json")
}

#[test]
fn apidoc_derives_openapi() {
    let openapi = ApiDoc::openapi();
    let value = serde_json::to_value(&openapi).expect("serialize openapi");
    assert_eq!(value["openapi"], "3.1.0");
}

#[test]
fn emit_produces_document_valid_against_official_schema() {
    let schema: Value = serde_json::from_str(include_str!("fixtures/openapi-3.1-schema.json"))
        .expect("schema fixture should parse");
    let doc = emitted_spec();
    let validator = draft202012::new(&schema).expect("compile schema");
    let output = validator.validate(&doc);

    assert!(
        output.is_ok(),
        "expected emitted spec to validate, got {output:?}"
    );
    assert_eq!(doc["paths"]["/v1/health"]["get"]["operationId"], "health");
}

#[test]
fn cli_prints_same_document_as_emit() {
    let output = Command::new(cargo_bin("au-kpis-openapi"))
        .output()
        .expect("run au-kpis-openapi binary");
    assert!(
        output.status.success(),
        "binary should exit successfully: {output:?}"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    assert_eq!(stdout, emit().expect("emit should succeed"));
}

#[test]
fn emitted_openapi_matches_snapshot() {
    insta::assert_json_snapshot!("openapi", emitted_spec());
}
