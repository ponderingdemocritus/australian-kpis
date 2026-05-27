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
fn emitted_parameters_reject_values_handlers_reject() {
    let doc = emitted_spec();

    let dataflows_params = doc["paths"]["/v1/dataflows"]["get"]["parameters"]
        .as_array()
        .expect("dataflows parameters");
    assert_parameter_schema(dataflows_params, "source", "minLength", 1);
    assert_parameter_schema(dataflows_params, "source", "maxLength", 128);
    assert_parameter_schema(dataflows_params, "frequency", "minLength", 1);

    let observations_params = doc["paths"]["/v1/observations"]["get"]["parameters"]
        .as_array()
        .expect("observations parameters");
    assert_parameter_schema(observations_params, "dataflow", "minLength", 1);
    assert_parameter_schema(observations_params, "dataflow", "maxLength", 128);
    assert_parameter_schema(
        observations_params,
        "format",
        "pattern",
        "^(json|csv|parquet)$",
    );

    let search_params = doc["paths"]["/v1/search"]["get"]["parameters"]
        .as_array()
        .expect("search parameters");
    assert_parameter_schema(search_params, "q", "minLength", 1);

    let series_params = doc["paths"]["/v1/series/{dataflow}/{series_key}"]["get"]["parameters"]
        .as_array()
        .expect("series parameters");
    assert_parameter_schema(series_params, "dataflow", "minLength", 1);
    assert_parameter_schema(series_params, "series_key", "minLength", 64);
    assert_parameter_schema(series_params, "series_key", "maxLength", 64);
    assert_parameter_schema(series_params, "series_key", "pattern", "^[0-9a-f]{64}$");
}

fn assert_parameter_schema<T>(parameters: &[Value], name: &str, key: &str, expected: T)
where
    T: Into<Value>,
{
    let parameter = parameters
        .iter()
        .find(|parameter| parameter["name"] == name)
        .unwrap_or_else(|| panic!("missing parameter `{name}`"));
    assert_eq!(parameter["schema"][key], expected.into(), "{name}.{key}");
}

#[test]
fn emitted_openapi_matches_snapshot() {
    insta::assert_json_snapshot!("openapi", emitted_spec());
}
