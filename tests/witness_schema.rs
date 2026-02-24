//! Schema validation tests for witness records (bd-3ju).

use std::path::PathBuf;

use rvl::cli::args::Args;
use rvl::cli::exit::Outcome;
use rvl::orchestrator::PipelineResult;
use rvl::witness::record::{WitnessRecord, canonical_json};

fn schema() -> serde_json::Value {
    let schema_str = include_str!("../docs/witness.v0.schema.json");
    serde_json::from_str(schema_str).expect("schema should be valid JSON")
}

fn validate(instance: &serde_json::Value) -> Result<(), String> {
    let schema_value = schema();
    let validator = jsonschema::validator_for(&schema_value).expect("schema should compile");
    let errors: Vec<String> = validator
        .iter_errors(instance)
        .map(|e| format!("{} at {}", e, e.instance_path()))
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

fn make_record(outcome: Outcome, prev: Option<String>) -> WitnessRecord {
    let args = Args::new(
        PathBuf::from("old.csv"),
        PathBuf::from("new.csv"),
        None,
        0.95,
        1e-9,
        None,
        false,
    );
    let result = PipelineResult {
        outcome,
        output: "test output".to_string(),
    };
    let mut rec = WitnessRecord::from_run(
        &args,
        &result,
        b"old content",
        b"new content",
        "old.csv",
        "new.csv",
        prev,
    );
    rec.ts = "2026-01-15T12:00:00Z".to_string();
    rec.compute_id();
    rec
}

// ── Schema validation passes for well-formed records ──────────────────

#[test]
fn schema_validates_no_real_change_record() {
    let rec = make_record(Outcome::NoRealChange, None);
    let json = canonical_json(&rec);
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    validate(&value).expect("NO_REAL_CHANGE record should validate");
}

#[test]
fn schema_validates_real_change_record() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    validate(&value).expect("REAL_CHANGE record should validate");
}

#[test]
fn schema_validates_refusal_record() {
    let rec = make_record(Outcome::Refusal, None);
    let json = canonical_json(&rec);
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    validate(&value).expect("REFUSAL record should validate");
}

#[test]
fn schema_validates_record_with_prev() {
    let first = make_record(Outcome::RealChange, None);
    let second = make_record(Outcome::NoRealChange, Some(first.id.clone()));
    let json = canonical_json(&second);
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    validate(&value).expect("record with prev should validate");
}

#[test]
fn schema_validates_record_with_all_params() {
    let args = Args::new(
        PathBuf::from("old.csv"),
        PathBuf::from("new.csv"),
        Some("account_id".to_string()),
        0.80,
        1e-6,
        Some(b'\t'),
        true,
    );
    let result = PipelineResult {
        outcome: Outcome::RealChange,
        output: "json output".to_string(),
    };
    let mut rec =
        WitnessRecord::from_run(&args, &result, b"old", b"new", "old.csv", "new.csv", None);
    rec.ts = "2026-01-15T12:00:00Z".to_string();
    rec.compute_id();
    let json = canonical_json(&rec);
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    validate(&value).expect("record with all params should validate");
}

// ── Schema validation fails for records with missing required fields ──

#[test]
fn schema_rejects_missing_id() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value.as_object_mut().unwrap().remove("id");
    assert!(validate(&value).is_err(), "missing id should fail");
}

#[test]
fn schema_rejects_missing_tool() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value.as_object_mut().unwrap().remove("tool");
    assert!(validate(&value).is_err(), "missing tool should fail");
}

#[test]
fn schema_rejects_missing_inputs() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value.as_object_mut().unwrap().remove("inputs");
    assert!(validate(&value).is_err(), "missing inputs should fail");
}

#[test]
fn schema_rejects_missing_ts() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value.as_object_mut().unwrap().remove("ts");
    assert!(validate(&value).is_err(), "missing ts should fail");
}

#[test]
fn schema_rejects_missing_prev() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value.as_object_mut().unwrap().remove("prev");
    assert!(validate(&value).is_err(), "missing prev should fail");
}

#[test]
fn schema_rejects_missing_outcome() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value.as_object_mut().unwrap().remove("outcome");
    assert!(validate(&value).is_err(), "missing outcome should fail");
}

// ── Schema validation fails for malformed blake3 hashes ───────────────

#[test]
fn schema_rejects_malformed_id_hash() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["id"] = serde_json::json!("not-a-blake3-hash");
    assert!(validate(&value).is_err(), "malformed id should fail");
}

#[test]
fn schema_rejects_short_id_hash() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["id"] = serde_json::json!("blake3:abcd");
    assert!(validate(&value).is_err(), "short id hash should fail");
}

#[test]
fn schema_rejects_malformed_binary_hash() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["binary_hash"] = serde_json::json!("sha256:abc123");
    assert!(
        validate(&value).is_err(),
        "non-blake3 binary_hash should fail"
    );
}

#[test]
fn schema_rejects_malformed_output_hash() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["output_hash"] = serde_json::json!("blake3:ZZZZ");
    assert!(
        validate(&value).is_err(),
        "malformed output_hash should fail"
    );
}

#[test]
fn schema_rejects_malformed_input_hash() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["inputs"][0]["hash"] = serde_json::json!("md5:abc");
    assert!(
        validate(&value).is_err(),
        "malformed input hash should fail"
    );
}

#[test]
fn schema_rejects_malformed_prev_hash() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["prev"] = serde_json::json!("blake3:tooshort");
    assert!(validate(&value).is_err(), "malformed prev should fail");
}

// ── Schema rejects type violations ────────────────────────────────────

#[test]
fn schema_rejects_empty_tool() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["tool"] = serde_json::json!("");
    assert!(validate(&value).is_err(), "empty tool should fail");
}

#[test]
fn schema_rejects_negative_exit_code() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["exit_code"] = serde_json::json!(-1);
    assert!(validate(&value).is_err(), "negative exit_code should fail");
}

#[test]
fn schema_rejects_empty_inputs_array() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["inputs"] = serde_json::json!([]);
    assert!(validate(&value).is_err(), "empty inputs array should fail");
}

#[test]
fn schema_rejects_negative_bytes() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["inputs"][0]["bytes"] = serde_json::json!(-1);
    assert!(validate(&value).is_err(), "negative bytes should fail");
}

#[test]
fn schema_rejects_additional_top_level_properties() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["extra_field"] = serde_json::json!("not allowed");
    assert!(
        validate(&value).is_err(),
        "additional top-level properties should fail"
    );
}

#[test]
fn schema_rejects_bad_ts_format() {
    let rec = make_record(Outcome::RealChange, None);
    let json = canonical_json(&rec);
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value["ts"] = serde_json::json!("2026/01/15 12:00:00");
    assert!(validate(&value).is_err(), "bad ts format should fail");
}

// ── Golden record round-trip ──────────────────────────────────────────

#[test]
fn golden_record_validates_against_schema() {
    let golden_str = include_str!("fixtures/witness/golden-record.json");
    let value: serde_json::Value =
        serde_json::from_str(golden_str).expect("golden record should be valid JSON");
    validate(&value).expect("golden record should validate against schema");
}

#[test]
fn golden_record_round_trips_without_mutation() {
    let golden_str = include_str!("fixtures/witness/golden-record.json");
    let value: serde_json::Value =
        serde_json::from_str(golden_str).expect("golden record should be valid JSON");

    // Deserialize into WitnessRecord, re-serialize to canonical JSON.
    let rec: WitnessRecord =
        serde_json::from_value(value.clone()).expect("should deserialize to WitnessRecord");
    let reserialized = canonical_json(&rec);
    let reserialized_value: serde_json::Value =
        serde_json::from_str(&reserialized).expect("reserialized should be valid JSON");

    assert_eq!(
        value, reserialized_value,
        "golden record should round-trip without mutation"
    );
}

#[test]
fn golden_record_id_is_verifiable() {
    let golden_str = include_str!("fixtures/witness/golden-record.json");
    let mut value: serde_json::Value =
        serde_json::from_str(golden_str).expect("golden record should be valid JSON");

    let stored_id = value["id"].as_str().unwrap().to_string();

    // Blank the id and re-hash to verify.
    value["id"] = serde_json::Value::String(String::new());
    let canonical = serde_json::to_string(&value).unwrap();
    let expected_id = format!("blake3:{}", blake3::hash(canonical.as_bytes()).to_hex());

    assert_eq!(
        stored_id, expected_id,
        "golden record id should be verifiable"
    );
}
