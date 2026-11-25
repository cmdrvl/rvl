//! bd-u1k: domain-agnostic non-numeric comparison coverage.

use std::path::PathBuf;
use std::process::{Command, Output};

use rvl::cli::args::Args;
use rvl::cli::exit::Outcome;
use rvl::orchestrator::{PipelineResult, ProfileRunInfo};
use rvl::witness::record::WitnessRecord;
use serde_json::Value;

fn rvl(old: &str, new: &str, extra: &[&str]) -> Output {
    let dir = tempfile_dir();
    let old_path = dir.join("old.csv");
    let new_path = dir.join("new.csv");
    std::fs::write(&old_path, old).expect("write old");
    std::fs::write(&new_path, new).expect("write new");
    let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
        .env_remove("RVL_REDACTION_KEY")
        .arg(&old_path)
        .arg(&new_path)
        .args(extra)
        .arg("--no-witness")
        .output()
        .expect("run rvl");
    std::fs::remove_dir_all(dir).expect("remove temp directory");
    output
}

fn rvl_with_profile(old: &str, new: &str, profile: &str, extra: &[&str]) -> Output {
    let dir = tempfile_dir();
    let old_path = dir.join("old.csv");
    let new_path = dir.join("new.csv");
    let profile_path = dir.join("profile.yaml");
    std::fs::write(&old_path, old).expect("write old");
    std::fs::write(&new_path, new).expect("write new");
    std::fs::write(&profile_path, profile).expect("write profile");
    let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
        .arg(&old_path)
        .arg(&new_path)
        .args(extra)
        .arg("--profile")
        .arg(&profile_path)
        .arg("--no-witness")
        .output()
        .expect("run rvl");
    std::fs::remove_dir_all(dir).expect("remove temp directory");
    output
}

fn tempfile_dir() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("rvl_non_numeric_{}_{}", std::process::id(), id));
    std::fs::create_dir_all(&path).expect("create temp directory");
    path
}

fn json(output: &Output) -> Value {
    let parsed = serde_json::from_slice(&output.stdout);
    assert!(
        parsed.is_ok(),
        "invalid JSON: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    parsed.unwrap_or(Value::Null)
}

fn validate_output(instance: &Value) {
    let schema_output = Command::new(env!("CARGO_BIN_EXE_rvl"))
        .arg("--schema")
        .output()
        .expect("run schema");
    assert!(schema_output.status.success());
    let schema: Value = serde_json::from_slice(&schema_output.stdout).expect("schema JSON");
    let validator = jsonschema::validator_for(&schema).expect("compile output schema");
    let errors = validator
        .iter_errors(instance)
        .map(|error| format!("{error} at {}", error.instance_path()))
        .collect::<Vec<_>>();
    assert!(
        errors.is_empty(),
        "non-numeric output did not validate: {errors:?}"
    );
}

#[test]
fn string_only_mode_reports_generic_transitions_and_redacts_them() {
    let old = "id,status,as_of,optional\nA,open,2024-01-01,\nB,closed,2024-01-03,x\nC,open,2024-01-05,y\n";
    let new = "id,status,as_of,optional\nA,closed,2024-01-03,z\nB,open,2024-01-05,\nC,closed,2024-01-07,y\n";
    let output = rvl(old, new, &["--key", "id", "--non-numeric", "--json"]);
    let value = json(&output);

    // Planted negative: before --non-numeric, this string-only comparison returned E_NO_NUMERIC.
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(value["outcome"], "REAL_CHANGE");
    assert_eq!(value["mode"], "non_numeric");
    assert_eq!(value["refusal"], Value::Null);
    assert_eq!(value["non_numeric"]["date_shifts"][0]["delta_days"], 2);
    assert_eq!(value["non_numeric"]["date_shifts"][0]["changed_rows"], 3);
    assert!(
        !value["non_numeric"]["categorical_transitions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        !value["non_numeric"]["nullity_flips"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        value["non_numeric"]["categorical_transitions"][0]["affected_rows"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row.as_str().unwrap().starts_with("redacted:ref:k"))
    );
    assert!(!serde_json::to_string(&value).unwrap().contains("blake3:"));

    let serialized = serde_json::to_string(&value).unwrap();
    assert!(!serialized.contains("status"));
    assert!(!serialized.contains("open"));
    assert!(!serialized.contains("2024-01-03"));
    assert!(serialized.contains("[REDACTED]"));

    let repeat = json(&rvl(old, new, &["--key", "id", "--non-numeric", "--json"]));
    assert_eq!(value["non_numeric"], repeat["non_numeric"]);

    let human = rvl(old, new, &["--key", "id", "--non-numeric"]);
    let human_stdout = String::from_utf8_lossy(&human.stdout);
    assert!(human_stdout.contains("[REDACTED]"));
    assert!(!human_stdout.contains("status"));
    assert!(!human_stdout.contains("open"));
}

#[test]
fn explicit_mode_reveals_encoded_non_numeric_identifiers() {
    let old = "id,status,as_of\nA,open,2024-01-01\n";
    let new = "id,status,as_of\nA,closed,2024-01-02\n";
    let output = rvl(
        old,
        new,
        &["--key", "id", "--non-numeric", "--explicit", "--json"],
    );
    let value = json(&output);
    assert_eq!(value["outcome"], "REAL_CHANGE");
    assert_eq!(value["non_numeric"]["date_shifts"][0]["column"], "u8:as_of");
    assert_eq!(
        value["non_numeric"]["categorical_transitions"][0]["old"],
        "u8:open"
    );
    assert_eq!(
        value["non_numeric"]["categorical_transitions"][0]["new"],
        "u8:closed"
    );
    assert_eq!(
        value["non_numeric"]["categorical_transitions"][0]["affected_rows"],
        serde_json::json!(["u8:A"])
    );
}

#[test]
fn high_cardinality_text_and_key_row_churn_are_structured() {
    let mut old = String::from("id,note\n");
    let mut new = String::from("id,note\n");
    for id in 0..40 {
        old.push_str(&format!("{id},old-note-{id}\n"));
    }
    for id in 1..=40 {
        new.push_str(&format!("{id},new-note-{id}\n"));
    }

    let output = rvl(
        &old,
        &new,
        &[
            "--key",
            "id",
            "--key-mode",
            "overlap",
            "--non-numeric",
            "--json",
        ],
    );
    let value = json(&output);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(value["outcome"], "REAL_CHANGE");
    assert_eq!(value["non_numeric"]["text_churn"][0]["changed_rows"], 39);
    assert_eq!(
        value["non_numeric"]["text_churn"][0]["byte_length_delta"],
        0
    );
    assert_eq!(value["non_numeric"]["row_churn"]["removed"], 1);
    assert_eq!(value["non_numeric"]["row_churn"]["added"], 1);
}

#[test]
fn unchanged_string_only_inputs_are_no_real_change() {
    let old = "id,status\nA,open\nB,closed\n";
    let output = rvl(old, old, &["--key", "id", "--non-numeric", "--json"]);
    let value = json(&output);
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(value["outcome"], "NO_REAL_CHANGE");
    assert_eq!(value["mode"], "non_numeric");
}

#[test]
fn non_numeric_json_validates_against_the_runtime_schema() {
    let old = "id,status\nA,open\n";
    let new = "id,status\nA,closed\n";
    let output = rvl(old, new, &["--key", "id", "--non-numeric", "--json"]);
    validate_output(&json(&output));
}

#[test]
fn witness_parameters_record_non_numeric_mode() {
    let mut args = Args::new(
        PathBuf::from("old.csv"),
        PathBuf::from("new.csv"),
        Some("id".to_string()),
        0.95,
        1e-9,
        None,
        true,
    );
    args.non_numeric = true;
    let result = PipelineResult {
        outcome: Outcome::RealChange,
        output: "{}".to_string(),
        profile: ProfileRunInfo::default(),
    };
    let witness = WitnessRecord::from_run(
        &args,
        &result,
        b"id,status\nA,open\n",
        b"id,status\nA,closed\n",
        "old.csv",
        "new.csv",
    );
    assert_eq!(witness.params["non_numeric"], true);
}

#[test]
fn profile_type_override_controls_non_numeric_classification() {
    let output = rvl_with_profile(
        "id,status\nA,open\n",
        "id,status\nA,closed\n",
        "include_columns: [id, status]\nkey: [id]\ncolumns:\n  status:\n    type: text\n",
        &["--non-numeric", "--json"],
    );
    let value = json(&output);
    assert_eq!(value["outcome"], "REAL_CHANGE");
    assert_eq!(
        value["non_numeric"]["categorical_transitions"],
        serde_json::json!([])
    );
    assert_eq!(value["non_numeric"]["text_churn"][0]["changed_rows"], 1);
}

#[test]
fn profile_date_override_accepts_excel_serials() {
    let output = rvl_with_profile(
        "id,as_of\nA,45292\n",
        "id,as_of\nA,45293\n",
        "include_columns: [id, as_of]\nkey: [id]\ncolumns:\n  as_of:\n    type: date\n",
        &["--non-numeric", "--json"],
    );
    let value = json(&output);
    assert_eq!(value["outcome"], "REAL_CHANGE");
    assert_eq!(value["non_numeric"]["date_shifts"][0]["delta_days"], 1);
}
