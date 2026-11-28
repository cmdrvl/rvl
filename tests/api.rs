use std::fs;
use std::process::Command;

use rvl::{CompareOptions, KeyMode, Outcome, compare_bytes};

fn fixture(name: &str) -> String {
    format!(
        "{}/tests/fixtures/formats/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn cli_json(old: &str, new: &str, extra: &[&str]) -> (i32, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
        .args([old, new, "--json", "--no-witness"])
        .args(extra)
        .env_remove("RVL_REDACTION_KEY")
        .output()
        .expect("rvl CLI should execute");
    (
        output.status.code().expect("CLI should have an exit code"),
        String::from_utf8(output.stdout).expect("JSON output is UTF-8"),
    )
}

fn options(old: &str, new: &str) -> CompareOptions {
    let mut options = CompareOptions::default();
    options.old_name = old.to_owned();
    options.new_name = new.to_owned();
    options
}

#[test]
fn compare_bytes_matches_cli_json_for_numeric_and_non_numeric_reports() {
    let old = fixture("old.csv");
    let new = fixture("new.csv");
    let mut numeric = options(&old, &new);
    numeric.key = Some("loan_id".to_owned());
    let numeric_result =
        compare_bytes(&fs::read(&old).unwrap(), &fs::read(&new).unwrap(), &numeric).unwrap();
    let (exit, cli) = cli_json(&old, &new, &["--key", "loan_id"]);
    assert_eq!(exit, 1);
    assert_eq!(numeric_result.outcome, Outcome::RealChange);
    assert_eq!(format!("{}\n", numeric_result.output), cli);

    let old = b"id,status\nA,open\nB,closed\n";
    let new = b"id,status\nA,closed\nB,closed\n";
    let mut non_numeric = CompareOptions::default();
    non_numeric.old_name = "memory-old.csv".to_owned();
    non_numeric.new_name = "memory-new.csv".to_owned();
    non_numeric.key = Some("id".to_owned());
    non_numeric.non_numeric = true;
    non_numeric.json = true;
    let result = compare_bytes(old, new, &non_numeric).unwrap();
    assert_eq!(result.outcome, Outcome::RealChange);
    assert!(!result.output.contains("open"));
    assert!(!result.output.contains("closed"));
    assert!(result.output.contains("redacted:ref:"));
}

#[test]
fn compare_bytes_supports_overlap_alignment_without_filesystem_inputs() {
    let old = b"id,value\nA,1\nB,2\n";
    let new = b"id,value\nB,3\nC,4\n";
    let mut options = CompareOptions::default();
    options.old_name = "/does/not/exist/old.csv".to_owned();
    options.new_name = "/does/not/exist/new.csv".to_owned();
    options.key = Some("id".to_owned());
    options.key_mode = KeyMode::Overlap;
    options.json = true;

    let result = compare_bytes(old, new, &options).unwrap();
    assert_eq!(result.outcome, Outcome::RealChange);
    let report: serde_json::Value = serde_json::from_str(&result.output).unwrap();
    assert_eq!(report["alignment"]["key_membership"]["overlap_count"], 1);
    assert_eq!(report["alignment"]["key_membership"]["old_only_count"], 1);
    assert_eq!(report["alignment"]["key_membership"]["new_only_count"], 1);
}

#[test]
fn compare_bytes_uses_open_bytes_for_jsonl() {
    let old = fixture("old.jsonl");
    let new = fixture("new.jsonl");
    let mut options = options(&old, &new);
    options.key = Some("loan_id".to_owned());

    let result =
        compare_bytes(&fs::read(&old).unwrap(), &fs::read(&new).unwrap(), &options).unwrap();
    let (exit, cli) = cli_json(&old, &new, &["--key", "loan_id"]);
    assert_eq!(exit, 1);
    assert_eq!(format!("{}\n", result.output), cli);
}
