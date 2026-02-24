//! Integration tests for witness ledger recording (bd-2im).
//!
//! Tests exercise the compiled binary to verify that:
//! - Normal runs produce witness records
//! - `--no-witness` suppresses recording
//! - Consecutive runs produce hash-chained records
//! - Witness failures do not affect exit codes

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

fn rvl_binary() -> PathBuf {
    // Use the test binary's directory to find the compiled rvl binary.
    let mut path = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    path.push("rvl");
    path
}

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("rvl_test_witness_integ_{id}_{seq}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_csv(dir: &Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path
}

fn cleanup(dir: &Path) {
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn normal_run_creates_witness_record() {
    let dir = temp_dir();
    let old = write_csv(&dir, "old.csv", "id,value\nA,1\nB,2\n");
    let new = write_csv(&dir, "new.csv", "id,value\nA,1\nB,3\n");
    let ledger = dir.join("witness.jsonl");

    let output = Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl");

    assert!(
        output.status.success() || output.status.code() == Some(1),
        "rvl should exit 0 or 1, got {:?}",
        output.status.code()
    );

    assert!(ledger.exists(), "witness ledger should be created");
    let content = std::fs::read_to_string(&ledger).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 1, "should have exactly one witness record");

    let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert!(parsed["id"].as_str().unwrap().starts_with("blake3:"));
    assert_eq!(parsed["tool"], "rvl");
    assert!(parsed["prev"].is_null());

    cleanup(&dir);
}

#[test]
fn no_witness_flag_suppresses_recording() {
    let dir = temp_dir();
    let old = write_csv(&dir, "old.csv", "id,value\nA,1\nB,2\n");
    let new = write_csv(&dir, "new.csv", "id,value\nA,1\nB,3\n");
    let ledger = dir.join("witness.jsonl");

    let output = Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .arg("--no-witness")
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl");

    assert!(
        output.status.success() || output.status.code() == Some(1),
        "rvl should exit 0 or 1, got {:?}",
        output.status.code()
    );

    assert!(
        !ledger.exists(),
        "witness ledger should NOT be created when --no-witness is used"
    );

    cleanup(&dir);
}

#[test]
fn consecutive_runs_produce_hash_chain() {
    let dir = temp_dir();
    let old = write_csv(&dir, "old.csv", "id,value\nA,1\nB,2\n");
    let new = write_csv(&dir, "new.csv", "id,value\nA,1\nB,3\n");
    let ledger = dir.join("witness.jsonl");

    // Run 1.
    let output1 = Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl (run 1)");
    assert!(
        output1.status.success() || output1.status.code() == Some(1),
        "run 1 failed: {:?}",
        output1.status.code()
    );

    // Run 2.
    let output2 = Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl (run 2)");
    assert!(
        output2.status.success() || output2.status.code() == Some(1),
        "run 2 failed: {:?}",
        output2.status.code()
    );

    let content = std::fs::read_to_string(&ledger).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2, "should have two witness records");

    let rec1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    let rec2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();

    assert!(rec1["prev"].is_null(), "first record should have null prev");
    assert_eq!(
        rec2["prev"].as_str().unwrap(),
        rec1["id"].as_str().unwrap(),
        "second record's prev must chain to first record's id"
    );

    cleanup(&dir);
}

#[test]
fn witness_record_id_is_verifiable_from_ledger() {
    let dir = temp_dir();
    let old = write_csv(&dir, "old.csv", "id,value\nA,1\nB,2\n");
    let new = write_csv(&dir, "new.csv", "id,value\nA,1\nB,3\n");
    let ledger = dir.join("witness.jsonl");

    Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl");

    let content = std::fs::read_to_string(&ledger).unwrap();
    let line = content.lines().next().unwrap();

    let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
    let stored_id = value["id"].as_str().unwrap().to_string();

    // Blank the id and re-hash to verify.
    value["id"] = serde_json::Value::String(String::new());
    let canonical = serde_json::to_string(&value).unwrap();
    let expected_id = format!("blake3:{}", blake3::hash(canonical.as_bytes()).to_hex());

    assert_eq!(
        stored_id, expected_id,
        "ledger record should be hash-verifiable"
    );

    cleanup(&dir);
}

#[test]
fn witness_failure_does_not_affect_exit_code() {
    let dir = temp_dir();
    let old = write_csv(&dir, "old.csv", "id,value\nA,1\nB,2\n");
    let new = write_csv(&dir, "new.csv", "id,value\nA,1\nB,3\n");

    // Point EPISTEMIC_WITNESS to an impossible path.
    let output = Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .env("EPISTEMIC_WITNESS", "/dev/null/impossible/witness.jsonl")
        .output()
        .expect("failed to run rvl");

    // The exit code should reflect the comparison result, not the witness failure.
    assert!(
        output.status.code() == Some(0) || output.status.code() == Some(1),
        "exit code should be 0 or 1 (comparison result), got {:?}",
        output.status.code()
    );

    // Stderr should mention the witness failure.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("witness"),
        "stderr should mention witness failure: {stderr}"
    );

    cleanup(&dir);
}

#[test]
fn witness_records_have_correct_input_paths() {
    let dir = temp_dir();
    let old = write_csv(&dir, "old.csv", "id,value\nA,1\nB,2\n");
    let new = write_csv(&dir, "new.csv", "id,value\nA,1\nB,3\n");
    let ledger = dir.join("witness.jsonl");

    Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl");

    let content = std::fs::read_to_string(&ledger).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
    let inputs = parsed["inputs"].as_array().unwrap();

    assert_eq!(inputs[0]["path"], old.to_str().unwrap());
    assert_eq!(inputs[1]["path"], new.to_str().unwrap());

    cleanup(&dir);
}

#[test]
fn witness_records_capture_file_sizes() {
    let dir = temp_dir();
    let old_content = "id,value\nA,1\nB,2\n";
    let new_content = "id,value\nA,1\nB,3\n";
    let old = write_csv(&dir, "old.csv", old_content);
    let new = write_csv(&dir, "new.csv", new_content);
    let ledger = dir.join("witness.jsonl");

    Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl");

    let content = std::fs::read_to_string(&ledger).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
    let inputs = parsed["inputs"].as_array().unwrap();

    assert_eq!(inputs[0]["bytes"], old_content.len() as u64);
    assert_eq!(inputs[1]["bytes"], new_content.len() as u64);

    cleanup(&dir);
}

#[test]
fn no_real_change_produces_exit_zero_with_witness() {
    let dir = temp_dir();
    // Identical files → NO_REAL_CHANGE → exit 0.
    let old = write_csv(&dir, "old.csv", "id,value\nA,1\nB,2\n");
    let new = write_csv(&dir, "new.csv", "id,value\nA,1\nB,2\n");
    let ledger = dir.join("witness.jsonl");

    let output = Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl");

    assert_eq!(
        output.status.code(),
        Some(0),
        "identical files should exit 0"
    );

    let content = std::fs::read_to_string(&ledger).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
    assert_eq!(parsed["outcome"], "NO_REAL_CHANGE");
    assert_eq!(parsed["exit_code"], 0);

    cleanup(&dir);
}

#[test]
fn real_change_produces_exit_one_with_witness() {
    let dir = temp_dir();
    let old = write_csv(&dir, "old.csv", "id,value\nA,1\nB,2\n");
    let new = write_csv(&dir, "new.csv", "id,value\nA,1\nB,999\n");
    let ledger = dir.join("witness.jsonl");

    let output = Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl");

    assert_eq!(output.status.code(), Some(1), "changed files should exit 1");

    let content = std::fs::read_to_string(&ledger).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
    assert_eq!(parsed["outcome"], "REAL_CHANGE");
    assert_eq!(parsed["exit_code"], 1);

    cleanup(&dir);
}
