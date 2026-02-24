//! Integration tests for witness query subcommands (bd-ogo).

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

fn rvl_binary() -> PathBuf {
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
    let dir = std::env::temp_dir().join(format!("rvl_test_wq_{id}_{seq}"));
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

/// Populate a ledger with records by running rvl against real CSV files.
fn populate_ledger(dir: &Path, ledger: &Path, runs: usize) {
    let old = write_csv(dir, "old.csv", "id,value\nA,1\nB,2\n");
    let new = write_csv(dir, "new.csv", "id,value\nA,1\nB,999\n");
    for _ in 0..runs {
        let output = Command::new(rvl_binary())
            .arg(old.to_str().unwrap())
            .arg(new.to_str().unwrap())
            .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
            .output()
            .expect("failed to run rvl");
        assert!(
            output.status.code() == Some(0) || output.status.code() == Some(1),
            "populate run failed: {:?}",
            output.status.code()
        );
    }
}

/// Write a raw JSONL ledger for controlled testing (with known timestamps etc).
fn write_raw_ledger(ledger: &Path, records: &[&str]) {
    std::fs::create_dir_all(ledger.parent().unwrap()).unwrap();
    let mut file = std::fs::File::create(ledger).unwrap();
    for record in records {
        writeln!(file, "{record}").unwrap();
    }
}

fn make_raw_record(outcome: &str, ts: &str) -> String {
    format!(
        r#"{{"binary_hash":"blake3:test","exit_code":{},"id":"blake3:fake","inputs":[{{"bytes":10,"hash":"blake3:aaa","path":"old.csv"}},{{"bytes":10,"hash":"blake3:bbb","path":"new.csv"}}],"outcome":"{outcome}","output_hash":"blake3:out","params":{{"delimiter":null,"json":false,"key":null,"threshold":0.95,"tolerance":1e-9}},"prev":null,"tool":"rvl","ts":"{ts}","version":"0.1.1"}}"#,
        match outcome {
            "REAL_CHANGE" => 1,
            "REFUSAL" => 2,
            _ => 0,
        },
    )
}

// --- witness last ---

#[test]
fn witness_last_shows_most_recent_record() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    populate_ledger(&dir, &ledger, 2);

    let output = Command::new(rvl_binary())
        .args(["witness", "last"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness last");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("id:"),
        "should contain human-formatted output"
    );
    assert!(stdout.contains("outcome:"));

    cleanup(&dir);
}

#[test]
fn witness_last_json() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    populate_ledger(&dir, &ledger, 1);

    let output = Command::new(rvl_binary())
        .args(["witness", "last", "--json"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness last --json");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(parsed["tool"], "rvl");

    cleanup(&dir);
}

#[test]
fn witness_last_empty_ledger_exits_1() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    std::fs::create_dir_all(ledger.parent().unwrap()).unwrap();
    std::fs::write(&ledger, "").unwrap();

    let output = Command::new(rvl_binary())
        .args(["witness", "last"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness last");

    assert_eq!(output.status.code(), Some(1));

    cleanup(&dir);
}

// --- witness query ---

#[test]
fn witness_query_returns_records() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    populate_ledger(&dir, &ledger, 3);

    let output = Command::new(rvl_binary())
        .args(["witness", "query"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness query");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("---"),
        "multiple records should be separated"
    );

    cleanup(&dir);
}

#[test]
fn witness_query_json() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    populate_ledger(&dir, &ledger, 2);

    let output = Command::new(rvl_binary())
        .args(["witness", "query", "--json"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness query --json");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert!(parsed.is_array());
    assert_eq!(parsed.as_array().unwrap().len(), 2);

    cleanup(&dir);
}

#[test]
fn witness_query_outcome_filter() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    write_raw_ledger(
        &ledger,
        &[
            &make_raw_record("REAL_CHANGE", "2026-01-01T00:00:00Z"),
            &make_raw_record("NO_REAL_CHANGE", "2026-01-02T00:00:00Z"),
            &make_raw_record("REAL_CHANGE", "2026-01-03T00:00:00Z"),
        ],
    );

    let output = Command::new(rvl_binary())
        .args(["witness", "query", "--outcome", "REAL_CHANGE", "--json"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness query --outcome");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    for rec in arr {
        assert_eq!(rec["outcome"], "REAL_CHANGE");
    }

    cleanup(&dir);
}

#[test]
fn witness_query_since_filter() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    write_raw_ledger(
        &ledger,
        &[
            &make_raw_record("REAL_CHANGE", "2026-01-01T00:00:00Z"),
            &make_raw_record("REAL_CHANGE", "2026-06-15T00:00:00Z"),
        ],
    );

    let output = Command::new(rvl_binary())
        .args([
            "witness",
            "query",
            "--since",
            "2026-06-01T00:00:00Z",
            "--json",
        ])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness query --since");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(parsed.as_array().unwrap().len(), 1);

    cleanup(&dir);
}

#[test]
fn witness_query_limit() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    write_raw_ledger(
        &ledger,
        &[
            &make_raw_record("REAL_CHANGE", "2026-01-01T00:00:00Z"),
            &make_raw_record("REAL_CHANGE", "2026-01-02T00:00:00Z"),
            &make_raw_record("REAL_CHANGE", "2026-01-03T00:00:00Z"),
        ],
    );

    let output = Command::new(rvl_binary())
        .args(["witness", "query", "--limit", "2", "--json"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness query --limit");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(parsed.as_array().unwrap().len(), 2);

    cleanup(&dir);
}

#[test]
fn witness_query_empty_ledger_exits_1() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    std::fs::create_dir_all(ledger.parent().unwrap()).unwrap();
    std::fs::write(&ledger, "").unwrap();

    let output = Command::new(rvl_binary())
        .args(["witness", "query"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness query");

    assert_eq!(output.status.code(), Some(1));

    cleanup(&dir);
}

// --- witness count ---

#[test]
fn witness_count_returns_total() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    write_raw_ledger(
        &ledger,
        &[
            &make_raw_record("REAL_CHANGE", "2026-01-01T00:00:00Z"),
            &make_raw_record("NO_REAL_CHANGE", "2026-01-02T00:00:00Z"),
            &make_raw_record("REAL_CHANGE", "2026-01-03T00:00:00Z"),
        ],
    );

    let output = Command::new(rvl_binary())
        .args(["witness", "count"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness count");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.trim(), "3");

    cleanup(&dir);
}

#[test]
fn witness_count_with_outcome_filter() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    write_raw_ledger(
        &ledger,
        &[
            &make_raw_record("REAL_CHANGE", "2026-01-01T00:00:00Z"),
            &make_raw_record("NO_REAL_CHANGE", "2026-01-02T00:00:00Z"),
            &make_raw_record("REAL_CHANGE", "2026-01-03T00:00:00Z"),
        ],
    );

    let output = Command::new(rvl_binary())
        .args(["witness", "count", "--outcome", "REAL_CHANGE"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness count --outcome");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.trim(), "2");

    cleanup(&dir);
}

#[test]
fn witness_count_json() {
    let dir = temp_dir();
    let ledger = dir.join("witness.jsonl");
    write_raw_ledger(
        &ledger,
        &[
            &make_raw_record("REAL_CHANGE", "2026-01-01T00:00:00Z"),
            &make_raw_record("REAL_CHANGE", "2026-01-02T00:00:00Z"),
        ],
    );

    let output = Command::new(rvl_binary())
        .args(["witness", "count", "--json"])
        .env("EPISTEMIC_WITNESS", ledger.to_str().unwrap())
        .output()
        .expect("failed to run rvl witness count --json");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(parsed["count"], 2);

    cleanup(&dir);
}

// --- regression: comparison still works ---

#[test]
fn comparison_mode_still_works() {
    let dir = temp_dir();
    let old = write_csv(&dir, "old.csv", "id,value\nA,1\nB,2\n");
    let new = write_csv(&dir, "new.csv", "id,value\nA,1\nB,999\n");

    let output = Command::new(rvl_binary())
        .arg(old.to_str().unwrap())
        .arg(new.to_str().unwrap())
        .arg("--no-witness")
        .output()
        .expect("failed to run rvl comparison");

    assert_eq!(
        output.status.code(),
        Some(1),
        "comparison with changes should exit 1"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("REAL CHANGE"),
        "should show REAL CHANGE output"
    );

    cleanup(&dir);
}
