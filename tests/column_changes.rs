//! Approved bd-zid first step: report every changed numeric column without changing the
//! existing global ranking, verdict, or metrics.

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const REDACTION_KEY: &str = "0123456789abcdef";

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "rvl-column-changes-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("scratch directory");
        Self(dir)
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, contents).expect("fixture");
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn run(old: &Path, new: &Path, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .env_remove("RVL_REDACTION_KEY")
        .arg(old)
        .arg(new)
        .arg("--key")
        .arg("id")
        .arg("--no-witness")
        .args(extra)
        .output()
        .expect("run rvl")
}

fn run_keyed(old: &Path, new: &Path, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .env("RVL_REDACTION_KEY", REDACTION_KEY)
        .arg(old)
        .arg(new)
        .arg("--key")
        .arg("id")
        .arg("--no-witness")
        .args(extra)
        .output()
        .expect("run keyed rvl")
}

fn json(output: &Output) -> Value {
    let parsed = serde_json::from_slice(&output.stdout);
    assert!(
        parsed.is_ok(),
        "stdout is not JSON: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    parsed.unwrap_or(Value::Null)
}

fn write_pair(scratch: &Scratch, header: &str, old_row: &str, new_row: &str) -> (PathBuf, PathBuf) {
    (
        scratch.write("old.csv", &format!("{header}\n{old_row}\n")),
        scratch.write("new.csv", &format!("{header}\n{new_row}\n")),
    )
}

#[test]
fn bead_example_lists_tail_rate_in_human_and_json_without_raw_defaults() {
    let scratch = Scratch::new("bead-example");
    let (old, new) = write_pair(
        &scratch,
        "id,balance,rate,tail",
        "A,1000000,5.0,1",
        "A,1000100,0.0,2",
    );

    let output = run(&old, &new, &["--json"]);
    assert_eq!(output.status.code(), Some(1));
    let report = json(&output);
    assert_eq!(report["outcome"], "REAL_CHANGE");
    assert_eq!(report["metrics"]["total_change"], 106.0);
    let columns = report["column_changes"].as_array().expect("column section");
    assert_eq!(columns.len(), 3);
    assert_eq!(
        columns
            .iter()
            .map(|entry| entry["changed_cells"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [1, 1, 1]
    );
    assert!(columns.iter().all(|entry| entry["max_abs_delta"].is_null()));
    let text = String::from_utf8_lossy(&output.stdout);
    for canary in ["balance", "rate", "tail", "1000000", "1000100", "5.0"] {
        assert!(
            !text.contains(canary),
            "default output leaked {canary}: {text}"
        );
    }

    let human = run(&old, &new, &[]);
    let human_text = String::from_utf8_lossy(&human.stdout);
    assert!(human_text.contains("Changed numeric columns (columns may use different units):"));
    assert!(human_text.contains("redacted:ref:c"));
    assert!(!human_text.contains("balance"));

    let explicit = json(&run(&old, &new, &["--json", "--explicit"]));
    let explicit_columns = explicit["column_changes"].as_array().unwrap();
    assert_eq!(explicit_columns[0]["column"], "u8:balance");
    assert_eq!(explicit_columns[0]["max_abs_delta"], 100.0);
    assert_eq!(explicit_columns[1]["column"], "u8:rate");
    assert_eq!(explicit_columns[1]["max_abs_delta"], 5.0);
    assert_eq!(explicit_columns[2]["column"], "u8:tail");
    assert_eq!(explicit_columns[2]["max_abs_delta"], 1.0);
}

#[test]
fn every_changed_column_is_listed_even_beyond_global_top_k() {
    let scratch = Scratch::new("many-columns");
    let mut header = String::from("id,big");
    let mut old = String::from("A,0");
    let mut new = String::from("A,1000");
    for index in 0..30 {
        header.push_str(&format!(",tail_{index:02}"));
        old.push_str(",0");
        new.push_str(",1");
    }
    let (old, new) = write_pair(&scratch, &header, &old, &new);
    let report = json(&run(&old, &new, &["--json"]));
    assert_eq!(report["outcome"], "REAL_CHANGE");
    assert_eq!(report["column_changes"].as_array().unwrap().len(), 31);
    assert!(report["contributors"].as_array().unwrap().len() < 31);
}

#[test]
fn tolerance_edges_count_only_deltas_above_tolerance() {
    let scratch = Scratch::new("tolerance");
    let (old, new) = write_pair(&scratch, "id,exact,over", "A,0,0", "A,1,2");

    let at_edge = json(&run(&old, &new, &["--json", "--tolerance", "1"]));
    assert_eq!(at_edge["outcome"], "REAL_CHANGE");
    assert_eq!(at_edge["column_changes"].as_array().unwrap().len(), 1);
    assert_eq!(at_edge["column_changes"][0]["changed_cells"], 1);

    let below_edge = json(&run(&old, &new, &["--json", "--tolerance", "0.999"]));
    assert_eq!(below_edge["column_changes"].as_array().unwrap().len(), 2);
}

#[test]
fn no_change_and_refusal_have_empty_column_sections() {
    let scratch = Scratch::new("empty");
    let (old, new) = write_pair(&scratch, "id,value", "A,1", "A,1");
    let no_change = run(&old, &new, &["--json"]);
    assert_eq!(no_change.status.code(), Some(0));
    assert!(
        json(&no_change)["column_changes"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let human = run(&old, &new, &[]);
    assert!(!String::from_utf8_lossy(&human.stdout).contains("Changed numeric columns"));

    let (bad_old, bad_new) = write_pair(&scratch, "id,value", "A,1", "A,not-a-number");
    let refusal = run(&bad_old, &bad_new, &["--json"]);
    assert_eq!(refusal.status.code(), Some(2));
    assert_eq!(json(&refusal)["outcome"], "REFUSAL");
    assert!(
        json(&refusal)["column_changes"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn non_numeric_and_profile_scopes_keep_numeric_column_section_scoped() {
    let scratch = Scratch::new("scopes");
    let (old, new) = write_pair(
        &scratch,
        "id,balance,rate,status",
        "A,10,5,open",
        "A,11,0,closed",
    );
    let combined = json(&run(&old, &new, &["--json", "--non-numeric"]));
    let combined_columns = combined["column_changes"].as_array().unwrap();
    assert_eq!(combined_columns.len(), 2);
    assert!(combined["non_numeric"].is_object());

    let profile = scratch.write("profile.yaml", "include_columns: [rate]\nkey: [id]\n");
    let profiled = Command::new(env!("CARGO_BIN_EXE_rvl"))
        .env_remove("RVL_REDACTION_KEY")
        .args([old.as_os_str(), new.as_os_str()])
        .args([
            "--profile",
            profile.to_str().unwrap(),
            "--json",
            "--no-witness",
            "--explicit",
        ])
        .output()
        .expect("run profiled rvl");
    let report = json(&profiled);
    assert_eq!(report["column_changes"].as_array().unwrap().len(), 1);
    assert_eq!(report["column_changes"][0]["column"], "u8:rate");
}

#[test]
fn keyed_mode_reuses_the_column_marker_in_contributors_and_section() {
    let scratch = Scratch::new("keyed");
    let (old, new) = write_pair(&scratch, "id,balance,rate", "A,100,5", "A,200,0");
    let report = json(&run_keyed(&old, &new, &["--json"]));
    let contributor_column = report["contributors"][0]["column"].as_str().unwrap();
    let column_marker = report["column_changes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["column"].as_str() == Some(contributor_column))
        .expect("contributor column in column section");
    assert!(contributor_column.starts_with("redacted:hmac-sha256:"));
    assert_eq!(column_marker["changed_cells"], 1);
}

#[test]
fn shared_tabular_inputs_emit_the_same_explicit_column_summary() {
    const EXTENSIONS: [&str; 5] = ["csv", "tsv", "jsonl", "parquet", "xlsx"];
    for extension in EXTENSIONS {
        let old = format!("tests/fixtures/formats/old.{extension}");
        let new = format!("tests/fixtures/formats/new.{extension}");
        let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
            .args([
                old.as_str(),
                new.as_str(),
                "--key",
                "loan_id",
                "--json",
                "--explicit",
                "--no-witness",
            ])
            .output()
            .expect("run shared format");
        let report = json(&output);
        assert_eq!(output.status.code(), Some(1), "{extension}");
        assert!(
            report["column_changes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["column"] == "u8:balance"),
            "{extension}: {report}"
        );
    }
}

#[test]
fn capsule_and_witness_carry_redacted_column_additions() {
    let scratch = Scratch::new("artifacts");
    let (old, new) = write_pair(&scratch, "id,balance,rate", "A,100,5", "A,200,0");
    let capsule_root = scratch.0.join("capsules");
    let witness = scratch.0.join("witness.jsonl");
    let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
        .env_remove("RVL_REDACTION_KEY")
        .env("EPISTEMIC_WITNESS", &witness)
        .arg(&old)
        .arg(&new)
        .args(["--key", "id", "--json", "--capsule-dir"])
        .arg(&capsule_root)
        .output()
        .expect("run artifact case");
    let report = json(&output);
    assert_eq!(output.status.code(), Some(1));
    assert!(report["column_changes"].is_array());

    let capsule_dir = std::fs::read_dir(&capsule_root)
        .expect("capsule root")
        .map(|entry| entry.expect("capsule entry").path())
        .next()
        .expect("one capsule");
    let manifest: Value = serde_json::from_str(
        &std::fs::read_to_string(capsule_dir.join("manifest.json")).expect("manifest"),
    )
    .expect("manifest json");
    let column_changes = manifest["column_changes"]
        .as_array()
        .expect("capsule columns");
    assert!(!column_changes.is_empty());
    assert!(column_changes.iter().all(|entry| {
        entry["column"]
            .as_str()
            .unwrap()
            .starts_with("redacted:ref:")
            && entry["max_abs_delta"].is_null()
    }));
    assert!(
        !serde_json::to_string(column_changes)
            .expect("column changes json")
            .contains("balance")
    );

    let witness_text = std::fs::read_to_string(witness).expect("witness");
    assert!(witness_text.contains("output_hash"));
    assert!(!witness_text.contains("balance"));
    let witness_record: Value = serde_json::from_str(witness_text.trim()).expect("witness json");
    let witness_columns = witness_record["params"]["column_changes"]
        .as_array()
        .expect("witness column changes");
    assert!(!witness_columns.is_empty());
    assert!(witness_columns.iter().all(|entry| {
        entry["column"]
            .as_str()
            .unwrap()
            .starts_with("redacted:ref:")
            && entry["max_abs_delta"].is_null()
    }));
}
