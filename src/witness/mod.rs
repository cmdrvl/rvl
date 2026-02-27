pub mod hash;
pub mod ledger;
pub mod query;
pub mod reader;
pub mod record;

use crate::cli::args::Args;
use crate::orchestrator::PipelineResult;

/// Build and append a witness record for a completed rvl run.
///
/// Re-reads both input files to hash their contents. All errors are
/// swallowed and logged to stderr — witness recording must never
/// affect the tool's primary exit code or output.
pub fn record_run(args: &Args, result: &PipelineResult) {
    let writer = match ledger::LedgerWriter::open() {
        Ok(w) => w,
        Err(e) => {
            eprintln!("rvl: witness: {e}");
            return;
        }
    };
    if let Err(e) = record_run_with_writer(args, result, &writer) {
        eprintln!("rvl: witness: {e}");
    }
}

/// Core implementation: build and append a witness record using an
/// explicit `LedgerWriter`. Separated from `record_run()` so that
/// tests can provide a `with_path()` writer without env var manipulation.
fn record_run_with_writer(
    args: &Args,
    result: &PipelineResult,
    writer: &ledger::LedgerWriter,
) -> Result<(), Box<dyn std::error::Error>> {
    let old_bytes = std::fs::read(args.old_path())?;
    let new_bytes = std::fs::read(args.new_path())?;

    let old_path = args.old_path().to_string_lossy().to_string();
    let new_path = args.new_path().to_string_lossy().to_string();

    let prev = writer.read_prev();

    let mut rec = record::WitnessRecord::from_run(
        args, result, &old_bytes, &new_bytes, &old_path, &new_path, prev,
    );
    rec.compute_id();
    writer.append(&rec)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::exit::Outcome;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("rvl_test_witness_mod_{id}_{seq}"))
    }

    fn write_csv(dir: &PathBuf, name: &str, content: &str) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    fn make_args(old: PathBuf, new: PathBuf) -> Args {
        Args::new(old, new, None, 0.95, 1e-9, None, false)
    }

    fn make_result(outcome: Outcome) -> PipelineResult {
        PipelineResult {
            outcome,
            output: "test output".to_string(),
            profile: crate::orchestrator::ProfileRunInfo::default(),
        }
    }

    fn cleanup(dir: &PathBuf) {
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn record_run_writes_valid_witness_record() {
        let dir = temp_dir();
        let old = write_csv(&dir, "old.csv", "id,value\nA,1\n");
        let new = write_csv(&dir, "new.csv", "id,value\nA,2\n");
        let ledger_path = dir.join("witness.jsonl");
        let writer = ledger::LedgerWriter::with_path(ledger_path.clone());

        let args = make_args(old, new);
        let result = make_result(Outcome::RealChange);
        record_run_with_writer(&args, &result, &writer).unwrap();

        let content = std::fs::read_to_string(&ledger_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1, "should produce exactly one ledger line");

        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert!(parsed["id"].as_str().unwrap().starts_with("blake3:"));
        assert_eq!(parsed["tool"], "rvl");
        assert_eq!(parsed["outcome"], "REAL_CHANGE");
        assert_eq!(parsed["exit_code"], 1);
        assert!(parsed["prev"].is_null());

        cleanup(&dir);
    }

    #[test]
    fn record_run_captures_correct_file_hashes() {
        let dir = temp_dir();
        let old_content = "id,value\nA,100\n";
        let new_content = "id,value\nA,200\n";
        let old = write_csv(&dir, "old.csv", old_content);
        let new = write_csv(&dir, "new.csv", new_content);
        let ledger_path = dir.join("witness.jsonl");
        let writer = ledger::LedgerWriter::with_path(ledger_path.clone());

        let args = make_args(old, new);
        let result = make_result(Outcome::RealChange);
        record_run_with_writer(&args, &result, &writer).unwrap();

        let content = std::fs::read_to_string(&ledger_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        let inputs = parsed["inputs"].as_array().unwrap();
        assert_eq!(inputs.len(), 2);

        // Verify old file hash matches direct hash.
        let expected_old_hash = format!("blake3:{}", hash::hash_bytes(old_content.as_bytes()));
        assert_eq!(inputs[0]["hash"], expected_old_hash);
        assert_eq!(inputs[0]["bytes"], old_content.len() as u64);

        // Verify new file hash matches direct hash.
        let expected_new_hash = format!("blake3:{}", hash::hash_bytes(new_content.as_bytes()));
        assert_eq!(inputs[1]["hash"], expected_new_hash);
        assert_eq!(inputs[1]["bytes"], new_content.len() as u64);

        cleanup(&dir);
    }

    #[test]
    fn record_run_captures_correct_paths() {
        let dir = temp_dir();
        let old = write_csv(&dir, "old.csv", "id,value\nA,1\n");
        let new = write_csv(&dir, "new.csv", "id,value\nA,2\n");
        let ledger_path = dir.join("witness.jsonl");
        let writer = ledger::LedgerWriter::with_path(ledger_path.clone());

        let args = make_args(old.clone(), new.clone());
        let result = make_result(Outcome::NoRealChange);
        record_run_with_writer(&args, &result, &writer).unwrap();

        let content = std::fs::read_to_string(&ledger_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        let inputs = parsed["inputs"].as_array().unwrap();
        assert_eq!(inputs[0]["path"], old.to_string_lossy().to_string());
        assert_eq!(inputs[1]["path"], new.to_string_lossy().to_string());

        cleanup(&dir);
    }

    #[test]
    fn record_run_chains_prev_on_consecutive_calls() {
        let dir = temp_dir();
        let old = write_csv(&dir, "old.csv", "id,value\nA,1\n");
        let new = write_csv(&dir, "new.csv", "id,value\nA,2\n");
        let ledger_path = dir.join("witness.jsonl");
        let writer = ledger::LedgerWriter::with_path(ledger_path.clone());

        let args = make_args(old, new);

        // First run.
        let result1 = make_result(Outcome::RealChange);
        record_run_with_writer(&args, &result1, &writer).unwrap();

        // Second run.
        let result2 = make_result(Outcome::NoRealChange);
        record_run_with_writer(&args, &result2, &writer).unwrap();

        let content = std::fs::read_to_string(&ledger_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);

        let rec1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let rec2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();

        assert!(rec1["prev"].is_null(), "first record should have null prev");
        assert_eq!(
            rec2["prev"].as_str().unwrap(),
            rec1["id"].as_str().unwrap(),
            "second record's prev should equal first record's id"
        );

        cleanup(&dir);
    }

    #[test]
    fn record_run_three_record_chain() {
        let dir = temp_dir();
        let old = write_csv(&dir, "old.csv", "id,value\nA,1\n");
        let new = write_csv(&dir, "new.csv", "id,value\nA,2\n");
        let ledger_path = dir.join("witness.jsonl");
        let writer = ledger::LedgerWriter::with_path(ledger_path.clone());

        let args = make_args(old, new);

        for outcome in [Outcome::RealChange, Outcome::NoRealChange, Outcome::Refusal] {
            let result = make_result(outcome);
            record_run_with_writer(&args, &result, &writer).unwrap();
        }

        let content = std::fs::read_to_string(&ledger_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);

        let r1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let r2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        let r3: serde_json::Value = serde_json::from_str(lines[2]).unwrap();

        assert!(r1["prev"].is_null());
        assert_eq!(r2["prev"].as_str().unwrap(), r1["id"].as_str().unwrap());
        assert_eq!(r3["prev"].as_str().unwrap(), r2["id"].as_str().unwrap());

        // Verify outcomes.
        assert_eq!(r1["outcome"], "REAL_CHANGE");
        assert_eq!(r2["outcome"], "NO_REAL_CHANGE");
        assert_eq!(r3["outcome"], "REFUSAL");

        cleanup(&dir);
    }

    #[test]
    fn record_run_id_is_verifiable() {
        let dir = temp_dir();
        let old = write_csv(&dir, "old.csv", "id,value\nA,1\n");
        let new = write_csv(&dir, "new.csv", "id,value\nA,2\n");
        let ledger_path = dir.join("witness.jsonl");
        let writer = ledger::LedgerWriter::with_path(ledger_path.clone());

        let args = make_args(old, new);
        let result = make_result(Outcome::RealChange);
        record_run_with_writer(&args, &result, &writer).unwrap();

        let content = std::fs::read_to_string(&ledger_path).unwrap();
        let line = content.lines().next().unwrap();

        // Verify: blank the id, hash the canonical JSON, compare to stored id.
        let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
        let stored_id = value["id"].as_str().unwrap().to_string();

        value["id"] = serde_json::Value::String(String::new());
        let canonical = serde_json::to_string(&value).unwrap();
        let expected_id = format!("blake3:{}", hash::hash_bytes(canonical.as_bytes()));

        assert_eq!(stored_id, expected_id, "record id should be verifiable");

        cleanup(&dir);
    }

    #[test]
    fn record_run_with_missing_old_file_returns_error() {
        let dir = temp_dir();
        let new = write_csv(&dir, "new.csv", "id,value\nA,1\n");
        let ledger_path = dir.join("witness.jsonl");
        let writer = ledger::LedgerWriter::with_path(ledger_path);

        let args = make_args(dir.join("nonexistent.csv"), new);
        let result = make_result(Outcome::Refusal);

        let err = record_run_with_writer(&args, &result, &writer);
        assert!(err.is_err(), "should fail when old file is missing");

        cleanup(&dir);
    }

    #[test]
    fn record_run_with_missing_new_file_returns_error() {
        let dir = temp_dir();
        let old = write_csv(&dir, "old.csv", "id,value\nA,1\n");
        let ledger_path = dir.join("witness.jsonl");
        let writer = ledger::LedgerWriter::with_path(ledger_path);

        let args = make_args(old, dir.join("nonexistent.csv"));
        let result = make_result(Outcome::Refusal);

        let err = record_run_with_writer(&args, &result, &writer);
        assert!(err.is_err(), "should fail when new file is missing");

        cleanup(&dir);
    }

    #[test]
    fn record_run_all_outcomes_produce_correct_exit_codes() {
        let dir = temp_dir();
        let old = write_csv(&dir, "old.csv", "id,value\nA,1\n");
        let new = write_csv(&dir, "new.csv", "id,value\nA,2\n");
        let ledger_path = dir.join("witness.jsonl");
        let writer = ledger::LedgerWriter::with_path(ledger_path.clone());

        let args = make_args(old, new);

        for (outcome, expected_code, expected_str) in [
            (Outcome::NoRealChange, 0, "NO_REAL_CHANGE"),
            (Outcome::RealChange, 1, "REAL_CHANGE"),
            (Outcome::Refusal, 2, "REFUSAL"),
        ] {
            let result = make_result(outcome);
            record_run_with_writer(&args, &result, &writer).unwrap();

            let content = std::fs::read_to_string(&ledger_path).unwrap();
            let last_line = content.lines().last().unwrap();
            let parsed: serde_json::Value = serde_json::from_str(last_line).unwrap();

            assert_eq!(parsed["exit_code"], expected_code);
            assert_eq!(parsed["outcome"], expected_str);
        }

        cleanup(&dir);
    }

    #[test]
    fn record_run_output_hash_matches() {
        let dir = temp_dir();
        let old = write_csv(&dir, "old.csv", "id,value\nA,1\n");
        let new = write_csv(&dir, "new.csv", "id,value\nA,2\n");
        let ledger_path = dir.join("witness.jsonl");
        let writer = ledger::LedgerWriter::with_path(ledger_path.clone());

        let output_text = "specific output content";
        let args = make_args(old, new);
        let result = PipelineResult {
            outcome: Outcome::RealChange,
            output: output_text.to_string(),
            profile: crate::orchestrator::ProfileRunInfo::default(),
        };
        record_run_with_writer(&args, &result, &writer).unwrap();

        let content = std::fs::read_to_string(&ledger_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        let expected_hash = format!("blake3:{}", hash::hash_bytes(output_text.as_bytes()));
        assert_eq!(parsed["output_hash"], expected_hash);

        cleanup(&dir);
    }

    #[test]
    fn record_run_does_not_panic_on_bad_ledger_path() {
        let dir = temp_dir();
        let old = write_csv(&dir, "old.csv", "id,value\nA,1\n");
        let new = write_csv(&dir, "new.csv", "id,value\nA,2\n");
        let writer =
            ledger::LedgerWriter::with_path(PathBuf::from("/dev/null/impossible/witness.jsonl"));

        let args = make_args(old, new);
        let result = make_result(Outcome::RealChange);

        // Should return error, not panic.
        let err = record_run_with_writer(&args, &result, &writer);
        assert!(err.is_err());

        cleanup(&dir);
    }
}
