use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;

use crate::witness::ledger::resolve_ledger_path;
use crate::witness::record::WitnessRecord;

pub struct LedgerReader {
    path: PathBuf,
}

impl LedgerReader {
    /// Open the ledger at the default/env-configured path.
    pub fn open() -> io::Result<Self> {
        let path = resolve_ledger_path()?;
        Ok(Self { path })
    }

    /// Create a reader targeting a specific path (for testing).
    pub fn with_path(path: PathBuf) -> Self {
        Self { path }
    }

    /// Read all valid records from the ledger. Skips malformed lines.
    /// Returns an empty vec if the file doesn't exist.
    pub fn records(&self) -> Vec<WitnessRecord> {
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return Vec::new(),
        };
        let reader = io::BufReader::new(file);
        reader
            .lines()
            .map_while(Result::ok)
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str::<WitnessRecord>(&line).ok())
            .collect()
    }

    /// Read only the last valid record. Returns None if ledger is empty or
    /// doesn't exist.
    pub fn last_record(&self) -> Option<WitnessRecord> {
        let file = File::open(&self.path).ok()?;
        let reader = io::BufReader::new(file);
        let mut last = None;
        for line in reader.lines().map_while(Result::ok) {
            let trimmed = line.trim().to_string();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(rec) = serde_json::from_str::<WitnessRecord>(&trimmed) {
                last = Some(rec);
            }
        }
        last
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::args::Args;
    use crate::cli::exit::Outcome;
    use crate::orchestrator::PipelineResult;
    use crate::witness::ledger::LedgerWriter;
    use crate::witness::record::WitnessRecord;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_ledger_path() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!("rvl_test_reader_{id}_{seq}"))
            .join("witness.jsonl")
    }

    fn make_record(outcome: &str, ts: &str) -> WitnessRecord {
        let args = Args::new(
            PathBuf::from("old.csv"),
            PathBuf::from("new.csv"),
            None,
            0.95,
            1e-9,
            None,
            false,
        );
        let pipeline_outcome = match outcome {
            "REAL_CHANGE" => Outcome::RealChange,
            "NO_REAL_CHANGE" => Outcome::NoRealChange,
            _ => Outcome::Refusal,
        };
        let result = PipelineResult {
            outcome: pipeline_outcome,
            output: "test output".to_string(),
        };
        let mut rec =
            WitnessRecord::from_run(&args, &result, b"old", b"new", "old.csv", "new.csv", None);
        rec.ts = ts.to_string();
        rec.compute_id();
        rec
    }

    fn write_records(path: &Path, records: &[WitnessRecord]) {
        let writer = LedgerWriter::with_path(path.to_path_buf());
        for rec in records {
            writer.append(rec).unwrap();
        }
    }

    fn cleanup(path: &Path) {
        if let Some(parent) = path.parent() {
            std::fs::remove_dir_all(parent).ok();
        }
    }

    #[test]
    fn records_on_nonexistent_file_returns_empty() {
        let path = temp_ledger_path();
        let reader = LedgerReader::with_path(path);
        assert!(reader.records().is_empty());
    }

    #[test]
    fn last_record_on_nonexistent_file_returns_none() {
        let path = temp_ledger_path();
        let reader = LedgerReader::with_path(path);
        assert!(reader.last_record().is_none());
    }

    #[test]
    fn records_on_empty_file_returns_empty() {
        let path = temp_ledger_path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "").unwrap();
        let reader = LedgerReader::with_path(path.clone());
        assert!(reader.records().is_empty());
        cleanup(&path);
    }

    #[test]
    fn records_returns_valid_records() {
        let path = temp_ledger_path();
        let rec1 = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z");
        let rec2 = make_record("NO_REAL_CHANGE", "2026-01-02T00:00:00Z");
        write_records(&path, &[rec1.clone(), rec2.clone()]);

        let reader = LedgerReader::with_path(path.clone());
        let records = reader.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].id, rec1.id);
        assert_eq!(records[1].id, rec2.id);
        cleanup(&path);
    }

    #[test]
    fn records_skips_malformed_lines() {
        let path = temp_ledger_path();
        let rec = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z");
        write_records(&path, std::slice::from_ref(&rec));

        // Append a malformed line.
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(file, "not valid json").unwrap();

        let reader = LedgerReader::with_path(path.clone());
        let records = reader.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, rec.id);
        cleanup(&path);
    }

    #[test]
    fn last_record_returns_final_valid_record() {
        let path = temp_ledger_path();
        let rec1 = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z");
        let rec2 = make_record("NO_REAL_CHANGE", "2026-01-02T00:00:00Z");
        write_records(&path, &[rec1, rec2.clone()]);

        let reader = LedgerReader::with_path(path.clone());
        let last = reader.last_record().unwrap();
        assert_eq!(last.id, rec2.id);
        cleanup(&path);
    }

    #[test]
    fn last_record_on_empty_file_returns_none() {
        let path = temp_ledger_path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "").unwrap();
        let reader = LedgerReader::with_path(path.clone());
        assert!(reader.last_record().is_none());
        cleanup(&path);
    }
}
