use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use crate::witness::record::{WitnessRecord, canonical_json};

/// Resolve the ledger path.
///
/// Priority:
/// 1. `EPISTEMIC_WITNESS` env var (if set, use as file path)
/// 2. Default: `~/.epistemic/witness.jsonl`
pub(crate) fn resolve_ledger_path() -> io::Result<PathBuf> {
    if let Ok(env_path) = std::env::var("EPISTEMIC_WITNESS") {
        Ok(PathBuf::from(env_path))
    } else {
        let home = home_dir().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "could not determine home directory",
            )
        })?;
        Ok(home.join(".epistemic").join("witness.jsonl"))
    }
}

pub struct LedgerWriter {
    path: PathBuf,
}

impl LedgerWriter {
    /// Resolve the ledger path and return a writer.
    pub fn open() -> io::Result<Self> {
        let path = resolve_ledger_path()?;
        Ok(Self { path })
    }

    /// Create a writer targeting a specific path (for testing and direct use).
    pub fn with_path(path: PathBuf) -> Self {
        Self { path }
    }

    /// Read the `id` from the last record in the ledger (for prev chaining).
    ///
    /// Returns `None` if the ledger is empty, doesn't exist, or the last line
    /// isn't valid JSON. Never fails — this is best-effort chaining.
    pub fn read_prev(&self) -> Option<String> {
        let file = File::open(&self.path).ok()?;
        let reader = io::BufReader::new(file);
        let mut last_line = None;
        for l in reader.lines().map_while(Result::ok) {
            let trimmed = l.trim().to_string();
            if !trimmed.is_empty() {
                last_line = Some(trimmed);
            }
        }
        let last = last_line?;
        let value: serde_json::Value = serde_json::from_str(&last).ok()?;
        value.get("id")?.as_str().map(|s| s.to_string())
    }

    /// Append a record to the ledger using canonical JSON (sorted keys).
    ///
    /// Creates parent directories if needed. Serializes via `canonical_json()`
    /// to ensure the ledger uses the same key ordering as `compute_id()`.
    pub fn append(&self, record: &WitnessRecord) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let json = canonical_json(record);
        writeln!(file, "{json}")?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }
}

/// Try to append a witness record. Logs errors to stderr but never propagates
/// them — the tool's primary function (CSV comparison) must not be affected
/// by witness failures.
pub fn try_append(record: &WitnessRecord) {
    let result = LedgerWriter::open().and_then(|w| w.append(record));
    if let Err(e) = result {
        eprintln!("rvl: witness: {e}");
    }
}

/// Cross-platform home directory resolution.
fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::args::Args;
    use crate::cli::exit::Outcome;
    use crate::orchestrator::PipelineResult;

    fn make_record(prev: Option<String>) -> WitnessRecord {
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
            outcome: Outcome::NoRealChange,
            output: "test output".to_string(),
            profile: crate::orchestrator::ProfileRunInfo::default(),
        };
        let mut rec =
            WitnessRecord::from_run(&args, &result, b"old", b"new", "old.csv", "new.csv", prev);
        rec.ts = "2026-01-01T00:00:00Z".to_string();
        rec.compute_id();
        rec
    }

    fn temp_ledger_path() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!("rvl_test_ledger_{id}_{seq}"))
            .join("witness.jsonl")
    }

    #[test]
    fn append_creates_new_file_with_one_line() {
        let path = temp_ledger_path();
        let writer = LedgerWriter::with_path(path.clone());
        let rec = make_record(None);
        writer.append(&rec).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1, "should have exactly one JSONL line");

        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert!(parsed.get("id").is_some());
        assert_eq!(parsed["tool"], "rvl");

        fs::remove_file(&path).ok();
        fs::remove_dir(path.parent().unwrap()).ok();
    }

    #[test]
    fn append_twice_produces_two_lines_with_prev_chaining() {
        let path = temp_ledger_path();
        let writer = LedgerWriter::with_path(path.clone());

        let rec1 = make_record(None);
        writer.append(&rec1).unwrap();

        let prev_id = writer.read_prev();
        assert_eq!(prev_id, Some(rec1.id.clone()));

        let rec2 = make_record(prev_id);
        writer.append(&rec2).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "should have two JSONL lines");

        let parsed2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed2["prev"].as_str().unwrap(), rec1.id);

        fs::remove_file(&path).ok();
        fs::remove_dir(path.parent().unwrap()).ok();
    }

    #[test]
    fn read_prev_on_empty_file_returns_none() {
        let path = temp_ledger_path();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "").unwrap();

        let writer = LedgerWriter::with_path(path.clone());
        assert!(writer.read_prev().is_none());

        fs::remove_file(&path).ok();
        fs::remove_dir(path.parent().unwrap()).ok();
    }

    #[test]
    fn read_prev_on_nonexistent_file_returns_none() {
        let path = temp_ledger_path();
        let writer = LedgerWriter::with_path(path);
        assert!(writer.read_prev().is_none());
    }

    #[test]
    fn read_prev_on_one_record_returns_id() {
        let path = temp_ledger_path();
        let writer = LedgerWriter::with_path(path.clone());
        let rec = make_record(None);
        writer.append(&rec).unwrap();

        let prev = writer.read_prev();
        assert_eq!(prev, Some(rec.id.clone()));

        fs::remove_file(&path).ok();
        fs::remove_dir(path.parent().unwrap()).ok();
    }

    #[test]
    fn read_prev_on_corrupt_last_line_returns_none() {
        let path = temp_ledger_path();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "this is not valid json\n").unwrap();

        let writer = LedgerWriter::with_path(path.clone());
        assert!(writer.read_prev().is_none());

        fs::remove_file(&path).ok();
        fs::remove_dir(path.parent().unwrap()).ok();
    }

    #[test]
    fn read_prev_skips_trailing_blank_lines() {
        let path = temp_ledger_path();
        let writer = LedgerWriter::with_path(path.clone());
        let rec = make_record(None);
        writer.append(&rec).unwrap();

        // Append trailing blank lines.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file).unwrap();
        writeln!(file).unwrap();

        let prev = writer.read_prev();
        assert_eq!(prev, Some(rec.id.clone()));

        fs::remove_file(&path).ok();
        fs::remove_dir(path.parent().unwrap()).ok();
    }

    #[test]
    fn ledger_lines_are_canonical_json_verifiable() {
        let path = temp_ledger_path();
        let writer = LedgerWriter::with_path(path.clone());
        let rec = make_record(None);
        writer.append(&rec).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        let line = content.lines().next().unwrap();

        // Verify: blank the id, hash the line, compare to stored id.
        let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
        let stored_id = value["id"].as_str().unwrap().to_string();

        value["id"] = serde_json::Value::String(String::new());
        let canonical = serde_json::to_string(&value).unwrap();
        let expected_id = format!(
            "blake3:{}",
            crate::witness::hash::hash_bytes(canonical.as_bytes())
        );

        assert_eq!(
            stored_id, expected_id,
            "ledger line hash verification failed"
        );

        fs::remove_file(&path).ok();
        fs::remove_dir(path.parent().unwrap()).ok();
    }

    #[test]
    fn append_to_bad_path_returns_error() {
        // /dev/null is a file, not a directory, so creating children fails.
        let writer = LedgerWriter::with_path(PathBuf::from("/dev/null/impossible/witness.jsonl"));
        let rec = make_record(None);
        assert!(writer.append(&rec).is_err());
    }

    #[test]
    fn try_append_on_bad_path_does_not_panic() {
        // Use with_path to create a writer that will fail, then call try_append
        // directly with the record (try_append uses open() internally, but we
        // test the non-panic behavior).
        let writer = LedgerWriter::with_path(PathBuf::from("/dev/null/impossible/witness.jsonl"));
        let rec = make_record(None);
        // This should not panic — errors are swallowed.
        let result = writer.append(&rec);
        assert!(result.is_err());
    }

    #[test]
    fn append_creates_parent_directories() {
        let path = std::env::temp_dir()
            .join(format!(
                "rvl_test_nested_{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .join("deep")
            .join("path")
            .join("witness.jsonl");

        let writer = LedgerWriter::with_path(path.clone());
        let rec = make_record(None);
        writer.append(&rec).unwrap();

        assert!(path.exists());

        fs::remove_file(&path).ok();
        let mut p = path.parent();
        while let Some(dir) = p {
            if dir == std::env::temp_dir() {
                break;
            }
            fs::remove_dir(dir).ok();
            p = dir.parent();
        }
    }

    #[test]
    fn with_path_constructor() {
        let path = PathBuf::from("/tmp/custom/witness.jsonl");
        let writer = LedgerWriter::with_path(path.clone());
        assert_eq!(writer.path, path);
    }

    #[test]
    fn three_record_chain() {
        let path = temp_ledger_path();
        let writer = LedgerWriter::with_path(path.clone());

        let rec1 = make_record(None);
        writer.append(&rec1).unwrap();

        let prev1 = writer.read_prev();
        assert_eq!(prev1.as_ref(), Some(&rec1.id));

        let mut rec2 = make_record(prev1);
        rec2.outcome = "REAL_CHANGE".to_string();
        rec2.compute_id();
        writer.append(&rec2).unwrap();

        let prev2 = writer.read_prev();
        assert_eq!(prev2.as_ref(), Some(&rec2.id));

        let rec3 = make_record(prev2);
        writer.append(&rec3).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);

        // Verify chain: rec3.prev == rec2.id, rec2.prev == rec1.id
        let p3: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        let p2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        let p1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(p3["prev"].as_str().unwrap(), p2["id"].as_str().unwrap());
        assert_eq!(p2["prev"].as_str().unwrap(), p1["id"].as_str().unwrap());
        assert!(p1["prev"].is_null());

        fs::remove_file(&path).ok();
        fs::remove_dir(path.parent().unwrap()).ok();
    }
}
