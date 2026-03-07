use serde::{Deserialize, Serialize};

use crate::cli::args::Args;
use crate::cli::exit::{self, Outcome};
use crate::orchestrator::PipelineResult;
use crate::witness::hash::{hash_bytes, hash_self};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WitnessRecord {
    pub id: String,
    pub tool: String,
    pub version: String,
    pub binary_hash: String,
    pub inputs: Vec<WitnessInput>,
    pub params: serde_json::Value,
    pub outcome: String,
    pub exit_code: u8,
    pub output_hash: String,
    pub ts: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WitnessInput {
    pub path: String,
    pub hash: String,
    pub bytes: u64,
}

/// Serialize a WitnessRecord to canonical JSON (sorted keys, single line).
///
/// serde_json does NOT sort keys when serializing structs directly — it
/// preserves declaration order. We roundtrip through `serde_json::Value`
/// (whose `Map` uses `BTreeMap` by default, which sorts keys) to guarantee
/// deterministic sorted output.
///
/// This function is the ONLY serialization path for witness records. Both
/// `compute_id()` and `LedgerWriter::append()` must use it so that
/// `hash(line_with_id_blanked) == record.id` always holds.
pub fn canonical_json(record: &WitnessRecord) -> String {
    let value = serde_json::to_value(record).expect("WitnessRecord is always serializable");
    serde_json::to_string(&value).expect("Value is always serializable")
}

impl WitnessRecord {
    /// Build a witness record from a completed rvl run.
    ///
    /// `old_bytes` / `new_bytes` are the raw file contents (for hashing).
    /// All hashing happens inside this function.
    pub fn from_run(
        args: &Args,
        result: &PipelineResult,
        old_bytes: &[u8],
        new_bytes: &[u8],
        old_path: &str,
        new_path: &str,
    ) -> Self {
        let binary_hash = hash_self()
            .map(|h| format!("blake3:{h}"))
            .unwrap_or_default();

        let inputs = vec![
            WitnessInput {
                path: old_path.to_string(),
                hash: format!("blake3:{}", hash_bytes(old_bytes)),
                bytes: old_bytes.len() as u64,
            },
            WitnessInput {
                path: new_path.to_string(),
                hash: format!("blake3:{}", hash_bytes(new_bytes)),
                bytes: new_bytes.len() as u64,
            },
        ];

        let outcome_str = match result.outcome {
            Outcome::NoRealChange => "NO_REAL_CHANGE",
            Outcome::RealChange => "REAL_CHANGE",
            Outcome::Refusal => "REFUSAL",
        };

        let delimiter_val = match args.delimiter {
            Some(b) => serde_json::Value::String(format!("0x{b:02x}")),
            None => serde_json::Value::Null,
        };

        let mut params = serde_json::Map::new();
        params.insert("delimiter".to_string(), delimiter_val);
        params.insert("json".to_string(), serde_json::Value::Bool(args.json));
        params.insert(
            "key".to_string(),
            args.key
                .as_ref()
                .map(|value| serde_json::Value::String(value.clone()))
                .unwrap_or(serde_json::Value::Null),
        );
        if result.profile.used {
            params.insert(
                "profile_id".to_string(),
                result
                    .profile
                    .profile_id
                    .as_ref()
                    .map(|value| serde_json::Value::String(value.clone()))
                    .unwrap_or(serde_json::Value::Null),
            );
            params.insert(
                "profile_sha256".to_string(),
                result
                    .profile
                    .profile_sha256
                    .as_ref()
                    .map(|value| serde_json::Value::String(value.clone()))
                    .unwrap_or(serde_json::Value::Null),
            );
        }
        params.insert("threshold".to_string(), serde_json::json!(args.threshold));
        params.insert("tolerance".to_string(), serde_json::json!(args.tolerance));
        let params = serde_json::Value::Object(params);

        let ts = {
            use std::time::SystemTime;
            let d = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = d.as_secs();
            // Manual ISO 8601 UTC formatting without pulling in chrono.
            // Days calculation from Unix epoch.
            let days = secs / 86400;
            let time_of_day = secs % 86400;
            let hours = time_of_day / 3600;
            let minutes = (time_of_day % 3600) / 60;
            let seconds = time_of_day % 60;

            // Date from days since 1970-01-01 (civil calendar algorithm).
            let (y, m, d) = days_to_date(days);
            format!("{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
        };

        Self {
            id: String::new(), // placeholder — call compute_id() after construction
            tool: "rvl".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            binary_hash,
            inputs,
            params,
            outcome: outcome_str.to_string(),
            exit_code: exit::exit_code(result.outcome),
            output_hash: format!("blake3:{}", hash_bytes(result.output.as_bytes())),
            ts,
        }
    }

    /// Compute the content-addressed id by hashing the canonical JSON with
    /// `id` set to empty string.
    pub fn compute_id(&mut self) {
        self.id = String::new();
        let canonical = canonical_json(self);
        self.id = format!("blake3:{}", hash_bytes(canonical.as_bytes()));
    }
}

/// Convert days since Unix epoch to (year, month, day).
/// Uses the civil calendar algorithm from Howard Hinnant.
fn days_to_date(days: u64) -> (i64, u64, u64) {
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_args(key: Option<&str>, json: bool) -> Args {
        Args::new(
            PathBuf::from("old.csv"),
            PathBuf::from("new.csv"),
            key.map(|s| s.to_string()),
            0.95,
            1e-9,
            None,
            json,
        )
    }

    fn make_result(outcome: Outcome) -> PipelineResult {
        PipelineResult {
            outcome,
            output: "test output".to_string(),
            profile: crate::orchestrator::ProfileRunInfo::default(),
        }
    }

    #[test]
    fn from_run_populates_all_fields() {
        let args = make_args(Some("id"), true);
        let result = make_result(Outcome::RealChange);
        let old = b"old content";
        let new = b"new content";

        let mut rec = WitnessRecord::from_run(&args, &result, old, new, "old.csv", "new.csv");
        rec.compute_id();

        assert!(!rec.id.is_empty());
        assert!(rec.id.starts_with("blake3:"));
        assert_eq!(rec.tool, "rvl");
        assert!(!rec.version.is_empty());
        assert!(rec.binary_hash.starts_with("blake3:"));
        assert_eq!(rec.inputs.len(), 2);
        assert_eq!(rec.inputs[0].path, "old.csv");
        assert!(rec.inputs[0].hash.starts_with("blake3:"));
        assert_eq!(rec.inputs[0].bytes, old.len() as u64);
        assert_eq!(rec.inputs[1].path, "new.csv");
        assert_eq!(rec.inputs[1].bytes, new.len() as u64);
        assert_eq!(rec.outcome, "REAL_CHANGE");
        assert_eq!(rec.exit_code, 1);
        assert!(rec.output_hash.starts_with("blake3:"));
        assert!(!rec.ts.is_empty());
        assert!(rec.ts.ends_with('Z'));
    }

    #[test]
    fn from_run_all_outcomes() {
        let args = make_args(None, false);

        let r1 = WitnessRecord::from_run(
            &args,
            &make_result(Outcome::NoRealChange),
            b"a",
            b"b",
            "a.csv",
            "b.csv",
        );
        assert_eq!(r1.outcome, "NO_REAL_CHANGE");
        assert_eq!(r1.exit_code, 0);

        let r2 = WitnessRecord::from_run(
            &args,
            &make_result(Outcome::RealChange),
            b"a",
            b"b",
            "a.csv",
            "b.csv",
        );
        assert_eq!(r2.outcome, "REAL_CHANGE");
        assert_eq!(r2.exit_code, 1);

        let r3 = WitnessRecord::from_run(
            &args,
            &make_result(Outcome::Refusal),
            b"a",
            b"b",
            "a.csv",
            "b.csv",
        );
        assert_eq!(r3.outcome, "REFUSAL");
        assert_eq!(r3.exit_code, 2);
    }

    #[test]
    fn compute_id_is_deterministic() {
        let args = make_args(Some("key"), false);
        let result = make_result(Outcome::NoRealChange);

        let mut r1 = WitnessRecord::from_run(&args, &result, b"x", b"y", "a.csv", "b.csv");
        // Force a fixed timestamp for determinism.
        r1.ts = "2026-01-01T00:00:00Z".to_string();
        r1.binary_hash = "blake3:fixed".to_string();
        r1.compute_id();

        let mut r2 = r1.clone();
        r2.id = String::new();
        r2.compute_id();

        assert_eq!(r1.id, r2.id);
    }

    #[test]
    fn compute_id_changes_when_field_changes() {
        let args = make_args(None, false);
        let result = make_result(Outcome::RealChange);

        let mut base = WitnessRecord::from_run(&args, &result, b"x", b"y", "a.csv", "b.csv");
        base.ts = "2026-01-01T00:00:00Z".to_string();
        base.binary_hash = "blake3:fixed".to_string();
        base.compute_id();
        let base_id = base.id.clone();

        // Change outcome.
        let mut variant = base.clone();
        variant.outcome = "NO_REAL_CHANGE".to_string();
        variant.compute_id();
        assert_ne!(variant.id, base_id, "id should change when outcome changes");

        // Change tool.
        let mut variant = base.clone();
        variant.tool = "other".to_string();
        variant.compute_id();
        assert_ne!(variant.id, base_id, "id should change when tool changes");

        // Change ts.
        let mut variant = base.clone();
        variant.ts = "2099-12-31T23:59:59Z".to_string();
        variant.compute_id();
        assert_ne!(variant.id, base_id, "id should change when ts changes");
    }

    #[test]
    fn canonical_json_has_alphabetically_sorted_keys() {
        let args = make_args(Some("id"), true);
        let result = make_result(Outcome::RealChange);
        let rec = WitnessRecord::from_run(&args, &result, b"a", b"b", "a.csv", "b.csv");

        let json = canonical_json(&rec);
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let obj = value.as_object().unwrap();
        let keys: Vec<&String> = obj.keys().collect();

        let expected_order = vec![
            "binary_hash",
            "exit_code",
            "id",
            "inputs",
            "outcome",
            "output_hash",
            "params",
            "tool",
            "ts",
            "version",
        ];

        assert_eq!(
            keys.iter().map(|k| k.as_str()).collect::<Vec<_>>(),
            expected_order,
            "canonical JSON keys must be in alphabetical order"
        );
    }

    #[test]
    fn canonical_json_inputs_have_sorted_keys() {
        let args = make_args(None, false);
        let result = make_result(Outcome::NoRealChange);
        let rec = WitnessRecord::from_run(&args, &result, b"a", b"b", "a.csv", "b.csv");

        let json = canonical_json(&rec);
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let inputs = value["inputs"].as_array().unwrap();
        for input in inputs {
            let keys: Vec<&String> = input.as_object().unwrap().keys().collect();
            assert_eq!(
                keys.iter().map(|k| k.as_str()).collect::<Vec<_>>(),
                vec!["bytes", "hash", "path"],
                "input keys must be alphabetically sorted"
            );
        }
    }

    #[test]
    fn canonical_json_params_have_sorted_keys() {
        let args = make_args(Some("loan_id"), true);
        let result = make_result(Outcome::RealChange);
        let rec = WitnessRecord::from_run(&args, &result, b"a", b"b", "a.csv", "b.csv");

        let json = canonical_json(&rec);
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let params = value["params"].as_object().unwrap();
        let keys: Vec<&String> = params.keys().collect();
        assert_eq!(
            keys.iter().map(|k| k.as_str()).collect::<Vec<_>>(),
            vec!["delimiter", "json", "key", "threshold", "tolerance"],
            "params keys must be alphabetically sorted"
        );
    }

    #[test]
    fn params_captures_cli_args() {
        let args = Args::new(
            PathBuf::from("old.csv"),
            PathBuf::from("new.csv"),
            Some("account_id".to_string()),
            0.80,
            1e-6,
            Some(b'\t'),
            true,
        );
        let result = make_result(Outcome::RealChange);
        let rec = WitnessRecord::from_run(&args, &result, b"a", b"b", "a.csv", "b.csv");

        assert_eq!(rec.params["key"], "account_id");
        assert_eq!(rec.params["threshold"], 0.80);
        assert_eq!(rec.params["tolerance"], 1e-6);
        assert_eq!(rec.params["delimiter"], "0x09");
        assert_eq!(rec.params["json"], true);
    }

    #[test]
    fn params_null_when_not_set() {
        let args = make_args(None, false);
        let result = make_result(Outcome::NoRealChange);
        let rec = WitnessRecord::from_run(&args, &result, b"a", b"b", "a.csv", "b.csv");

        assert!(rec.params["key"].is_null());
        assert!(rec.params["delimiter"].is_null());
        assert_eq!(rec.params["json"], false);
    }

    #[test]
    fn params_include_profile_fields_when_profile_used() {
        let args = make_args(None, false);
        let mut result = make_result(Outcome::NoRealChange);
        result.profile = crate::orchestrator::ProfileRunInfo {
            used: true,
            profile_id: Some("csv.loan_tape.core.v0".to_string()),
            profile_sha256: Some("sha256:abc".to_string()),
            capsule_profile: None,
        };
        let rec = WitnessRecord::from_run(&args, &result, b"a", b"b", "a.csv", "b.csv");

        assert_eq!(rec.params["profile_id"], "csv.loan_tape.core.v0");
        assert_eq!(rec.params["profile_sha256"], "sha256:abc");
    }

    #[test]
    fn from_run_leaves_id_empty_until_compute_id() {
        let args = make_args(None, false);
        let result = make_result(Outcome::NoRealChange);

        let rec = WitnessRecord::from_run(&args, &result, b"a", b"b", "a.csv", "b.csv");
        assert!(rec.id.is_empty());
    }

    #[test]
    fn roundtrip_serialize_deserialize() {
        let args = make_args(Some("id"), true);
        let result = make_result(Outcome::RealChange);
        let mut rec = WitnessRecord::from_run(&args, &result, b"old", b"new", "old.csv", "new.csv");
        rec.compute_id();

        let json = canonical_json(&rec);
        let deserialized: WitnessRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, rec.id);
        assert_eq!(deserialized.tool, rec.tool);
        assert_eq!(deserialized.version, rec.version);
        assert_eq!(deserialized.outcome, rec.outcome);
        assert_eq!(deserialized.exit_code, rec.exit_code);
        assert_eq!(deserialized.ts, rec.ts);
        assert_eq!(deserialized.inputs.len(), 2);
    }

    #[test]
    fn ts_format_is_iso8601() {
        let args = make_args(None, false);
        let result = make_result(Outcome::NoRealChange);
        let rec = WitnessRecord::from_run(&args, &result, b"a", b"b", "a.csv", "b.csv");

        // Pattern: YYYY-MM-DDTHH:MM:SSZ
        assert!(rec.ts.ends_with('Z'));
        assert_eq!(rec.ts.len(), 20);
        assert_eq!(&rec.ts[4..5], "-");
        assert_eq!(&rec.ts[7..8], "-");
        assert_eq!(&rec.ts[10..11], "T");
        assert_eq!(&rec.ts[13..14], ":");
        assert_eq!(&rec.ts[16..17], ":");
    }

    #[test]
    fn days_to_date_known_values() {
        // 1970-01-01 = day 0
        assert_eq!(days_to_date(0), (1970, 1, 1));
        // 2000-01-01 = day 10957
        assert_eq!(days_to_date(10957), (2000, 1, 1));
        // 2026-02-24 = day 20508
        assert_eq!(days_to_date(20508), (2026, 2, 24));
    }
}
