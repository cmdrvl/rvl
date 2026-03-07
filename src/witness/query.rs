use crate::witness::record::{WitnessRecord, canonical_json};
use std::path::Path;

/// Filters for querying witness records.
#[derive(Debug, Default)]
pub struct QueryFilter {
    pub tool: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub outcome: Option<String>,
    pub input_hash: Option<String>,
    pub limit: usize,
}

impl QueryFilter {
    pub fn new() -> Self {
        Self {
            limit: 20,
            ..Default::default()
        }
    }

    /// Check if a record matches all active filters.
    pub fn matches(&self, record: &WitnessRecord) -> bool {
        if let Some(ref tool) = self.tool
            && record.tool != *tool
        {
            return false;
        }
        if let Some(ref since) = self.since
            && record.ts.as_str() < since.as_str()
        {
            return false;
        }
        if let Some(ref until) = self.until
            && record.ts.as_str() > until.as_str()
        {
            return false;
        }
        if let Some(ref outcome) = self.outcome
            && !record.outcome.eq_ignore_ascii_case(outcome)
        {
            return false;
        }
        if let Some(ref input_hash) = self.input_hash {
            let has_match = record
                .inputs
                .iter()
                .any(|input| input.hash.contains(input_hash.as_str()));
            if !has_match {
                return false;
            }
        }
        true
    }
}

/// Format a single record in human-readable multi-line form.
pub fn format_record_human(record: &WitnessRecord) -> String {
    let mut lines = Vec::new();
    lines.push(format!("id:       {}", record.id));
    lines.push(format!("ts:       {}", record.ts));
    lines.push(format!("tool:     {}", record.tool));
    lines.push(format!("version:  {}", record.version));
    lines.push(format!("outcome:  {}", record.outcome));
    lines.push(format!("exit:     {}", record.exit_code));
    for (i, input) in record.inputs.iter().enumerate() {
        lines.push(format!(
            "input[{}]: {} ({} bytes, {})",
            i, input.path, input.bytes, input.hash
        ));
    }
    lines.join("\n")
}

/// Format a single record as canonical JSON.
pub fn format_record_json(record: &WitnessRecord) -> String {
    canonical_json(record)
}

/// Format multiple records in human-readable form with separators.
pub fn format_records_human(records: &[WitnessRecord]) -> String {
    const ID_WIDTH: usize = 14;
    const TS_WIDTH: usize = 20;
    const OUTCOME_WIDTH: usize = 15;

    let mut lines = Vec::with_capacity(records.len() + 1);
    lines.push(format!(
        "{:<ID_WIDTH$} {:<TS_WIDTH$} {:<OUTCOME_WIDTH$} {}",
        "ID (short)", "Timestamp", "Outcome", "Inputs"
    ));

    for record in records {
        lines.push(format!(
            "{:<ID_WIDTH$} {:<TS_WIDTH$} {:<OUTCOME_WIDTH$} {}",
            short_id(&record.id),
            record.ts,
            record.outcome,
            summarize_inputs(record),
        ));
    }

    lines.join("\n")
}

/// Format multiple records as a JSON array.
pub fn format_records_json(records: &[WitnessRecord]) -> String {
    let values: Vec<serde_json::Value> = records
        .iter()
        .map(|r| serde_json::to_value(r).expect("WitnessRecord is always serializable"))
        .collect();
    serde_json::to_string(&values).expect("Vec<Value> is always serializable")
}

/// Format a count in human-readable form.
pub fn format_count_human(count: usize) -> String {
    format!("{count}")
}

/// Format a count as JSON.
pub fn format_count_json(count: usize) -> String {
    format!("{{\"count\":{count}}}")
}

fn short_id(full_id: &str) -> String {
    if let Some(hash) = full_id.strip_prefix("blake3:") {
        let short = &hash[..hash.len().min(4)];
        return format!("blake3:{short}");
    }

    full_id.chars().take(12).collect()
}

fn summarize_inputs(record: &WitnessRecord) -> String {
    match record.inputs.as_slice() {
        [] => "-".to_string(),
        [single] => basename(&single.path),
        [first, second, ..] => format!("{} -> {}", basename(&first.path), basename(&second.path)),
    }
}

fn basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::args::Args;
    use crate::cli::exit::Outcome;
    use crate::orchestrator::PipelineResult;
    use std::path::PathBuf;

    fn make_record(outcome_str: &str, ts: &str, tool: &str) -> WitnessRecord {
        let args = Args::new(
            PathBuf::from("old.csv"),
            PathBuf::from("new.csv"),
            None,
            0.95,
            1e-9,
            None,
            false,
        );
        let outcome = match outcome_str {
            "REAL_CHANGE" => Outcome::RealChange,
            "NO_REAL_CHANGE" => Outcome::NoRealChange,
            _ => Outcome::Refusal,
        };
        let result = PipelineResult {
            outcome,
            output: "test output".to_string(),
            profile: crate::orchestrator::ProfileRunInfo::default(),
        };
        let mut rec = WitnessRecord::from_run(&args, &result, b"old", b"new", "old.csv", "new.csv");
        rec.ts = ts.to_string();
        rec.tool = tool.to_string();
        rec.compute_id();
        rec
    }

    #[test]
    fn filter_matches_all_when_empty() {
        let filter = QueryFilter::new();
        let rec = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z", "rvl");
        assert!(filter.matches(&rec));
    }

    #[test]
    fn filter_tool() {
        let mut filter = QueryFilter::new();
        filter.tool = Some("rvl".to_string());
        let rec = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z", "rvl");
        assert!(filter.matches(&rec));

        filter.tool = Some("other".to_string());
        assert!(!filter.matches(&rec));
    }

    #[test]
    fn filter_since() {
        let mut filter = QueryFilter::new();
        filter.since = Some("2026-01-02T00:00:00Z".to_string());
        let rec = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z", "rvl");
        assert!(
            !filter.matches(&rec),
            "record before since should not match"
        );

        let rec2 = make_record("REAL_CHANGE", "2026-01-03T00:00:00Z", "rvl");
        assert!(filter.matches(&rec2));
    }

    #[test]
    fn filter_until() {
        let mut filter = QueryFilter::new();
        filter.until = Some("2026-01-02T00:00:00Z".to_string());
        let rec = make_record("REAL_CHANGE", "2026-01-03T00:00:00Z", "rvl");
        assert!(!filter.matches(&rec), "record after until should not match");

        let rec2 = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z", "rvl");
        assert!(filter.matches(&rec2));
    }

    #[test]
    fn filter_outcome_case_insensitive() {
        let mut filter = QueryFilter::new();
        filter.outcome = Some("real_change".to_string());
        let rec = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z", "rvl");
        assert!(filter.matches(&rec));

        filter.outcome = Some("REFUSAL".to_string());
        assert!(!filter.matches(&rec));
    }

    #[test]
    fn filter_input_hash_substring() {
        let mut filter = QueryFilter::new();
        let rec = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z", "rvl");
        // Use a substring of the actual hash.
        let hash_substr = &rec.inputs[0].hash[7..15]; // skip "blake3:" prefix, take 8 chars
        filter.input_hash = Some(hash_substr.to_string());
        assert!(filter.matches(&rec));

        filter.input_hash = Some("nonexistent_hash_value".to_string());
        assert!(!filter.matches(&rec));
    }

    #[test]
    fn filter_combined() {
        let mut filter = QueryFilter::new();
        filter.tool = Some("rvl".to_string());
        filter.outcome = Some("REAL_CHANGE".to_string());
        filter.since = Some("2026-01-01T00:00:00Z".to_string());
        filter.until = Some("2026-01-31T23:59:59Z".to_string());

        let rec = make_record("REAL_CHANGE", "2026-01-15T12:00:00Z", "rvl");
        assert!(filter.matches(&rec));

        let rec_wrong_outcome = make_record("REFUSAL", "2026-01-15T12:00:00Z", "rvl");
        assert!(!filter.matches(&rec_wrong_outcome));

        let rec_too_early = make_record("REAL_CHANGE", "2025-12-31T00:00:00Z", "rvl");
        assert!(!filter.matches(&rec_too_early));
    }

    #[test]
    fn format_record_human_output() {
        let rec = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z", "rvl");
        let output = format_record_human(&rec);
        assert!(output.contains("id:"));
        assert!(output.contains("ts:       2026-01-01T00:00:00Z"));
        assert!(output.contains("tool:     rvl"));
        assert!(output.contains("outcome:  REAL_CHANGE"));
        assert!(output.contains("input[0]:"));
        assert!(output.contains("input[1]:"));
    }

    #[test]
    fn format_record_json_is_valid_json() {
        let rec = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z", "rvl");
        let json_str = format_record_json(&rec);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["tool"], "rvl");
    }

    #[test]
    fn format_records_human_outputs_compact_table() {
        let rec1 = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z", "rvl");
        let rec2 = make_record("NO_REAL_CHANGE", "2026-01-02T00:00:00Z", "rvl");
        let output = format_records_human(&[rec1, rec2]);
        assert!(output.contains("ID (short)"));
        assert!(output.contains("Timestamp"));
        assert!(output.contains("Outcome"));
        assert!(output.contains("Inputs"));
        assert!(output.contains("old.csv -> new.csv"));
    }

    #[test]
    fn format_records_human_empty_still_has_header() {
        let output = format_records_human(&[]);
        assert_eq!(
            output,
            "ID (short)     Timestamp            Outcome         Inputs"
        );
    }

    #[test]
    fn format_records_json_is_array() {
        let rec1 = make_record("REAL_CHANGE", "2026-01-01T00:00:00Z", "rvl");
        let rec2 = make_record("NO_REAL_CHANGE", "2026-01-02T00:00:00Z", "rvl");
        let json_str = format_records_json(&[rec1, rec2]);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 2);
    }

    #[test]
    fn format_count_human_output() {
        assert_eq!(format_count_human(42), "42");
    }

    #[test]
    fn format_count_json_output() {
        let json_str = format_count_json(42);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["count"], 42);
    }
}
