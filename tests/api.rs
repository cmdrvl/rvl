use std::fs;
use std::process::Command;

use rvl::{CompareOptions, KeyMode, Outcome, compare_bytes};
use serde_json::Value;

const FORMAT_EXTS: [&str; 5] = ["csv", "tsv", "jsonl", "parquet", "xlsx"];

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

fn assert_api_cli_parity(
    old: &str,
    new: &str,
    extra: &[&str],
    key: Option<&str>,
    key_mode: KeyMode,
    non_numeric: bool,
    explicit: bool,
) {
    let mut api_options = options(old, new);
    api_options.key = key.map(str::to_owned);
    api_options.key_mode = key_mode;
    api_options.non_numeric = non_numeric;
    api_options.explicit = explicit;

    let result = compare_bytes(
        &fs::read(old).expect("old fixture should be readable"),
        &fs::read(new).expect("new fixture should be readable"),
        &api_options,
    )
    .expect("in-memory comparison should return a domain result");
    let (exit, cli) = cli_json(old, new, extra);
    let cli_report: Value = serde_json::from_str(&cli).expect("CLI output should be JSON");
    let api_report: Value =
        serde_json::from_str(&result.output).expect("API output should be JSON");

    let outcome_text = cli_report["outcome"].as_str().unwrap_or_default();
    let expected_outcome = match outcome_text {
        "NO_REAL_CHANGE" => Outcome::NoRealChange,
        "REAL_CHANGE" => Outcome::RealChange,
        "REFUSAL" => Outcome::Refusal,
        other => {
            assert!(
                matches!(other, "NO_REAL_CHANGE" | "REAL_CHANGE" | "REFUSAL"),
                "unexpected CLI outcome {other:?}: {cli_report}"
            );
            Outcome::Refusal
        }
    };
    let expected_exit = match expected_outcome {
        Outcome::NoRealChange => 0,
        Outcome::RealChange => 1,
        Outcome::Refusal => 2,
    };

    assert_eq!(result.outcome, expected_outcome, "{old} -> {new} {extra:?}");
    assert_eq!(exit, expected_exit, "{old} -> {new} {extra:?}");
    assert_eq!(
        api_report, cli_report,
        "API/CLI JSON differs for {old} -> {new}"
    );
    assert_eq!(format!("{}\n", result.output), cli, "API/CLI bytes differ");
    if expected_outcome == Outcome::Refusal {
        assert_eq!(
            api_report["refusal"]["code"], cli_report["refusal"]["code"],
            "API/CLI refusal code differs"
        );
    }
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

#[test]
fn compare_bytes_matches_cli_for_golden_outcomes_modes_and_formats() {
    for explicit in [false, true] {
        for old_ext in FORMAT_EXTS {
            for new_ext in FORMAT_EXTS {
                let old = fixture(&format!("old.{old_ext}"));
                let new = fixture(&format!("new.{new_ext}"));
                let mut numeric_args = vec!["--key", "loan_id"];
                if explicit {
                    numeric_args.push("--explicit");
                }
                assert_api_cli_parity(
                    &old,
                    &new,
                    &numeric_args,
                    Some("loan_id"),
                    KeyMode::Exact,
                    false,
                    explicit,
                );

                let unchanged = fixture(&format!("new.{new_ext}"));
                assert_api_cli_parity(
                    &unchanged,
                    &unchanged,
                    &numeric_args,
                    Some("loan_id"),
                    KeyMode::Exact,
                    false,
                    explicit,
                );

                let mut non_numeric_args = vec!["--key", "loan_id", "--non-numeric"];
                if explicit {
                    non_numeric_args.push("--explicit");
                }
                assert_api_cli_parity(
                    &old,
                    &new,
                    &non_numeric_args,
                    Some("loan_id"),
                    KeyMode::Exact,
                    true,
                    explicit,
                );

                let mut overlap_args = vec!["--key", "loan_id", "--key-mode", "overlap"];
                if explicit {
                    overlap_args.push("--explicit");
                }
                assert_api_cli_parity(
                    &old,
                    &new,
                    &overlap_args,
                    Some("loan_id"),
                    KeyMode::Overlap,
                    false,
                    explicit,
                );
            }
        }
    }
}

#[test]
fn compare_bytes_matches_cli_for_golden_refusals() {
    let cases = [
        (
            format!(
                "{}/tests/fixtures/regression/no_numeric_old.csv",
                env!("CARGO_MANIFEST_DIR")
            ),
            format!(
                "{}/tests/fixtures/regression/no_numeric_new.csv",
                env!("CARGO_MANIFEST_DIR")
            ),
            vec!["--key", "id"],
            Some("id"),
        ),
        (
            format!(
                "{}/tests/fixtures/regression/missingness_key_old.csv",
                env!("CARGO_MANIFEST_DIR")
            ),
            format!(
                "{}/tests/fixtures/regression/missingness_key_new.csv",
                env!("CARGO_MANIFEST_DIR")
            ),
            vec!["--key", "id"],
            Some("id"),
        ),
        (
            fixture("old.csv"),
            fixture("binary.parquet"),
            Vec::new(),
            None,
        ),
        (
            fixture("old.csv"),
            fixture("not_really.parquet"),
            Vec::new(),
            None,
        ),
    ];

    for explicit in [false, true] {
        for (old, new, mut args, key) in cases.clone() {
            if explicit {
                args.push("--explicit");
            }
            assert_api_cli_parity(&old, &new, &args, key, KeyMode::Exact, false, explicit);
        }
    }
}
