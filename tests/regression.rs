use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rvl::cli::args::Args;
use rvl::orchestrator;
use serde_json::Value;

const REGRESSION_DIR: &str = "tests/fixtures/regression";

fn run_case(old: &str, new: &str, key: Option<&str>, json: bool) -> String {
    let args = Args {
        old: Some(PathBuf::from(old)),
        new: Some(PathBuf::from(new)),
        key: key.map(|value| value.to_string()),
        threshold: 0.95,
        tolerance: 1e-9,
        delimiter: None,
        profile: None,
        profile_id: None,
        capsule_out: None,
        json,
        no_witness: true,
        describe: false,
        command: None,
    };
    orchestrator::run(&args)
        .expect("pipeline run should succeed")
        .output
}

fn load_text(path: &str) -> String {
    std::fs::read_to_string(path).expect("fixture should be readable")
}

fn load_json(path: &str) -> Value {
    let raw = load_text(path);
    serde_json::from_str(&raw).expect("expected JSON fixture")
}

fn normalize_human(text: &str) -> String {
    text.trim_end_matches('\n').to_string()
}

fn unique_temp_csv(label: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rvl-regression-{label}-{}-{seq}-{nanos}.csv",
        std::process::id(),
    ))
}

fn assert_case(name: &str, old: &str, new: &str, key: Option<&str>) {
    let human_actual = normalize_human(&run_case(old, new, key, false));
    let human_expected = normalize_human(&load_text(&format!("{REGRESSION_DIR}/{name}.human.txt")));
    assert_eq!(
        human_actual, human_expected,
        "human output mismatch for {name}"
    );

    let json_actual: Value =
        serde_json::from_str(&run_case(old, new, key, true)).expect("json output should parse");
    let json_expected = load_json(&format!("{REGRESSION_DIR}/{name}.json"));
    assert_eq!(
        json_actual, json_expected,
        "json output mismatch for {name}"
    );
}

#[test]
fn regression_real_change() {
    assert_case(
        "real_change",
        "tests/fixtures/regression/real_change_old.csv",
        "tests/fixtures/regression/real_change_new.csv",
        Some("id"),
    );
}

#[test]
fn regression_no_real_change() {
    assert_case(
        "no_real_change",
        "tests/fixtures/regression/no_real_change_old.csv",
        "tests/fixtures/regression/no_real_change_new.csv",
        None,
    );
}

#[test]
fn regression_no_numeric() {
    assert_case(
        "no_numeric",
        "tests/fixtures/regression/no_numeric_old.csv",
        "tests/fixtures/regression/no_numeric_new.csv",
        Some("id"),
    );
}

#[test]
fn regression_basic() {
    assert_case(
        "basic",
        "tests/fixtures/regression/basic_old.csv",
        "tests/fixtures/regression/basic_new.csv",
        None,
    );
}

#[test]
fn regression_missingness_key() {
    assert_case(
        "missingness_key",
        "tests/fixtures/regression/missingness_key_old.csv",
        "tests/fixtures/regression/missingness_key_new.csv",
        Some("id"),
    );
}

#[test]
fn key_mode_row_ref_refusals_prefer_key_over_record() {
    let missing_old_path = unique_temp_csv("missing-old");
    let missing_new_path = unique_temp_csv("missing-new");
    std::fs::write(&missing_old_path, "id,amount\nA,100\nB,200.75\n")
        .expect("write missingness old fixture");
    std::fs::write(&missing_new_path, "id,amount\nA,100\nB,\n")
        .expect("write missingness new fixture");

    let missing_json = run_case(
        missing_old_path.to_string_lossy().as_ref(),
        missing_new_path.to_string_lossy().as_ref(),
        Some("id"),
        true,
    );
    let missing_value: Value =
        serde_json::from_str(&missing_json).expect("missingness run should emit JSON");
    assert_eq!(missing_value["outcome"], "REFUSAL");
    assert_eq!(missing_value["refusal"]["code"], "E_MISSINGNESS");
    assert_eq!(missing_value["refusal"]["detail"]["key"], "u8:B");
    assert!(
        missing_value["refusal"]["detail"].get("record").is_none(),
        "key mode missingness detail should not include record"
    );

    let mixed_old_path = unique_temp_csv("mixed-old");
    let mixed_new_path = unique_temp_csv("mixed-new");
    std::fs::write(&mixed_old_path, "id,amount\nA,100\nB,200\n")
        .expect("write mixed-types old fixture");
    std::fs::write(&mixed_new_path, "id,amount\nA,abc\nB,210\n")
        .expect("write mixed-types new fixture");

    let mixed_json = run_case(
        mixed_old_path.to_string_lossy().as_ref(),
        mixed_new_path.to_string_lossy().as_ref(),
        Some("id"),
        true,
    );
    let mixed_value: Value = serde_json::from_str(&mixed_json).expect("mixed-types run JSON");
    assert_eq!(mixed_value["outcome"], "REFUSAL");
    assert_eq!(mixed_value["refusal"]["code"], "E_MIXED_TYPES");
    assert_eq!(mixed_value["refusal"]["detail"]["key"], "u8:A");
    assert!(
        mixed_value["refusal"]["detail"].get("record").is_none(),
        "key mode mixed-types detail should not include record"
    );
}
