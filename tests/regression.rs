use std::path::PathBuf;

use rvl::cli::args::Args;
use rvl::orchestrator;
use serde_json::Value;

const REGRESSION_DIR: &str = "tests/fixtures/regression";

fn run_case(old: &str, new: &str, key: Option<&str>, json: bool) -> String {
    let args = Args {
        old: PathBuf::from(old),
        new: PathBuf::from(new),
        key: key.map(|value| value.to_string()),
        threshold: 0.95,
        tolerance: 1e-9,
        delimiter: None,
        json,
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
