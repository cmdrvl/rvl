mod helpers;

use std::path::PathBuf;

use rvl::cli::args::Args;
use rvl::cli::exit::Outcome;
use rvl::orchestrator;

fn set_manifest_cwd() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    std::env::set_current_dir(&manifest_dir).expect("set current dir to manifest");
}

fn regression_args(json: bool) -> Args {
    Args {
        old: PathBuf::from("tests/fixtures/regression/basic_old.csv"),
        new: PathBuf::from("tests/fixtures/regression/basic_new.csv"),
        key: None,
        threshold: 0.95,
        tolerance: 1e-9,
        delimiter: None,
        json,
    }
}

#[test]
fn regression_basic_human_output() {
    set_manifest_cwd();
    let args = regression_args(false);
    let result = orchestrator::run(&args).expect("run regression case");
    assert_eq!(result.outcome, Outcome::RealChange);

    let expected = String::from_utf8(helpers::read_fixture("regression/basic.human.txt"))
        .expect("expected human output utf-8");
    let expected = expected.trim_end_matches('\n');
    assert_eq!(result.output, expected);
}

#[test]
fn regression_basic_json_output() {
    set_manifest_cwd();
    let args = regression_args(true);
    let result = orchestrator::run(&args).expect("run regression case");
    assert_eq!(result.outcome, Outcome::RealChange);

    let expected: serde_json::Value =
        serde_json::from_slice(&helpers::read_fixture("regression/basic.json"))
            .expect("expected json output");
    let actual: serde_json::Value =
        serde_json::from_str(&result.output).expect("actual json output");
    assert_eq!(actual, expected);
}
