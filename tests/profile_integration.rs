use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rvl::cli::args::Args;
use rvl::orchestrator;
use rvl::witness::record::WitnessRecord;

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("rvl_test_profile_integration_{id}_{seq}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_file(dir: &Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path
}

fn run(args: &Args) -> orchestrator::PipelineResult {
    orchestrator::run(args).expect("orchestrator run should succeed")
}

fn run_json(args: &Args) -> serde_json::Value {
    let result = run(args);
    serde_json::from_str(&result.output).expect("json output should parse")
}

fn make_args(old: &Path, new: &Path) -> Args {
    Args {
        old: Some(old.to_path_buf()),
        new: Some(new.to_path_buf()),
        key: None,
        threshold: 0.95,
        tolerance: 1e-9,
        delimiter: None,
        profile: None,
        profile_id: None,
        capsule_out: None,
        json: true,
        no_witness: true,
        describe: false,
        explicit: false,
        schema: false,
        version: false,
        command: None,
    }
}

fn cleanup(dir: &Path) {
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn profile_key_derivation_and_column_scoping_apply() {
    let dir = temp_dir();
    let old = write_file(
        &dir,
        "old.csv",
        "loan_id,balance,rate,ltv,dscr,state\nA,100,5.0,80,1.2,CA\nB,200,6.0,75,1.1,NY\n",
    );
    let new = write_file(
        &dir,
        "new.csv",
        "loan_id,balance,rate,ltv,dscr,state\nA,110,5.0,80,1.2,CA\nB,200,6.1,75,1.1,NY\n",
    );
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [loan_id, balance, rate, ltv, dscr]\nkey: [loan_id]\n",
    );

    let mut args = make_args(&old, &new);
    args.profile = Some(profile);
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REAL_CHANGE");
    assert_eq!(json["alignment"]["mode"], "key");
    assert_eq!(json["alignment"]["key_column"], "u8:loan_id");
    assert_eq!(json["counts"]["columns_old"], 4);
    assert_eq!(json["counts"]["columns_new"], 4);
    assert_eq!(json["counts"]["columns_common"], 4);
    assert_eq!(json["counts"]["numeric_columns"], 4);
    assert!(json["profile_id"].is_null());
    assert!(json["profile_sha256"].is_null());

    cleanup(&dir);
}

#[test]
fn profile_with_columns_not_in_dataset_is_ignored() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "loan_id,balance\nA,100\nB,200\n");
    let new = write_file(&dir, "new.csv", "loan_id,balance\nA,101\nB,200\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [loan_id, balance, missing_one, missing_two]\nkey: [loan_id]\n",
    );

    let mut args = make_args(&old, &new);
    args.profile = Some(profile);
    let json = run_json(&args);
    assert_eq!(json["outcome"], "REAL_CHANGE");
    assert_eq!(json["counts"]["columns_common"], 1);
    assert_eq!(json["counts"]["numeric_columns"], 1);

    cleanup(&dir);
}

#[test]
fn key_conflict_refuses_when_key_flag_and_profile_key_are_both_set() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "loan_id,balance\nA,100\n");
    let new = write_file(&dir, "new.csv", "loan_id,balance\nA,100\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [loan_id, balance]\nkey: [loan_id]\n",
    );

    let mut args = make_args(&old, &new);
    args.profile = Some(profile);
    args.key = Some("loan_id".to_string());
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REFUSAL");
    assert_eq!(json["refusal"]["code"], "E_KEY_CONFLICT");
    assert_eq!(json["refusal"]["detail"]["key_flag"], "loan_id");
    assert_eq!(json["refusal"]["detail"]["profile_key"][0], "loan_id");

    cleanup(&dir);
}

#[test]
fn ambiguous_profile_selectors_refuse_as_domain_error() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,value\nA,1\n");
    let new = write_file(&dir, "new.csv", "id,value\nA,1\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [id, value]\nkey: [id]\n",
    );

    let mut args = make_args(&old, &new);
    args.profile = Some(profile.clone());
    args.profile_id = Some("csv.demo.v0".to_string());
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REFUSAL");
    assert_eq!(json["refusal"]["code"], "E_AMBIGUOUS_PROFILE");
    assert_eq!(
        json["refusal"]["detail"]["profile_path"],
        profile.to_string_lossy().to_string()
    );
    assert_eq!(json["refusal"]["detail"]["profile_id"], "csv.demo.v0");

    cleanup(&dir);
}

#[test]
fn profile_id_not_found_refuses_with_profile_not_found() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,value\nA,1\n");
    let new = write_file(&dir, "new.csv", "id,value\nA,1\n");

    let mut args = make_args(&old, &new);
    args.profile_id = Some(format!(
        "csv.never.exists.{}.v0",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let wanted = args.profile_id.clone().unwrap();
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REFUSAL");
    assert_eq!(json["refusal"]["code"], "E_PROFILE_NOT_FOUND");
    assert_eq!(json["refusal"]["detail"]["profile_id"], wanted);

    cleanup(&dir);
}

#[test]
fn frozen_profile_populates_json_and_witness_metadata() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "loan_id,balance\nA,100\n");
    let new = write_file(&dir, "new.csv", "loan_id,balance\nA,110\n");
    let profile = write_file(
        &dir,
        "frozen.yaml",
        "profile_id: csv.loan_tape.core.v0\nprofile_sha256: sha256:c9d594a1\ninclude_columns: [loan_id, balance]\nkey: [loan_id]\n",
    );

    let mut args = make_args(&old, &new);
    args.profile = Some(profile);
    let result = run(&args);
    let json: serde_json::Value = serde_json::from_str(&result.output).unwrap();

    assert_eq!(json["profile_id"], "csv.loan_tape.core.v0");
    assert_eq!(json["profile_sha256"], "sha256:c9d594a1");

    let mut witness = WitnessRecord::from_run(
        &args,
        &result,
        b"loan_id,balance\nA,100\n",
        b"loan_id,balance\nA,110\n",
        "old.csv",
        "new.csv",
    );
    witness.compute_id();
    assert_eq!(witness.params["profile_id"], "csv.loan_tape.core.v0");
    assert_eq!(witness.params["profile_sha256"], "sha256:c9d594a1");

    cleanup(&dir);
}

#[test]
fn profile_id_can_resolve_from_existing_path_selector() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "loan_id,balance\nA,100\n");
    let new = write_file(&dir, "new.csv", "loan_id,balance\nA,110\n");
    let profile = write_file(
        &dir,
        "frozen.yaml",
        "profile_id: csv.loan_tape.core.v0\nprofile_sha256: sha256:abcd\ninclude_columns: [loan_id, balance]\nkey: [loan_id]\n",
    );

    let mut args = make_args(&old, &new);
    args.profile_id = Some(profile.to_string_lossy().to_string());
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REAL_CHANGE");
    assert_eq!(json["alignment"]["mode"], "key");
    assert_eq!(json["profile_id"], "csv.loan_tape.core.v0");
    assert_eq!(json["profile_sha256"], "sha256:abcd");

    cleanup(&dir);
}

#[test]
fn draft_profile_renders_human_profile_line() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,value\nA,1\n");
    let new = write_file(&dir, "new.csv", "id,value\nA,1\n");
    let profile = write_file(
        &dir,
        "draft.yaml",
        "include_columns: [id, value]\nkey: []\n",
    );

    let mut args = make_args(&old, &new);
    args.profile = Some(profile);
    args.json = false;
    let result = run(&args);

    assert!(result.output.contains("Profile: (draft, no ID)"));

    cleanup(&dir);
}

#[test]
fn profile_with_empty_key_allows_key_flag() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "loan_id,balance\nA,100\nB,200\n");
    let new = write_file(&dir, "new.csv", "loan_id,balance\nA,100\nB,220\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [loan_id, balance]\nkey: []\n",
    );

    let mut args = make_args(&old, &new);
    args.profile = Some(profile);
    args.key = Some("loan_id".to_string());
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REAL_CHANGE");
    assert_eq!(json["alignment"]["mode"], "key");

    cleanup(&dir);
}
