use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rvl::cli::args::Args;
use rvl::orchestrator;
use rvl::witness::record::WitnessRecord;
use serde_json::Value;

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("rvl_test_exhaustive_{id}_{seq}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    std::fs::remove_dir_all(dir).ok();
}

fn write_file(dir: &Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&path, content).unwrap();
    path
}

fn write_column_registry(dir: &Path) {
    write_file(
        dir,
        "registries/annex_columns_v0/registry.json",
        r#"{"id":"annex-columns-v0","version":"1.0.0"}"#,
    );
    write_file(
        dir,
        "registries/annex_columns_v0/aliases.json",
        r#"[
  {"input":"Loan Number","canonical_id":"loan_id_number","canonical_type":"column_name","rule_id":"alias"},
  {"input":"Status","canonical_id":"loan_status","canonical_type":"column_name","rule_id":"alias"},
  {"input":"Balance","canonical_id":"current_balance","canonical_type":"column_name","rule_id":"alias"}
]"#,
    );
}

fn make_args(old: &Path, new: &Path) -> Args {
    Args {
        old: Some(old.to_path_buf()),
        new: Some(new.to_path_buf()),
        key: None,
        threshold: 0.95,
        tolerance: 1e-9,
        delimiter: None,
        exhaustive: false,
        audit_fields: false,
        max_audit_changes: 10_000,
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

fn run_json(args: &Args) -> Value {
    let result = orchestrator::run(args).expect("run should succeed");
    serde_json::from_str(&result.output).expect("json output should parse")
}

#[test]
fn exhaustive_key_mode_emits_all_numeric_changes_with_json_shape_and_redaction() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,a,b\nA,0,0\nB,0,0\n");
    let new = write_file(&dir, "new.csv", "id,a,b\nA,1,1\nB,1,1\n");

    let mut args = make_args(&old, &new);
    args.key = Some("id".to_string());
    args.exhaustive = true;
    let result = orchestrator::run(&args).expect("run should succeed");
    let json: Value = serde_json::from_str(&result.output).unwrap();

    assert_eq!(json["outcome"], "REAL_CHANGE");
    assert_eq!(json["mode"], "exhaustive_numeric");
    assert_eq!(json["audit"]["numeric_changes_emitted"], 4);
    assert_eq!(json["audit"]["field_changes_emitted"], 0);
    assert_eq!(json["audit"]["truncated"], false);
    assert_eq!(json["metrics"]["top_k_coverage"], 1.0);
    assert_eq!(json["contributors"].as_array().unwrap().len(), 4);
    assert_eq!(json["contributors"][0]["row_id"], "u8:A");
    assert_eq!(json["contributors"][0]["column"], "u8:a");
    assert!(json["contributors"][0]["old"].is_null());

    let witness = WitnessRecord::from_run(
        &args,
        &result,
        b"id,a,b\nA,0,0\nB,0,0\n",
        b"id,a,b\nA,1,1\nB,1,1\n",
        "old.csv",
        "new.csv",
    );
    assert_eq!(witness.params["exhaustive"], true);
    assert_eq!(witness.params["max_audit_changes"], 10_000);

    cleanup(&dir);
}

#[test]
fn exhaustive_explicit_output_includes_numeric_values() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,a\nA,2\n");
    let new = write_file(&dir, "new.csv", "id,a\nA,5\n");

    let mut args = make_args(&old, &new);
    args.key = Some("id".to_string());
    args.exhaustive = true;
    args.explicit = true;
    let json = run_json(&args);

    assert_eq!(json["contributors"][0]["old"], 2.0);
    assert_eq!(json["contributors"][0]["new"], 5.0);
    assert_eq!(json["contributors"][0]["delta"], 3.0);
    assert_eq!(json["contributors"][0]["contribution"], 3.0);

    cleanup(&dir);
}

#[test]
fn exhaustive_no_change_reports_no_real_change_with_empty_audit() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,a\nA,2\n");
    let new = write_file(&dir, "new.csv", "id,a\nA,2\n");

    let mut args = make_args(&old, &new);
    args.key = Some("id".to_string());
    args.exhaustive = true;
    let json = run_json(&args);

    assert_eq!(json["outcome"], "NO_REAL_CHANGE");
    assert_eq!(json["mode"], "exhaustive_numeric");
    assert_eq!(json["audit"]["numeric_changes_emitted"], 0);
    assert!(json["contributors"].as_array().unwrap().is_empty());

    cleanup(&dir);
}

#[test]
fn exhaustive_row_order_mode_uses_row_indices() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "a,b\n0,0\n0,0\n");
    let new = write_file(&dir, "new.csv", "a,b\n1,0\n0,1\n");

    let mut args = make_args(&old, &new);
    args.exhaustive = true;
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REAL_CHANGE");
    assert_eq!(json["alignment"]["mode"], "row_order");
    assert_eq!(json["contributors"].as_array().unwrap().len(), 2);
    assert_eq!(json["contributors"][0]["row_id"], "u8:1");
    assert_eq!(json["contributors"][0]["column"], "u8:a");
    assert_eq!(json["contributors"][1]["row_id"], "u8:2");
    assert_eq!(json["contributors"][1]["column"], "u8:b");

    cleanup(&dir);
}

#[test]
fn broad_diffuse_change_is_real_change_in_exhaustive_mode() {
    let dir = temp_dir();
    let headers = (1..=30)
        .map(|idx| format!("v{idx}"))
        .collect::<Vec<_>>()
        .join(",");
    let old_values = vec!["0"; 30].join(",");
    let new_values = vec!["1"; 30].join(",");
    let old = write_file(&dir, "old.csv", &format!("id,{headers}\nA,{old_values}\n"));
    let new = write_file(&dir, "new.csv", &format!("id,{headers}\nA,{new_values}\n"));

    let mut default_args = make_args(&old, &new);
    default_args.key = Some("id".to_string());
    let default_json = run_json(&default_args);
    assert_eq!(default_json["outcome"], "REFUSAL");
    assert_eq!(default_json["refusal"]["code"], "E_DIFFUSE");

    let mut exhaustive_args = default_args.clone();
    exhaustive_args.exhaustive = true;
    let exhaustive_json = run_json(&exhaustive_args);
    assert_eq!(exhaustive_json["outcome"], "REAL_CHANGE");
    assert_eq!(
        exhaustive_json["contributors"].as_array().unwrap().len(),
        30
    );
    assert_eq!(exhaustive_json["audit"]["numeric_changes_emitted"], 30);

    cleanup(&dir);
}

#[test]
fn exhaustive_refuses_when_audit_limit_is_exceeded() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,a,b,c\nA,0,0,0\n");
    let new = write_file(&dir, "new.csv", "id,a,b,c\nA,1,1,1\n");

    let mut args = make_args(&old, &new);
    args.key = Some("id".to_string());
    args.exhaustive = true;
    args.max_audit_changes = 2;
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REFUSAL");
    assert_eq!(json["mode"], "exhaustive_numeric");
    assert_eq!(json["refusal"]["code"], "E_AUDIT_LIMIT");
    assert_eq!(json["refusal"]["detail"]["changed_cells"], 3);
    assert_eq!(json["refusal"]["detail"]["max_audit_changes"], 2);
    assert!(json["audit"].is_null());

    cleanup(&dir);
}

#[test]
fn exhaustive_capsule_replay_preserves_output_shape() {
    let dir = temp_dir();
    let capsule_root = dir.join("capsules");
    let old = write_file(&dir, "old.csv", "id,a,b\nA,0,0\n");
    let new = write_file(&dir, "new.csv", "id,a,b\nA,1,1\n");

    let mut args = make_args(&old, &new);
    args.key = Some("id".to_string());
    args.exhaustive = true;
    args.max_audit_changes = 50;
    args.capsule_out = Some(capsule_root.clone());
    let first = orchestrator::run(&args).expect("first run should succeed");
    let first_json: Value = serde_json::from_str(&first.output).unwrap();

    let mut capsule_dirs = std::fs::read_dir(&capsule_root)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    capsule_dirs.sort();
    let capsule_dir = capsule_dirs.pop().expect("capsule dir");
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(capsule_dir.join("manifest.json")).unwrap())
            .unwrap();

    assert_eq!(manifest["args"]["exhaustive"], true);
    assert_eq!(manifest["args"]["max_audit_changes"], 50);
    assert!(
        manifest["replay_command"]
            .as_str()
            .unwrap()
            .contains("--exhaustive --max-audit-changes 50")
    );

    let mut replay_args = make_args(&capsule_dir.join("old.csv"), &capsule_dir.join("new.csv"));
    replay_args.key = Some("id".to_string());
    replay_args.exhaustive = true;
    replay_args.max_audit_changes = 50;
    let replay_json = run_json(&replay_args);

    assert_eq!(replay_json["outcome"], first_json["outcome"]);
    assert_eq!(replay_json["mode"], first_json["mode"]);
    assert_eq!(
        replay_json["audit"]["numeric_changes_emitted"],
        first_json["audit"]["numeric_changes_emitted"]
    );

    cleanup(&dir);
}

#[test]
fn audit_fields_without_exhaustive_refuses_deterministically() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,status\nA,open\n");
    let new = write_file(&dir, "new.csv", "id,status\nA,closed\n");

    let mut args = make_args(&old, &new);
    args.audit_fields = true;
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REFUSAL");
    assert_eq!(
        json["refusal"]["code"],
        "E_AUDIT_FIELDS_REQUIRES_EXHAUSTIVE"
    );

    cleanup(&dir);
}

#[test]
fn audit_fields_without_profile_refuses() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,status\nA,open\n");
    let new = write_file(&dir, "new.csv", "id,status\nA,closed\n");

    let mut args = make_args(&old, &new);
    args.key = Some("id".to_string());
    args.exhaustive = true;
    args.audit_fields = true;
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REFUSAL");
    assert_eq!(json["refusal"]["code"], "E_AUDIT_FIELDS_REQUIRES_PROFILE");

    cleanup(&dir);
}

#[test]
fn audit_fields_string_only_change_returns_real_change_redacted() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,status\nA,open\n");
    let new = write_file(&dir, "new.csv", "id,status\nA,closed\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [id, status]\nkey: [id]\n",
    );

    let mut args = make_args(&old, &new);
    args.exhaustive = true;
    args.audit_fields = true;
    args.profile = Some(profile);
    let result = orchestrator::run(&args).expect("run should succeed");
    let json: Value = serde_json::from_str(&result.output).unwrap();

    assert_eq!(json["outcome"], "REAL_CHANGE");
    assert_eq!(json["contributors"].as_array().unwrap().len(), 0);
    assert_eq!(json["audit"]["numeric_changes_emitted"], 0);
    assert_eq!(json["audit"]["field_changes_emitted"], 1);
    assert_eq!(json["field_changes"][0]["row_id"], "u8:A");
    assert_eq!(json["field_changes"][0]["column"], "u8:status");
    assert!(json["field_changes"][0]["old"].is_null());
    assert!(json["field_changes"][0]["new"].is_null());

    let witness = WitnessRecord::from_run(
        &args,
        &result,
        b"id,status\nA,open\n",
        b"id,status\nA,closed\n",
        "old.csv",
        "new.csv",
    );
    assert_eq!(witness.params["audit_fields"], true);

    cleanup(&dir);
}

#[test]
fn audit_fields_no_changes_returns_no_real_change_with_empty_field_changes() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,status\nA,open\n");
    let new = write_file(&dir, "new.csv", "id,status\nA,open\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [id, status]\nkey: [id]\n",
    );

    let mut args = make_args(&old, &new);
    args.exhaustive = true;
    args.audit_fields = true;
    args.profile = Some(profile);
    let json = run_json(&args);

    assert_eq!(json["outcome"], "NO_REAL_CHANGE");
    assert_eq!(json["audit"]["numeric_changes_emitted"], 0);
    assert_eq!(json["audit"]["field_changes_emitted"], 0);
    assert!(json["field_changes"].as_array().unwrap().is_empty());

    cleanup(&dir);
}

#[test]
fn audit_fields_explicit_output_includes_old_new_values() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,status\nA,open\n");
    let new = write_file(&dir, "new.csv", "id,status\nA,closed\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [id, status]\nkey: [id]\n",
    );

    let mut args = make_args(&old, &new);
    args.exhaustive = true;
    args.audit_fields = true;
    args.explicit = true;
    args.profile = Some(profile);
    let json = run_json(&args);

    assert_eq!(json["field_changes"][0]["old"], "u8:open");
    assert_eq!(json["field_changes"][0]["new"], "u8:closed");

    cleanup(&dir);
}

#[test]
fn audit_fields_mixed_numeric_and_field_changes_emit_separate_sections() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,balance,status\nA,100,open\n");
    let new = write_file(&dir, "new.csv", "id,balance,status\nA,125,closed\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [id, balance, status]\nkey: [id]\n",
    );

    let mut args = make_args(&old, &new);
    args.exhaustive = true;
    args.audit_fields = true;
    args.profile = Some(profile);
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REAL_CHANGE");
    assert_eq!(json["contributors"].as_array().unwrap().len(), 1);
    assert_eq!(json["contributors"][0]["column"], "u8:balance");
    assert_eq!(json["field_changes"].as_array().unwrap().len(), 1);
    assert_eq!(json["field_changes"][0]["column"], "u8:status");
    assert_eq!(json["audit"]["numeric_changes_emitted"], 1);
    assert_eq!(json["audit"]["field_changes_emitted"], 1);

    cleanup(&dir);
}

#[test]
fn audit_fields_uses_column_registry_canonical_headers() {
    let dir = temp_dir();
    write_column_registry(&dir);
    let old = write_file(&dir, "old.csv", "Loan Number,Status,Balance\nA,open,100\n");
    let new = write_file(
        &dir,
        "new.csv",
        "Loan Number,Status,Balance\nA,closed,100\n",
    );
    let profile = write_file(
        &dir,
        "profile.yaml",
        "column_registry: registries/annex_columns_v0\ninclude_columns: [loan_id_number, loan_status, current_balance]\nkey: [loan_id_number]\n",
    );

    let mut args = make_args(&old, &new);
    args.exhaustive = true;
    args.audit_fields = true;
    args.profile = Some(profile);
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REAL_CHANGE");
    assert_eq!(json["alignment"]["key_column"], "u8:loan_id_number");
    assert_eq!(json["field_changes"][0]["column"], "u8:loan_status");

    cleanup(&dir);
}

#[test]
fn audit_fields_limit_counts_numeric_and_field_changes_together() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,balance,status\nA,100,open\n");
    let new = write_file(&dir, "new.csv", "id,balance,status\nA,125,closed\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [id, balance, status]\nkey: [id]\n",
    );

    let mut args = make_args(&old, &new);
    args.exhaustive = true;
    args.audit_fields = true;
    args.max_audit_changes = 1;
    args.profile = Some(profile);
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REFUSAL");
    assert_eq!(json["refusal"]["code"], "E_AUDIT_LIMIT");
    assert_eq!(json["refusal"]["detail"]["changed_cells"], 2);
    assert_eq!(json["refusal"]["detail"]["max_audit_changes"], 1);

    cleanup(&dir);
}

#[test]
fn audit_fields_ordering_is_row_id_then_column() {
    let dir = temp_dir();
    let old = write_file(
        &dir,
        "old.csv",
        "id,status,kind\nB,open,warehouse\nA,open,retail\n",
    );
    let new = write_file(
        &dir,
        "new.csv",
        "id,status,kind\nB,closed,warehouse\nA,closed,office\n",
    );
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [id, status, kind]\nkey: [id]\n",
    );

    let mut args = make_args(&old, &new);
    args.exhaustive = true;
    args.audit_fields = true;
    args.profile = Some(profile);
    let json = run_json(&args);
    let fields = json["field_changes"].as_array().unwrap();

    assert_eq!(fields[0]["row_id"], "u8:A");
    assert_eq!(fields[0]["column"], "u8:kind");
    assert_eq!(fields[1]["row_id"], "u8:A");
    assert_eq!(fields[1]["column"], "u8:status");
    assert_eq!(fields[2]["row_id"], "u8:B");
    assert_eq!(fields[2]["column"], "u8:status");

    cleanup(&dir);
}

#[test]
fn audit_fields_preserves_mixed_types_refusal() {
    let dir = temp_dir();
    let old = write_file(&dir, "old.csv", "id,status\nA,1\nB,open\n");
    let new = write_file(&dir, "new.csv", "id,status\nA,2\nB,closed\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [id, status]\nkey: [id]\n",
    );

    let mut args = make_args(&old, &new);
    args.exhaustive = true;
    args.audit_fields = true;
    args.profile = Some(profile);
    let json = run_json(&args);

    assert_eq!(json["outcome"], "REFUSAL");
    assert_eq!(json["refusal"]["code"], "E_MIXED_TYPES");

    cleanup(&dir);
}

#[test]
fn audit_fields_capsule_replay_preserves_field_audit_flags() {
    let dir = temp_dir();
    let capsule_root = dir.join("capsules");
    let old = write_file(&dir, "old.csv", "id,status\nA,open\n");
    let new = write_file(&dir, "new.csv", "id,status\nA,closed\n");
    let profile = write_file(
        &dir,
        "profile.yaml",
        "include_columns: [id, status]\nkey: [id]\n",
    );

    let mut args = make_args(&old, &new);
    args.exhaustive = true;
    args.audit_fields = true;
    args.profile = Some(profile);
    args.capsule_out = Some(capsule_root.clone());
    let first = orchestrator::run(&args).expect("first run should succeed");
    let first_json: Value = serde_json::from_str(&first.output).unwrap();

    let mut capsule_dirs = std::fs::read_dir(&capsule_root)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    capsule_dirs.sort();
    let capsule_dir = capsule_dirs.pop().expect("capsule dir");
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(capsule_dir.join("manifest.json")).unwrap())
            .unwrap();

    assert_eq!(manifest["args"]["audit_fields"], true);
    assert!(
        manifest["replay_command"]
            .as_str()
            .unwrap()
            .contains("--audit-fields")
    );

    let mut replay_args = make_args(&capsule_dir.join("old.csv"), &capsule_dir.join("new.csv"));
    replay_args.exhaustive = true;
    replay_args.audit_fields = true;
    replay_args.profile = Some(capsule_dir.join("profile.yaml"));
    let replay_json = run_json(&replay_args);

    assert_eq!(replay_json["outcome"], first_json["outcome"]);
    assert_eq!(
        replay_json["field_changes"].as_array().unwrap().len(),
        first_json["field_changes"].as_array().unwrap().len()
    );

    cleanup(&dir);
}
