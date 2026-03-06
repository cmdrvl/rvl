use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use rvl::cli::args::Args;
use rvl::orchestrator;
use serde_json::Value;

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("regression")
        .join(name)
}

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("rvl_test_capsule_{id}_{seq}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    std::fs::remove_dir_all(dir).ok();
}

fn write_file(dir: &Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).expect("test file should be writable");
    path
}

fn only_capsule_dir(capsule_root: &Path) -> PathBuf {
    let mut capsule_dirs = std::fs::read_dir(capsule_root)
        .expect("capsule root should exist")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    capsule_dirs.sort();
    assert_eq!(
        capsule_dirs.len(),
        1,
        "expected exactly one capsule directory"
    );
    capsule_dirs.pop().expect("capsule dir")
}

fn run_rvl_binary(current_dir: &Path, home: Option<&Path>, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rvl"));
    command.current_dir(current_dir).args(args);
    if let Some(home) = home {
        command.env("HOME", home);
    }
    command.output().expect("rvl invocation should execute")
}

fn run_replay_script(capsule_dir: &Path, home: Option<&Path>) -> Output {
    let mut command = Command::new("./replay.sh");
    let bin_dir = Path::new(env!("CARGO_BIN_EXE_rvl"))
        .parent()
        .expect("rvl binary path should have parent");
    let path = std::env::var_os("PATH").unwrap_or_default();
    let mut joined_path = std::ffi::OsString::new();
    joined_path.push(bin_dir.as_os_str());
    joined_path.push(":");
    joined_path.push(path);
    command.current_dir(capsule_dir).env("PATH", joined_path);
    if let Some(home) = home {
        command.env("HOME", home);
    }
    command
        .output()
        .expect("capsule replay script should execute")
}

fn run_with_capsule(
    old: &Path,
    new: &Path,
    key: Option<&str>,
    capsule_root: &Path,
) -> (Value, Value, PathBuf) {
    let args = Args {
        old: Some(old.to_path_buf()),
        new: Some(new.to_path_buf()),
        key: key.map(str::to_string),
        threshold: 0.95,
        tolerance: 1e-9,
        delimiter: None,
        profile: None,
        profile_id: None,
        capsule_out: Some(capsule_root.to_path_buf()),
        json: true,
        no_witness: true,
        describe: false,
        explicit: false,
        schema: false,
        version: false,
        command: None,
    };

    let first = orchestrator::run(&args).expect("first run should succeed");
    let first_json: Value = serde_json::from_str(&first.output).expect("first output should parse");

    let capsule_dir = only_capsule_dir(capsule_root);

    let manifest_path = capsule_dir.join("manifest.json");
    let manifest_raw = std::fs::read_to_string(&manifest_path).expect("manifest should exist");
    let manifest: Value = serde_json::from_str(&manifest_raw).expect("manifest should be JSON");

    (first_json, manifest, capsule_dir)
}

fn parse_delimiter(raw: Option<&str>) -> Option<u8> {
    raw.map(|value| {
        let trimmed = value.strip_prefix("0x").unwrap_or(value);
        u8::from_str_radix(trimmed, 16).expect("delimiter should be 0xNN")
    })
}

fn replay_from_manifest(manifest: &Value, capsule_dir: &Path) -> Value {
    let args_block = manifest["args"].as_object().expect("manifest.args object");
    let delimiter = parse_delimiter(args_block.get("delimiter").and_then(Value::as_str));

    let args = Args {
        old: Some(capsule_dir.join("old.csv")),
        new: Some(capsule_dir.join("new.csv")),
        key: args_block
            .get("key")
            .and_then(Value::as_str)
            .map(str::to_string),
        threshold: args_block
            .get("threshold")
            .and_then(Value::as_f64)
            .expect("manifest.args.threshold"),
        tolerance: args_block
            .get("tolerance")
            .and_then(Value::as_f64)
            .expect("manifest.args.tolerance"),
        delimiter,
        profile: args_block
            .get("profile")
            .and_then(Value::as_str)
            .map(PathBuf::from),
        profile_id: args_block
            .get("profile_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        capsule_out: None,
        json: args_block
            .get("json")
            .and_then(Value::as_bool)
            .expect("manifest.args.json"),
        no_witness: true,
        describe: false,
        explicit: false,
        schema: false,
        version: false,
        command: None,
    };

    let replay = orchestrator::run(&args).expect("replay run should succeed");
    serde_json::from_str(&replay.output).expect("replay output should parse")
}

fn assert_manifest_shape(manifest: &Value) {
    let obj = manifest.as_object().expect("manifest object");
    let expected = [
        "version",
        "capsule_id",
        "tool",
        "args",
        "outcome",
        "refusal_code",
        "contributor_summary",
        "replay_command",
        "artifacts",
    ];
    assert_eq!(obj.len(), expected.len(), "manifest key count changed");
    for key in expected {
        assert!(obj.contains_key(key), "missing manifest key: {key}");
    }

    assert_eq!(manifest["version"], "rvl.capsule.v0");
    assert_eq!(manifest["tool"]["name"], "rvl");
    assert!(manifest["tool"]["version"].is_string());
    assert!(manifest["capsule_id"].as_str().unwrap_or("").len() >= 16);
    assert!(
        manifest["replay_command"]
            .as_str()
            .unwrap_or("")
            .starts_with("rvl old.csv new.csv")
    );

    for (artifact_key, expected_path) in [
        ("old_csv", "old.csv"),
        ("new_csv", "new.csv"),
        ("output", "output.txt"),
        ("replay", "replay.sh"),
    ] {
        assert_eq!(manifest["artifacts"][artifact_key]["path"], expected_path);
        assert!(
            manifest["artifacts"][artifact_key]["hash"]
                .as_str()
                .unwrap_or("")
                .starts_with("blake3:")
        );
        assert!(
            manifest["artifacts"][artifact_key]["bytes"]
                .as_u64()
                .expect("artifact bytes")
                > 0
        );
    }

    assert!(manifest["args"].is_object());
    assert!(manifest["contributor_summary"]["count"].is_u64());
    assert!(manifest["contributor_summary"]["coverage"].is_number());
    assert!(manifest["contributor_summary"]["top"].is_array());
}

#[test]
fn capsule_replay_real_change_preserves_outcome_and_contributors() {
    let dir = temp_dir();
    let capsule_root = dir.join("capsules");
    let old = fixture_path("real_change_old.csv");
    let new = fixture_path("real_change_new.csv");

    let (first_json, manifest, capsule_dir) =
        run_with_capsule(&old, &new, Some("id"), &capsule_root);
    assert_manifest_shape(&manifest);

    assert_eq!(first_json["outcome"], "REAL_CHANGE");
    assert_eq!(manifest["outcome"], "REAL_CHANGE");
    assert!(manifest["refusal_code"].is_null());

    let replay_json = replay_from_manifest(&manifest, &capsule_dir);
    assert_eq!(replay_json["outcome"], first_json["outcome"]);
    assert_eq!(
        replay_json["contributors"]
            .as_array()
            .map(|arr| arr.len())
            .unwrap_or(0) as u64,
        manifest["contributor_summary"]["count"]
            .as_u64()
            .expect("contributor count")
    );

    cleanup(&dir);
}

#[test]
fn capsule_replay_refusal_preserves_refusal_code() {
    let dir = temp_dir();
    let capsule_root = dir.join("capsules");
    let old = fixture_path("missingness_key_old.csv");
    let new = fixture_path("missingness_key_new.csv");

    let (first_json, manifest, capsule_dir) =
        run_with_capsule(&old, &new, Some("id"), &capsule_root);
    assert_manifest_shape(&manifest);

    assert_eq!(first_json["outcome"], "REFUSAL");
    let refusal_code = first_json["refusal"]["code"]
        .as_str()
        .expect("refusal code");
    assert_eq!(manifest["outcome"], "REFUSAL");
    assert_eq!(manifest["refusal_code"], refusal_code);

    let replay_json = replay_from_manifest(&manifest, &capsule_dir);
    assert_eq!(replay_json["outcome"], "REFUSAL");
    assert_eq!(replay_json["refusal"]["code"], refusal_code);

    cleanup(&dir);
}

#[test]
fn capsule_replay_script_is_self_contained_for_relative_profile_path() {
    let dir = temp_dir();
    let capsule_root = dir.join("capsules");
    std::fs::create_dir_all(&capsule_root).unwrap();
    write_file(&dir, "old.csv", "loan_id,balance\nA,100\n");
    write_file(&dir, "new.csv", "loan_id,balance\nA,110\n");
    write_file(
        &dir,
        "profile.yaml",
        "profile_id: csv.loan_tape.core.v0\nprofile_sha256: sha256:abc123\ninclude_columns:\n  - loan_id\n  - balance\nkey:\n  - loan_id\n",
    );

    let first = run_rvl_binary(
        &dir,
        None,
        &[
            "old.csv",
            "new.csv",
            "--json",
            "--no-witness",
            "--capsule-out",
            "capsules",
            "--profile",
            "profile.yaml",
        ],
    );
    assert_eq!(first.status.code(), Some(1));
    let first_stderr = String::from_utf8_lossy(&first.stderr);
    assert!(
        first_stderr.trim().is_empty(),
        "unexpected stderr: {}",
        first_stderr
    );
    let first_json: Value = serde_json::from_slice(&first.stdout).expect("first output json");

    let capsule_dir = only_capsule_dir(&capsule_root);
    let manifest: Value = serde_json::from_str(
        &std::fs::read_to_string(capsule_dir.join("manifest.json")).expect("manifest"),
    )
    .expect("manifest json");
    assert_eq!(manifest["artifacts"]["profile"]["path"], "profile.yaml");
    assert_eq!(
        manifest["replay_command"],
        "rvl old.csv new.csv --profile profile.yaml --threshold 0.95 --tolerance 0.000000001 --json --no-witness"
    );

    let replay = run_replay_script(&capsule_dir, None);
    assert_eq!(replay.status.code(), Some(1));
    let replay_stderr = String::from_utf8_lossy(&replay.stderr);
    assert!(
        replay_stderr.trim().is_empty(),
        "unexpected replay stderr: {}",
        replay_stderr
    );
    let replay_json: Value = serde_json::from_slice(&replay.stdout).expect("replay output json");
    assert_eq!(replay_json["outcome"], first_json["outcome"]);
    assert_eq!(replay_json["profile_id"], first_json["profile_id"]);
    assert_eq!(replay_json["profile_sha256"], first_json["profile_sha256"]);

    cleanup(&dir);
}

#[test]
fn capsule_replay_script_is_self_contained_for_profile_id_runs() {
    let dir = temp_dir();
    let capsule_root = dir.join("capsules");
    let home = dir.join("home");
    let profiles_dir = home.join(".epistemic").join("profiles");
    std::fs::create_dir_all(&profiles_dir).unwrap();
    write_file(&dir, "old.csv", "loan_id,balance\nA,100\n");
    write_file(&dir, "new.csv", "loan_id,balance\nA,110\n");
    write_file(
        &profiles_dir,
        "loan_tape.yaml",
        "profile_id: csv.loan_tape.core.v0\nprofile_sha256: sha256:abc123\ninclude_columns:\n  - loan_id\n  - balance\nkey:\n  - loan_id\n",
    );

    let first = run_rvl_binary(
        &dir,
        Some(&home),
        &[
            "old.csv",
            "new.csv",
            "--json",
            "--no-witness",
            "--capsule-out",
            "capsules",
            "--profile-id",
            "csv.loan_tape.core.v0",
        ],
    );
    assert_eq!(first.status.code(), Some(1));
    let first_json: Value = serde_json::from_slice(&first.stdout).expect("first output json");

    let capsule_dir = only_capsule_dir(&capsule_root);
    let manifest: Value = serde_json::from_str(
        &std::fs::read_to_string(capsule_dir.join("manifest.json")).expect("manifest"),
    )
    .expect("manifest json");
    assert_eq!(manifest["artifacts"]["profile"]["path"], "profile.yaml");
    assert_eq!(manifest["args"]["profile_id"], "csv.loan_tape.core.v0");

    let replay_home = dir.join("empty-home");
    std::fs::create_dir_all(&replay_home).unwrap();
    let replay = run_replay_script(&capsule_dir, Some(&replay_home));
    assert_eq!(replay.status.code(), Some(1));
    let replay_stderr = String::from_utf8_lossy(&replay.stderr);
    assert!(
        replay_stderr.trim().is_empty(),
        "unexpected replay stderr: {}",
        replay_stderr
    );
    let replay_json: Value = serde_json::from_slice(&replay.stdout).expect("replay output json");
    assert_eq!(replay_json["outcome"], first_json["outcome"]);
    assert_eq!(replay_json["profile_id"], first_json["profile_id"]);
    assert_eq!(replay_json["profile_sha256"], first_json["profile_sha256"]);

    cleanup(&dir);
}
