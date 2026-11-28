//! bd-k3c: `--lock` input verification (parity with shape/verify/benchmark).
//!
//! Two kinds of lockfile are used on purpose: REAL ones produced by the installed
//! `vacuum | hashbytes | lock` pipeline (member paths are relative to the vacuum root, so these
//! exercise the basename-match path that real lockfiles depend on), and hand-written ones for the
//! cases the tools cannot produce (stale hash, malformed file, wrong version).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::Value;

fn temp_dir(label: &str) -> PathBuf {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("rvl_lock_{label}_{}_{n}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn rvl(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .args(args)
        .output()
        .expect("run rvl")
}

fn rvl_in(current_dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .current_dir(current_dir)
        .args(args)
        .output()
        .expect("run rvl")
}

fn json(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(&out.stdout)))
}

fn s(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// Two CSVs with a real numeric change, in their own directory.
fn dataset(label: &str) -> (PathBuf, PathBuf, PathBuf) {
    let dir = temp_dir(label);
    let old = dir.join("old.csv");
    let new = dir.join("new.csv");
    fs::write(&old, "id,v\n1,10\n2,20\n").unwrap();
    fs::write(&new, "id,v\n1,10\n2,99\n").unwrap();
    (dir, old, new)
}

fn have(tool: &str) -> bool {
    Command::new(tool).arg("--version").output().is_ok()
}

/// `vacuum --json <root> | hashbytes | lock --dataset-id t --output <out>`; `None` when the
/// toolchain is not installed (the caller then reports the skip loudly).
fn real_lock(root: &Path, out: &Path) -> Option<()> {
    if !(have("vacuum") && have("hashbytes") && have("lock")) {
        eprintln!(
            "SKIPPED: vacuum/hashbytes/lock not all on PATH; real-lockfile half not exercised"
        );
        return None;
    }
    let mut vacuum = Command::new("vacuum")
        .args(["--json", root.file_name().unwrap().to_str().unwrap()])
        .current_dir(root.parent().unwrap())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("vacuum");
    let mut hash = Command::new("hashbytes")
        .stdin(vacuum.stdout.take().unwrap())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("hashbytes");
    let status = Command::new("lock")
        .args(["--dataset-id", "t", "--no-witness", "--output", s(out)])
        .stdin(hash.stdout.take().unwrap())
        .stderr(Stdio::null())
        .status()
        .expect("lock");
    assert!(status.success(), "lock failed to produce a lockfile");
    assert!(
        vacuum.wait().expect("wait vacuum").success(),
        "vacuum failed"
    );
    assert!(
        hash.wait().expect("wait hashbytes").success(),
        "hashbytes failed"
    );
    Some(())
}

fn lock_hash_of(lockfile: &Path) -> String {
    let doc: Value = serde_json::from_str(&fs::read_to_string(lockfile).unwrap()).unwrap();
    doc["lock_hash"]
        .as_str()
        .expect("real lockfile has lock_hash")
        .to_string()
}

// ── real lockfiles ──────────────────────────────────────────────────────────

#[test]
fn real_lockfile_verifies_inputs_and_reports_its_identity() {
    let (dir, old, new) = dataset("real_ok");
    let lock = dir.join("inputs.lock.json");
    if real_lock(&dir, &lock).is_none() {
        return;
    }
    let out = rvl(&[
        s(&old),
        s(&new),
        "--lock",
        s(&lock),
        "--json",
        "--no-witness",
    ]);
    let v = json(&out);
    assert_eq!(v["outcome"], "REAL_CHANGE", "{v}");
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(v["lock"][0]["lockfile"], s(&lock));
    assert_eq!(v["lock"][0]["lock_hash"], lock_hash_of(&lock));
}

#[test]
fn real_lockfile_detects_drift_and_next_command_runs() {
    let (dir, old, new) = dataset("real_drift");
    let lock = dir.join("inputs.lock.json");
    if real_lock(&dir, &lock).is_none() {
        return;
    }
    // Planted drift: change a byte after the lock was taken.
    fs::write(&new, "id,v\n1,10\n2,100\n").unwrap();

    let out = rvl(&[
        s(&old),
        s(&new),
        "--lock",
        s(&lock),
        "--json",
        "--no-witness",
    ]);
    let v = json(&out);
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(v["refusal"]["code"], "E_INPUT_DRIFT", "{v}");
    let detail = &v["refusal"]["detail"];
    assert_eq!(detail["file"], "new");
    assert!(
        detail["expected_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_ne!(detail["expected_hash"], detail["actual_hash"]);
    assert!(
        v.get("lock").is_none(),
        "nothing was verified, so no lock receipt"
    );

    // The advertised next_command must be a real, runnable `lock verify` that also flags the drift.
    let next = v["refusal"]["next_command"].as_str().expect("next_command");
    let argv: Vec<&str> = next.split_whitespace().collect();
    assert_eq!(argv[0], "lock", "{next}");
    let ran = Command::new(argv[0])
        .args(&argv[1..])
        .output()
        .expect("lock verify");
    assert_eq!(
        ran.status.code(),
        Some(1),
        "`{next}` should report the drifted member (exit 1): {}",
        String::from_utf8_lossy(&ran.stdout)
    );
}

#[test]
fn real_lockfile_refuses_input_it_does_not_pin() {
    let (dir, old, new) = dataset("real_missing");
    // Lock a directory that only contains old.csv.
    let only_old = temp_dir("real_missing_only_old");
    fs::copy(&old, only_old.join("old.csv")).unwrap();
    let lock = dir.join("only_old.lock.json");
    if real_lock(&only_old, &lock).is_none() {
        return;
    }
    let out = rvl(&[
        s(&old),
        s(&new),
        "--lock",
        s(&lock),
        "--json",
        "--no-witness",
    ]);
    let v = json(&out);
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(v["refusal"]["code"], "E_INPUT_NOT_LOCKED", "{v}");
    assert_eq!(v["refusal"]["detail"]["file"], "new");
}

// ── hand-written lockfiles ──────────────────────────────────────────────────

fn sha256_of(path: &Path) -> String {
    use sha2::{Digest, Sha256};
    format!("sha256:{:x}", Sha256::digest(fs::read(path).unwrap()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn write_lock(dir: &Path, name: &str, members: &[(&str, String)]) -> PathBuf {
    let members: Vec<Value> = members
        .iter()
        .map(|(path, hash)| serde_json::json!({"path": path, "bytes_hash": hash}))
        .collect();
    let path = dir.join(name);
    fs::write(
        &path,
        serde_json::json!({"version": "lock.v0", "lock_hash": "sha256:feed", "members": members})
            .to_string(),
    )
    .unwrap();
    path
}

#[test]
fn exact_absolute_path_member_verifies() {
    let (dir, old, new) = dataset("abs_ok");
    let lock = write_lock(
        &dir,
        "abs.lock.json",
        &[(s(&old), sha256_of(&old)), (s(&new), sha256_of(&new))],
    );
    let v = json(&rvl(&[
        s(&old),
        s(&new),
        "--lock",
        s(&lock),
        "--json",
        "--no-witness",
    ]));
    assert_eq!(v["outcome"], "REAL_CHANGE");
    assert_eq!(v["lock"][0]["lock_hash"], "sha256:feed");
}

#[test]
fn stale_hash_refuses_drift_and_unlocked_input_refuses_not_locked() {
    let (dir, old, new) = dataset("neg");
    let stale_hash = sha256_bytes(b"stale bytes");
    let stale = write_lock(
        &dir,
        "stale.lock.json",
        &[(s(&old), sha256_of(&old)), (s(&new), stale_hash.clone())],
    );
    let out = rvl(&[
        s(&old),
        s(&new),
        "--lock",
        s(&stale),
        "--json",
        "--no-witness",
    ]);
    assert_eq!(out.status.code(), Some(2));
    let v = json(&out);
    assert_eq!(v["refusal"]["code"], "E_INPUT_DRIFT");
    assert_eq!(v["refusal"]["detail"]["expected_hash"], stale_hash);

    let partial = write_lock(&dir, "partial.lock.json", &[(s(&old), sha256_of(&old))]);
    let out = rvl(&[
        s(&old),
        s(&new),
        "--lock",
        s(&partial),
        "--json",
        "--no-witness",
    ]);
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(json(&out)["refusal"]["code"], "E_INPUT_NOT_LOCKED");
}

#[test]
fn malformed_or_missing_lockfile_refuses_io_naming_the_lockfile() {
    let (dir, old, new) = dataset("io");
    let bad = dir.join("bad.lock.json");
    fs::write(&bad, "not json").unwrap();
    for lock in [bad, dir.join("absent.lock.json")] {
        let out = rvl(&[
            s(&old),
            s(&new),
            "--lock",
            s(&lock),
            "--json",
            "--no-witness",
        ]);
        assert_eq!(out.status.code(), Some(2));
        let v = json(&out);
        assert_eq!(v["refusal"]["code"], "E_IO", "{v}");
        assert_eq!(v["refusal"]["detail"]["lockfile"], s(&lock));
    }
}

#[test]
fn repeated_lock_flag_combines_lockfiles() {
    let (dir, old, new) = dataset("multi");
    let a = write_lock(&dir, "a.lock.json", &[(s(&old), sha256_of(&old))]);
    let b = write_lock(&dir, "b.lock.json", &[(s(&new), sha256_of(&new))]);
    let v = json(&rvl(&[
        s(&old),
        s(&new),
        "--lock",
        s(&a),
        "--lock",
        s(&b),
        "--json",
        "--no-witness",
    ]));
    assert_eq!(v["outcome"], "REAL_CHANGE", "{v}");
    assert_eq!(v["lock"].as_array().unwrap().len(), 2);
}

#[test]
fn conflicting_digests_across_two_locks_refuse_drift() {
    let (dir, old, _new) = dataset("conflicting_locks");
    let matching = sha256_of(&old);
    let differing = sha256_bytes(b"different bytes");
    let first = write_lock(&dir, "first.lock.json", &[(s(&old), matching)]);
    let second = write_lock(&dir, "second.lock.json", &[(s(&old), differing)]);

    let out = rvl(&[
        s(&old),
        s(&old),
        "--lock",
        s(&first),
        "--lock",
        s(&second),
        "--json",
        "--no-witness",
    ]);
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(json(&out)["refusal"]["code"], "E_INPUT_DRIFT");
}

#[test]
fn parent_folder_locking_accepts_dot_paths_and_refuses_swapped_periods() {
    let dir = temp_dir("parent_periods");
    fs::create_dir_all(dir.join("dec")).unwrap();
    fs::create_dir_all(dir.join("nov")).unwrap();
    fs::write(dir.join("dec/loans.csv"), "loan_id,balance\nA,100\n").unwrap();
    fs::write(dir.join("nov/loans.csv"), "loan_id,balance\nA,110\n").unwrap();
    let lock = write_lock(
        &dir,
        "parent.lock.json",
        &[
            ("dec/loans.csv", sha256_of(&dir.join("dec/loans.csv"))),
            ("nov/loans.csv", sha256_of(&dir.join("nov/loans.csv"))),
        ],
    );
    let args = [
        "./dec/loans.csv",
        "./nov/loans.csv",
        "--lock",
        s(&lock),
        "--json",
        "--no-witness",
    ];

    let right = rvl_in(&dir, &args);
    assert_eq!(right.status.code(), Some(1));
    assert_eq!(json(&right)["outcome"], "REAL_CHANGE");

    fs::write(dir.join("dec/loans.csv"), "loan_id,balance\nA,110\n").unwrap();
    fs::write(dir.join("nov/loans.csv"), "loan_id,balance\nA,100\n").unwrap();
    let swapped = rvl_in(&dir, &args);
    assert_eq!(swapped.status.code(), Some(2));
    assert_eq!(json(&swapped)["refusal"]["code"], "E_INPUT_DRIFT");
}

#[cfg(not(windows))]
#[test]
fn unix_typed_backslash_is_not_a_path_separator() {
    let dir = temp_dir("unix_backslash");
    let input = dir.join(r"e\loans.csv");
    fs::write(&input, "loan_id,balance\nA,100\n").unwrap();
    let lock = write_lock(
        &dir,
        "slash.lock.json",
        &[("e/loans.csv", sha256_of(&input))],
    );
    let out = rvl_in(
        &dir,
        &[
            r"e\loans.csv",
            r"e\loans.csv",
            "--lock",
            s(&lock),
            "--json",
            "--no-witness",
        ],
    );
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(json(&out)["refusal"]["code"], "E_INPUT_NOT_LOCKED");
}

#[test]
fn without_lock_flag_output_has_no_lock_field_and_comparison_is_unchanged() {
    let (_dir, old, new) = dataset("nolock");
    let v = json(&rvl(&[s(&old), s(&new), "--json", "--no-witness"]));
    assert_eq!(v["outcome"], "REAL_CHANGE");
    assert!(v.get("lock").is_none());
}

#[test]
fn witness_record_carries_lock_paths_and_verified_identities() {
    let (dir, old, new) = dataset("witness");
    let lock = write_lock(
        &dir,
        "w.lock.json",
        &[(s(&old), sha256_of(&old)), (s(&new), sha256_of(&new))],
    );
    let ledger = dir.join("witness.jsonl");
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .args([s(&old), s(&new), "--lock", s(&lock)])
        .env("EPISTEMIC_WITNESS", &ledger)
        .output()
        .expect("run rvl");
    let record: Value =
        serde_json::from_str(fs::read_to_string(&ledger).unwrap().lines().last().unwrap()).unwrap();
    assert_eq!(record["params"]["lock"][0], s(&lock));
    assert_eq!(
        record["params"]["lock_verified"][0]["lock_hash"],
        "sha256:feed"
    );
}
