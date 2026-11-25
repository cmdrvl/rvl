//! bd-16z: `--key-mode overlap` end-to-end tests against the real binary.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

struct Td(PathBuf);

impl Td {
    fn new() -> Self {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("rvl_key_overlap_{}_{n}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        Td(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, body).unwrap();
    path
}

fn rvl(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .args(args)
        .arg("--no-witness")
        .output()
        .expect("run rvl")
}

fn json(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("bad json {e}: {}", String::from_utf8_lossy(&out.stdout)))
}

/// old: 1..=5; new: 3..=7 (removed 1,2; added 6,7; overlap 3,4,5; 5 changes).
fn planted(dir: &Path) -> (PathBuf, PathBuf) {
    let old = write(
        dir,
        "old.csv",
        "id,bal\n1,100\n2,200\n3,300\n4,400\n5,500\n",
    );
    let new = write(
        dir,
        "new.csv",
        "id,bal\n3,300\n4,400\n5,999\n6,600\n7,700\n",
    );
    (old, new)
}

#[test]
fn exact_mode_still_refuses_with_runnable_overlap_next_command() {
    let dir = Td::new();
    let (old, new) = planted(dir.path());
    let out = rvl(&[
        old.to_str().unwrap(),
        new.to_str().unwrap(),
        "--key",
        "id",
        "--json",
    ]);
    let v = json(&out);
    assert_eq!(v["outcome"], "REFUSAL");
    assert_eq!(v["refusal"]["code"], "E_KEY_MISMATCH");
    assert_eq!(out.status.code(), Some(2));
    assert!(v["alignment"].get("key_membership").is_none());
    let next = v["refusal"]["next_command"].as_str().expect("next_command");
    assert!(next.contains("--key-mode overlap"), "{next}");
    assert!(next.contains("--key id"), "{next}");
    assert!(next.contains(old.to_str().unwrap()), "{next}");
}

#[test]
fn overlap_mode_reports_added_removed_and_scopes_verdict() {
    let dir = Td::new();
    let (old, new) = planted(dir.path());
    let out = rvl(&[
        old.to_str().unwrap(),
        new.to_str().unwrap(),
        "--key",
        "id",
        "--key-mode",
        "overlap",
        "--explicit",
        "--json",
    ]);
    let v = json(&out);
    assert_eq!(v["outcome"], "REAL_CHANGE", "{v}");
    let m = &v["alignment"]["key_membership"];
    assert_eq!(m["verdict_scope"], "continuing_rows");
    assert_eq!(m["overlap_count"], 3);
    assert_eq!(m["old_only_count"], 2);
    assert_eq!(m["new_only_count"], 2);
    assert_eq!(
        m["old_only_samples"],
        serde_json::json!([["u8:1"], ["u8:2"]])
    );
    assert_eq!(
        m["new_only_samples"],
        serde_json::json!([["u8:6"], ["u8:7"]])
    );
    // Only the continuing row 5 changed.
    assert_eq!(v["counts"]["rows_aligned"], 3);
}

#[test]
fn overlap_samples_are_redacted_without_explicit() {
    let dir = Td::new();
    let (old, new) = planted(dir.path());
    let out = rvl(&[
        old.to_str().unwrap(),
        new.to_str().unwrap(),
        "--key",
        "id",
        "--key-mode",
        "overlap",
        "--json",
    ]);
    let m = &json(&out)["alignment"]["key_membership"];
    assert_eq!(m["old_only_count"], 2);
    assert!(m.get("old_only_samples").is_none());
    assert!(m.get("new_only_samples").is_none());
}

#[test]
fn removals_only_and_additions_only() {
    let dir = Td::new();
    let old = write(dir.path(), "o.csv", "id,bal\n1,1\n2,2\n3,3\n");
    let removed = write(dir.path(), "r.csv", "id,bal\n1,1\n2,2\n");
    let added = write(dir.path(), "a.csv", "id,bal\n1,1\n2,2\n3,3\n4,4\n");
    for (new, o, n) in [(&removed, 1, 0), (&added, 0, 1)] {
        let out = rvl(&[
            old.to_str().unwrap(),
            new.to_str().unwrap(),
            "--key",
            "id",
            "--key-mode",
            "overlap",
            "--json",
        ]);
        let v = json(&out);
        // Continuing rows unchanged => NO_REAL_CHANGE despite membership drift.
        assert_eq!(v["outcome"], "NO_REAL_CHANGE", "{v}");
        let m = &v["alignment"]["key_membership"];
        assert_eq!(m["old_only_count"], o);
        assert_eq!(m["new_only_count"], n);
    }
}

#[test]
fn zero_overlap_refuses_no_overlap() {
    let dir = Td::new();
    let old = write(dir.path(), "o.csv", "id,bal\n1,1\n");
    let new = write(dir.path(), "n.csv", "id,bal\n2,2\n");
    let out = rvl(&[
        old.to_str().unwrap(),
        new.to_str().unwrap(),
        "--key",
        "id",
        "--key-mode",
        "overlap",
        "--json",
    ]);
    let v = json(&out);
    assert_eq!(v["outcome"], "REFUSAL");
    assert_eq!(v["refusal"]["code"], "E_NO_OVERLAP");
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn composite_key_via_profile() {
    let dir = Td::new();
    let profile = write(
        dir.path(),
        "p.yaml",
        "include_columns: [unit, bldg, amt]\nkey: [unit, bldg]\n",
    );
    let old = write(
        dir.path(),
        "o.csv",
        "unit,bldg,amt\n1,A,10\n1,B,20\n2,A,30\n",
    );
    let new = write(
        dir.path(),
        "n.csv",
        "unit,bldg,amt\n1,A,10\n2,A,31\n3,C,5\n",
    );
    let out = rvl(&[
        old.to_str().unwrap(),
        new.to_str().unwrap(),
        "--profile",
        profile.to_str().unwrap(),
        "--key-mode",
        "overlap",
        "--explicit",
        "--json",
    ]);
    let v = json(&out);
    assert_eq!(v["outcome"], "REAL_CHANGE", "{v}");
    let m = &v["alignment"]["key_membership"];
    assert_eq!(m["overlap_count"], 2);
    assert_eq!(m["old_only_samples"], serde_json::json!([["u8:1", "u8:B"]]));
    assert_eq!(m["new_only_samples"], serde_json::json!([["u8:3", "u8:C"]]));

    // Exact mode with the same profile: refusal whose next_command keeps the profile.
    let out = rvl(&[
        old.to_str().unwrap(),
        new.to_str().unwrap(),
        "--profile",
        profile.to_str().unwrap(),
        "--json",
    ]);
    let v = json(&out);
    assert_eq!(v["refusal"]["code"], "E_KEY_MISMATCH");
    let next = v["refusal"]["next_command"].as_str().unwrap();
    assert!(
        next.contains("--key-mode overlap") && next.contains("--profile"),
        "{next}"
    );
}

#[test]
fn overlap_without_key_refuses_need_key() {
    let dir = Td::new();
    let (old, new) = planted(dir.path());
    let out = rvl(&[
        old.to_str().unwrap(),
        new.to_str().unwrap(),
        "--key-mode",
        "overlap",
        "--json",
    ]);
    assert_eq!(json(&out)["refusal"]["code"], "E_NEED_KEY");
}

/// shape && rvl pipeline on the 13-removed / 0-added scenario.
#[test]
fn shape_then_rvl_pipeline_with_removed_rows() {
    if Command::new("shape").arg("--version").output().is_err() {
        eprintln!("SKIPPED: `shape` not on PATH; pipeline half not exercised");
        return;
    }
    let dir = Td::new();
    let mut old = String::from("loan_id,bal\n");
    let mut new = String::from("loan_id,bal\n");
    for i in 0..40 {
        old.push_str(&format!("L{i:03},{}\n", 1000 + i));
        if i >= 13 {
            new.push_str(&format!("L{i:03},{}\n", 1000 + i));
        }
    }
    let old = write(dir.path(), "nov.csv", &old);
    let new = write(dir.path(), "dec.csv", &new);
    let shape = Command::new("shape")
        .args([
            old.to_str().unwrap(),
            new.to_str().unwrap(),
            "--key",
            "loan_id",
        ])
        .output()
        .unwrap();
    assert_eq!(shape.status.code(), Some(0), "shape must say COMPATIBLE");

    let exact = rvl(&[
        old.to_str().unwrap(),
        new.to_str().unwrap(),
        "--key",
        "loan_id",
        "--json",
    ]);
    let v = json(&exact);
    assert_eq!(v["refusal"]["code"], "E_KEY_MISMATCH");
    let next = v["refusal"]["next_command"].as_str().unwrap().to_string();
    assert!(next.contains("--key-mode overlap"));

    let ok = rvl(&[
        old.to_str().unwrap(),
        new.to_str().unwrap(),
        "--key",
        "loan_id",
        "--key-mode",
        "overlap",
        "--json",
    ]);
    let v = json(&ok);
    assert_eq!(v["outcome"], "NO_REAL_CHANGE");
    assert_eq!(v["alignment"]["key_membership"]["old_only_count"], 13);
    assert_eq!(v["alignment"]["key_membership"]["new_only_count"], 0);
    assert_eq!(v["alignment"]["key_membership"]["overlap_count"], 27);
}
