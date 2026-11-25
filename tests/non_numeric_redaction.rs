//! bd-u1k rework: the default `--non-numeric` output carries no value-derived hash. Values and
//! rows become deterministic ordinal refs, with an opt-in keyed HMAC (RVL_REDACTION_KEY).

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

const KEY_ENV: &str = "RVL_REDACTION_KEY";
const TEST_KEY: &str = "0123456789abcdef";
const PLANTED_KEY: &str = "PLANTED-KEYCANARY-5e2c";

// Planted canaries: a row key and two categorical values that must never appear in default output.
const OLD_VALUE: &str = "open";
const NEW_VALUE: &str = "closed";
const ROW_KEY: &str = "ROWKEY-CANARY-9a8b";

// Independent known answers (openssl dgst -sha256 -hmac <key>, first 32 hex), see tests below.
//   printf '%s' open   | openssl dgst -sha256 -hmac 0123456789abcdef
//   printf '%s' closed | openssl dgst -sha256 -hmac 0123456789abcdef
const OPEN_MARKER: &str = "redacted:hmac-sha256:b0dd9e34e5ee47920ba0efa2d9a3e5be";
const CLOSED_MARKER: &str = "redacted:hmac-sha256:1fa915cc3c47c712fd41242d017fb1cf";

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "rvl-redaction-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        Self(dir)
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, contents).expect("write file");
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn run(old: &Path, new: &Path, key: Option<&str>, extra: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rvl"));
    command.env_remove(KEY_ENV);
    if let Some(key) = key {
        command.env(KEY_ENV, key);
    }
    command
        .arg(old)
        .arg(new)
        .args(["--key", "id", "--non-numeric", "--no-witness"])
        .args(extra)
        .output()
        .expect("run rvl")
}

fn text(output: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        Value::String(format!(
            "unparsed stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    })
}

fn blake3_hex(value: &str) -> String {
    blake3::hash(value.as_bytes()).to_hex().to_string()
}

/// The old default tokens for a value: the first 16 hex and the full digest, with and without prefix.
fn old_tokens(value: &str) -> Vec<String> {
    let hex = blake3_hex(value);
    vec![
        format!("blake3:{}", &hex[..16]),
        format!("blake3:{hex}"),
        hex[..16].to_owned(),
        hex,
    ]
}

fn fixtures(scratch: &Scratch) -> (PathBuf, PathBuf) {
    let old = scratch.write(
        "old.csv",
        &format!("id,status,as_of\n{ROW_KEY},{OLD_VALUE},2024-01-01\nB,{OLD_VALUE},2024-01-02\nC,closed,2024-01-03\n"),
    );
    let new = scratch.write(
        "new.csv",
        &format!("id,status,as_of\n{ROW_KEY},{NEW_VALUE},2024-01-03\nB,{NEW_VALUE},2024-01-04\nC,{OLD_VALUE},2024-01-05\n"),
    );
    (old, new)
}

fn transitions(value: &Value) -> Vec<(String, String, Vec<String>)> {
    value["non_numeric"]["categorical_transitions"]
        .as_array()
        .expect("transitions")
        .iter()
        .map(|t| {
            (
                t["old"].as_str().expect("old").to_owned(),
                t["new"].as_str().expect("new").to_owned(),
                t["affected_rows"]
                    .as_array()
                    .expect("rows")
                    .iter()
                    .map(|row| row.as_str().expect("row").to_owned())
                    .collect(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Default: ordinal refs
// ---------------------------------------------------------------------------

#[test]
fn default_output_uses_ordinal_refs_with_no_value_derived_hash() {
    let scratch = Scratch::new("ordinal");
    let (old, new) = fixtures(&scratch);
    let output = run(&old, &new, None, &["--json"]);
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let value = json(&output);
    let all = transitions(&value);
    assert!(!all.is_empty(), "{value}");

    // Every value is a v-ref and every row a k-ref; nothing hash-shaped.
    for (old, new, rows) in &all {
        assert!(
            old.starts_with("redacted:ref:v") && new.starts_with("redacted:ref:v"),
            "{all:?}"
        );
        assert!(
            rows.iter().all(|row| row.starts_with("redacted:ref:k")),
            "{all:?}"
        );
    }
    let out = text(&output);
    for canary in [OLD_VALUE, NEW_VALUE, ROW_KEY] {
        // Substring check on the bare canary would also match words like "closed" in prose, so
        // search for the exact JSON string forms and for the old hash tokens.
        assert!(
            !out.contains(&format!("\"{canary}\"")),
            "{canary} leaked: {out}"
        );
        for token in old_tokens(canary) {
            assert!(
                !out.contains(&token),
                "old token {token} for {canary} still present: {out}"
            );
        }
    }
    assert!(!out.contains("blake3"), "{out}");
    assert!(!out.contains("hmac"), "{out}");
    assert!(!out.contains(ROW_KEY), "{out}");
}

#[test]
fn equal_values_share_a_ref_distinct_values_do_not_and_numbering_follows_data_order() {
    let scratch = Scratch::new("equality");
    let (old, new) = fixtures(&scratch);
    let value = json(&run(&old, &new, None, &["--json"]));
    let all = transitions(&value);
    // Transitions: open->closed (2 rows: ROW_KEY and B) and closed->open (1 row: C).
    let by_rows: std::collections::BTreeMap<usize, &(String, String, Vec<String>)> =
        all.iter().map(|t| (t.2.len(), t)).collect();
    let open_to_closed = by_rows[&2];
    let closed_to_open = by_rows[&1];
    // open and closed are the only two values: v1 and v2 by first appearance.
    assert_eq!(open_to_closed.0, "redacted:ref:v1");
    assert_eq!(open_to_closed.1, "redacted:ref:v2");
    // The reverse transition reuses the same refs, swapped.
    assert_eq!(closed_to_open.0, open_to_closed.1);
    assert_eq!(closed_to_open.1, open_to_closed.0);
    // Three distinct rows get three distinct k-refs; the key namespace is separate from values.
    let mut rows: Vec<&String> = all.iter().flat_map(|t| t.2.iter()).collect();
    rows.sort();
    rows.dedup();
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert!(rows.iter().all(|row| row.starts_with("redacted:ref:k")));
}

#[test]
fn default_output_is_byte_identical_across_runs_and_in_human_mode() {
    let scratch = Scratch::new("determinism");
    let (old, new) = fixtures(&scratch);
    let first = run(&old, &new, None, &["--json"]);
    let second = run(&old, &new, None, &["--json"]);
    assert_eq!(first.stdout, second.stdout);

    let human = run(&old, &new, None, &[]);
    let human_text = text(&human);
    assert!(human_text.contains("[REDACTED]"), "{human_text}");
    assert!(!human_text.contains(ROW_KEY), "{human_text}");
    for token in old_tokens(OLD_VALUE)
        .into_iter()
        .chain(old_tokens(NEW_VALUE))
    {
        assert!(!human_text.contains(&token), "{human_text}");
    }
}

#[test]
fn ref_numbering_does_not_follow_value_byte_order() {
    // Same two transitions, rows swapped between the files. zebra->zebra2 touches two rows and
    // apple->apple2 one. If entries were ordered by value bytes, apple (one row) would come
    // first in both runs and the ref numbers would reveal that apple sorts before zebra. They
    // follow the data instead: the transition seen first in the file is the first entry.
    let scratch = Scratch::new("order");
    let zebra_first_old = scratch.write("old1.csv", "id,status\nA,zebra\nB,zebra\nC,apple\n");
    let zebra_first_new = scratch.write("new1.csv", "id,status\nA,zebra2\nB,zebra2\nC,apple2\n");
    let apple_first_old = scratch.write("old2.csv", "id,status\nA,apple\nB,zebra\nC,zebra\n");
    let apple_first_new = scratch.write("new2.csv", "id,status\nA,apple2\nB,zebra2\nC,zebra2\n");

    let first = transitions(&json(&run(
        &zebra_first_old,
        &zebra_first_new,
        None,
        &["--json"],
    )));
    let second = transitions(&json(&run(
        &apple_first_old,
        &apple_first_new,
        None,
        &["--json"],
    )));
    assert_eq!(first.len(), 2, "{first:?}");
    assert_eq!(second.len(), 2, "{second:?}");
    assert_eq!(
        first[0].2.len(),
        2,
        "zebra (two rows) is first in the first file: {first:?}"
    );
    assert_eq!(
        second[0].2.len(),
        1,
        "apple (one row) is first in the second file: {second:?}"
    );
    // The first entry always owns v1 and v2, whichever value it is.
    for entries in [&first, &second] {
        assert_eq!(entries[0].0, "redacted:ref:v1");
        assert_eq!(entries[0].1, "redacted:ref:v2");
        assert_eq!(entries[1].0, "redacted:ref:v3");
    }
}

// ---------------------------------------------------------------------------
// Keyed markers
// ---------------------------------------------------------------------------

#[test]
fn keyed_markers_match_the_independent_openssl_known_answers() {
    let scratch = Scratch::new("keyed");
    let (old, new) = fixtures(&scratch);
    let output = run(&old, &new, Some(TEST_KEY), &["--json"]);
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let all = transitions(&json(&output));
    let open_to_closed = all
        .iter()
        .find(|t| t.2.len() == 2)
        .expect("open->closed transition");
    assert_eq!(open_to_closed.0, OPEN_MARKER);
    assert_eq!(open_to_closed.1, CLOSED_MARKER);
    assert!(
        all.iter()
            .flat_map(|t| t.2.iter())
            .all(|row| row.starts_with("redacted:hmac-sha256:") && row.len() == 21 + 32)
    );
    let out = text(&output);
    assert!(!out.contains("redacted:ref:"), "{out}");
    assert!(!out.contains(ROW_KEY), "{out}");

    // Stable across runs and inputs: a different file with the same values yields the same markers.
    let other_old = scratch.write("o2.csv", "id,status\nZZ,open\n");
    let other_new = scratch.write("n2.csv", "id,status\nZZ,closed\n");
    let other = transitions(&json(&run(
        &other_old,
        &other_new,
        Some(TEST_KEY),
        &["--json"],
    )));
    assert_eq!(
        (other[0].0.as_str(), other[0].1.as_str()),
        (OPEN_MARKER, CLOSED_MARKER)
    );

    // A different key gives different markers.
    let different = transitions(&json(&run(
        &old,
        &new,
        Some("0123456789abcdeg"),
        &["--json"],
    )));
    assert_ne!(
        different.iter().find(|t| t.2.len() == 2).expect("t").0,
        OPEN_MARKER
    );
}

#[test]
fn old_blake3_tokens_match_nothing_in_default_or_keyed_output() {
    let scratch = Scratch::new("dictionary");
    let (old, new) = fixtures(&scratch);
    for (label, key) in [("default", None), ("keyed", Some(TEST_KEY))] {
        for extra in [&["--json"][..], &[][..]] {
            let out = text(&run(&old, &new, key, extra));
            for canary in [OLD_VALUE, NEW_VALUE, ROW_KEY, "A", "B", "C"] {
                for token in old_tokens(canary) {
                    assert!(
                        !out.contains(&token),
                        "{label} {extra:?}: old token for {canary}: {out}"
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Key hygiene and refusals
// ---------------------------------------------------------------------------

#[test]
fn empty_and_short_keys_are_refused_with_the_typed_code_and_16_bytes_work() {
    let scratch = Scratch::new("refusal");
    let (old, new) = fixtures(&scratch);
    for (label, key) in [("empty", ""), ("15 bytes", "ABCDEFGHIJKLMNO")] {
        for extra in [&["--json"][..], &[][..], &["--json", "--explicit"][..]] {
            let output = run(&old, &new, Some(key), extra);
            assert_eq!(
                output.status.code(),
                Some(2),
                "{label} {extra:?}: {}",
                text(&output)
            );
            let out = text(&output);
            assert!(out.contains("E_REDACTION_KEY_INVALID"), "{label}: {out}");
            assert!(
                !out.contains("redacted:ref:"),
                "{label} fell back to ordinals: {out}"
            );
            assert!(!out.contains(ROW_KEY));
            if !key.is_empty() {
                assert!(!out.contains(key), "{label} echoed the key: {out}");
            }
        }
        let refusal = json(&run(&old, &new, Some(key), &["--json"]));
        assert_eq!(
            refusal["refusal"]["code"], "E_REDACTION_KEY_INVALID",
            "{label}"
        );
        assert_eq!(refusal["refusal"]["detail"]["min_bytes"], 16);
    }
    let ok = run(&old, &new, Some("0123456789abcdef"), &["--json"]);
    assert_eq!(ok.status.code(), Some(1));
    assert!(text(&ok).contains("redacted:hmac-sha256:"));
}

#[test]
fn explicit_still_shows_raw_values_even_with_a_key_set() {
    let scratch = Scratch::new("explicit");
    let (old, new) = fixtures(&scratch);
    let output = run(&old, &new, Some(TEST_KEY), &["--json", "--explicit"]);
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let all = transitions(&json(&output));
    assert!(
        all.iter().any(|t| t.0 == "u8:open" && t.1 == "u8:closed"),
        "{all:?}"
    );
    assert!(
        all.iter()
            .flat_map(|t| t.2.iter())
            .any(|row| row.contains(ROW_KEY)),
        "{all:?}"
    );
    assert!(!text(&output).contains("redacted:"), "{}", text(&output));
}

#[test]
fn the_key_never_appears_in_any_channel_including_the_witness_ledger_and_doctor() {
    let scratch = Scratch::new("hygiene");
    let (old, new) = fixtures(&scratch);
    let ledger = scratch.0.join("witness.jsonl");

    let mut seen: Vec<(&str, String)> = Vec::new();
    // A keyed run that DOES write a witness record, JSON and human.
    for extra in [&["--json"][..], &[][..]] {
        let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
            .env(KEY_ENV, PLANTED_KEY)
            .env("EPISTEMIC_WITNESS", &ledger)
            .arg(&old)
            .arg(&new)
            .args(["--key", "id", "--non-numeric"])
            .args(extra)
            .output()
            .expect("run rvl");
        assert_eq!(output.status.code(), Some(1), "{}", text(&output));
        seen.push(("run", text(&output)));
    }
    // A refusal that does not involve the key (missing key column) and a bad-key refusal.
    for key in [PLANTED_KEY, "short-PLANTED"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
            .env(KEY_ENV, key)
            .env("EPISTEMIC_WITNESS", &ledger)
            .arg(&old)
            .arg(&new)
            .args(["--key", "no_such_column", "--non-numeric"])
            .output()
            .expect("run rvl");
        seen.push(("refusal", text(&output)));
    }
    for args in [
        &["doctor", "health", "--json"][..],
        &["doctor", "capabilities", "--json"][..],
        &["doctor", "robot-docs"][..],
        &["--describe"][..],
        &["--schema"][..],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
            .env(KEY_ENV, PLANTED_KEY)
            .env("EPISTEMIC_WITNESS", &ledger)
            .args(args)
            .output()
            .expect("run rvl");
        seen.push(("introspection", text(&output)));
    }
    for (label, out) in &seen {
        assert!(!out.contains(PLANTED_KEY), "{label} leaked the key: {out}");
        assert!(
            !out.contains("short-PLANTED"),
            "{label} leaked the key: {out}"
        );
    }
    let ledger_text = std::fs::read_to_string(&ledger).expect("witness ledger written");
    assert!(!ledger_text.is_empty());
    assert!(!ledger_text.contains(PLANTED_KEY) && !ledger_text.contains("short-PLANTED"));
    assert!(!ledger_text.contains(ROW_KEY));
}

#[test]
fn numeric_mode_is_untouched_by_the_key_variable() {
    // The key only matters for --non-numeric output; a numeric run with a junk key still works.
    let scratch = Scratch::new("numeric");
    let old = scratch.write("old.csv", "id,v\nA,1\nB,2\n");
    let new = scratch.write("new.csv", "id,v\nA,1\nB,3\n");
    let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
        .env(KEY_ENV, "short")
        .arg(&old)
        .arg(&new)
        .args(["--key", "id", "--json", "--no-witness"])
        .output()
        .expect("run rvl");
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
}
