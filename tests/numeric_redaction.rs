//! bd-u7n: default numeric output carries no raw contributor row_id, row_key or column name.
//! Without --explicit each contributor is an ordinal ref (one `k#` per key TUPLE, `c#` per column,
//! numbered in contribution-rank order) or, with RVL_REDACTION_KEY, a keyed HMAC marker; the
//! same convention as the --non-numeric output (bd-u1k, bd-ez4).

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

const KEY_ENV: &str = "RVL_REDACTION_KEY";
const KEY: &str = "0123456789abcdef";
const OTHER_KEY: &str = "fedcba9876543210";
const PLANTED_KEY: &str = "PLANTED-KEYCANARY-77d1";

// Canaries: composite-key VALUES and a numeric column NAME that must never appear by default.
const BORROWER_ANN: &str = "CANARY-BORROWER-ann";
const BORROWER_BOB: &str = "CANARY-BORROWER-bob";
const BORROWER_CY: &str = "CANARY-BORROWER-cy";
const COLUMN: &str = "CANARY_balance_col";

// Independent known answers, computed outside rvl with python's hmac:
//   hmac.new(key, frame([borrower, loan]), sha256).hexdigest()[:32] where frame() is, per
//   component, the 8-byte little-endian length then the bytes; the column marker is the HMAC of
//   the raw column bytes. Key 0123456789abcdef.
const BOB_TUPLE_MARKER: &str = "redacted:hmac-sha256:c62d4f515dabed5d36780c540111a799";
const CY_TUPLE_MARKER: &str = "redacted:hmac-sha256:4149211ff1899890674cc6fbd67e4d00";
const COLUMN_MARKER: &str = "redacted:hmac-sha256:7e5e139bf4f5b689a575f934b75545b4";
const OTHER_COLUMN_MARKER: &str = "redacted:hmac-sha256:b6bf6b0e4ff455514d7d61f60fc509b8";

struct Scratch(PathBuf);

static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "rvl-numeric-redaction-{name}-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
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

struct Fixture {
    _scratch: Scratch,
    old: PathBuf,
    new: PathBuf,
    profile: PathBuf,
}

/// Composite key (borrower, loan). Changes: bob +60 on the canary column (rank 1), cy +30 on the
/// canary column (rank 2) and +6 on `other` (rank 3); ann is unchanged.
fn fixture(old_rows: &str, new_rows: &str) -> Fixture {
    let scratch = Scratch::new("fx");
    let header = format!("borrower,loan,{COLUMN},other\n");
    let old = scratch.write("old.csv", &format!("{header}{old_rows}"));
    let new = scratch.write("new.csv", &format!("{header}{new_rows}"));
    let profile = scratch.write(
        "profile.yaml",
        &format!("include_columns: [borrower, loan, {COLUMN}, other]\nkey: [borrower, loan]\n"),
    );
    Fixture {
        _scratch: scratch,
        old,
        new,
        profile,
    }
}

fn standard() -> Fixture {
    fixture(
        &format!("{BORROWER_ANN},L1,100,1\n{BORROWER_BOB},L2,200,2\n{BORROWER_CY},L3,300,3\n"),
        &format!("{BORROWER_ANN},L1,100,1\n{BORROWER_BOB},L2,260,2\n{BORROWER_CY},L3,330,9\n"),
    )
}

fn run(f: &Fixture, key: Option<&str>, extra: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rvl"));
    command.env_remove(KEY_ENV);
    if let Some(key) = key {
        command.env(KEY_ENV, key);
    }
    command
        .arg(&f.old)
        .arg(&f.new)
        .arg("--profile")
        .arg(&f.profile)
        .arg("--no-witness")
        .args(extra)
        .output()
        .expect("run rvl")
}

fn printed(output: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("not JSON ({e}): {}", printed(output)))
}

fn contributors(output: &Output) -> Vec<Value> {
    json(output)["contributors"].as_array().unwrap().clone()
}

#[test]
fn default_json_and_human_carry_no_key_value_or_column_name() {
    let f = standard();
    for extra in [&["--json"][..], &[][..]] {
        let output = run(&f, None, extra);
        assert_eq!(
            output.status.code(),
            Some(1),
            "{extra:?}: {}",
            printed(&output)
        );
        let text = printed(&output);
        for canary in [BORROWER_ANN, BORROWER_BOB, BORROWER_CY, COLUMN, "L2", "L3"] {
            assert!(
                !text.contains(canary),
                "{extra:?}: {canary} printed: {text}"
            );
        }
    }
    // Shape: the JSON contributors are refs with no raw row_key and no values.
    let items = contributors(&run(&f, None, &["--json"]));
    let ids: Vec<(String, String)> = items
        .iter()
        .map(|c| {
            (
                c["row_id"].as_str().unwrap().into(),
                c["column"].as_str().unwrap().into(),
            )
        })
        .collect();
    assert_eq!(
        ids,
        [
            ("redacted:ref:k1".into(), "redacted:ref:c1".into()),
            ("redacted:ref:k2".into(), "redacted:ref:c1".into()),
            ("redacted:ref:k2".into(), "redacted:ref:c2".into()),
        ]
    );
    for item in &items {
        assert!(item.get("row_key").is_none() && item.get("old").is_none());
    }
    // No contributor carries the raw identifier encoding. (The alignment block still names the key
    // COLUMNS; that is outside this change and is listed on the bead.)
    assert!(!serde_json::to_string(&items).unwrap().contains("u8:"));
    // Human lines carry the same refs.
    let human = String::from_utf8_lossy(&run(&f, None, &[]).stdout).into_owned();
    assert!(
        human.contains("1. redacted:ref:k1.redacted:ref:c1  +62.5% of total change"),
        "{human}"
    );
    assert!(
        human.contains("3. redacted:ref:k2.redacted:ref:c2"),
        "{human}"
    );
}

#[test]
fn explicit_shows_the_raw_identity_and_values() {
    let f = standard();
    let json_out = run(&f, None, &["--json", "--explicit"]);
    let first = &contributors(&json_out)[0];
    assert_eq!(first["row_id"], format!("u8:{BORROWER_BOB} + u8:L2"));
    assert_eq!(
        first["row_key"],
        serde_json::json!([format!("u8:{BORROWER_BOB}"), "u8:L2".to_string()])
    );
    assert_eq!(first["column"], format!("u8:{COLUMN}"));
    assert_eq!(first["old"], 200.0);
    assert_eq!(first["new"], 260.0);
    assert_eq!(first["delta"], 60.0);
    let human = String::from_utf8_lossy(&run(&f, None, &["--explicit"]).stdout).into_owned();
    assert!(
        human.contains(BORROWER_BOB) && human.contains(COLUMN),
        "{human}"
    );
}

#[test]
fn ordinals_follow_contribution_rank_not_value_or_name_bytes() {
    // Whichever borrower has the biggest change is k1, however its name sorts and wherever its
    // row sits in the file.
    for (ann_new, zed_new, first_key_is_ann) in [(160, 110, true), (110, 160, false)] {
        let f = fixture(
            "AAA-first,L1,100,1\nZZZ-last,L2,100,1\n",
            &format!("AAA-first,L1,{ann_new},1\nZZZ-last,L2,{zed_new},1\n"),
        );
        let named = contributors(&run(&f, None, &["--json", "--explicit"]));
        let default = contributors(&run(&f, None, &["--json"]));
        assert_eq!(default[0]["row_id"], "redacted:ref:k1");
        assert_eq!(default[1]["row_id"], "redacted:ref:k2");
        let top_is_ann = named[0]["row_id"].as_str().unwrap().contains("AAA-first");
        assert_eq!(top_is_ann, first_key_is_ann, "{named:?}");
    }
    // The same input in a different row order gives the same output.
    let ordered = fixture(
        "A,L1,100,1\nB,L2,100,1\nC,L3,100,1\n",
        "A,L1,150,1\nB,L2,120,1\nC,L3,110,1\n",
    );
    let shuffled = fixture(
        "C,L3,100,1\nA,L1,100,1\nB,L2,100,1\n",
        "B,L2,120,1\nC,L3,110,1\nA,L1,150,1\n",
    );
    let strip = |output: &Output| {
        let mut value = json(output);
        let object = value.as_object_mut().unwrap();
        object.remove("files");
        value
    };
    assert_eq!(
        strip(&run(&ordered, None, &["--json"])),
        strip(&run(&shuffled, None, &["--json"]))
    );
}

#[test]
fn output_is_byte_identical_across_runs() {
    let f = standard();
    for extra in [&["--json"][..], &[][..]] {
        for key in [None, Some(KEY)] {
            assert_eq!(
                run(&f, key, extra).stdout,
                run(&f, key, extra).stdout,
                "{extra:?} key={}",
                key.is_some()
            );
        }
    }
}

#[test]
fn keyed_markers_equal_independent_hmac_known_answers() {
    let f = standard();
    let output = run(&f, Some(KEY), &["--json"]);
    assert_eq!(output.status.code(), Some(1), "{}", printed(&output));
    let items = contributors(&output);
    // One marker per key TUPLE over the length-framed components; one per column over its bytes.
    assert_eq!(items[0]["row_id"], BOB_TUPLE_MARKER);
    assert_eq!(items[0]["column"], COLUMN_MARKER);
    assert_eq!(items[1]["row_id"], CY_TUPLE_MARKER);
    assert_eq!(items[2]["column"], OTHER_COLUMN_MARKER);
    let text = printed(&output);
    for secret in [BORROWER_BOB, COLUMN, KEY] {
        assert!(!text.contains(secret), "{secret} printed: {text}");
    }
    // Human output carries the same markers.
    let human = printed(&run(&f, Some(KEY), &[]));
    assert!(
        human.contains(&format!("{BOB_TUPLE_MARKER}.{COLUMN_MARKER}")),
        "{human}"
    );
    // A different key changes every marker.
    let other = contributors(&run(&f, Some(OTHER_KEY), &["--json"]));
    for (a, b) in items.iter().zip(&other) {
        assert_ne!(a["row_id"], b["row_id"]);
        assert_ne!(a["column"], b["column"]);
    }
}

#[test]
fn an_unusable_key_refuses_default_numeric_runs_and_never_echoes_the_key() {
    let f = standard();
    for (bad, reason) in [("", "empty"), (PLANTED_KEY.get(..8).unwrap(), "too_short")] {
        for extra in [&["--json"][..], &[][..]] {
            let output = run(&f, Some(bad), extra);
            assert_eq!(
                output.status.code(),
                Some(2),
                "{bad:?} {extra:?}: {}",
                printed(&output)
            );
            let text = printed(&output);
            assert!(text.contains("E_REDACTION_KEY_INVALID"), "{text}");
            assert!(bad.is_empty() || !text.contains(bad), "key echoed: {text}");
            if extra == ["--json"] {
                assert_eq!(json(&output)["refusal"]["detail"]["reason"], reason);
            }
        }
    }
    // With --explicit no marker is printed, so an unusable key is ignored as before.
    let explicit = run(&f, Some("short"), &["--json", "--explicit"]);
    assert_eq!(explicit.status.code(), Some(1), "{}", printed(&explicit));
}

#[test]
fn the_planted_key_never_appears_in_output_or_the_witness_ledger() {
    let f = standard();
    let ledger = f.old.with_file_name("witness.jsonl");
    for extra in [&["--json"][..], &[][..]] {
        let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
            .env(KEY_ENV, PLANTED_KEY)
            .env("EPISTEMIC_WITNESS", &ledger)
            .arg(&f.old)
            .arg(&f.new)
            .arg("--profile")
            .arg(&f.profile)
            .args(extra)
            .output()
            .expect("run rvl");
        assert_eq!(output.status.code(), Some(1), "{}", printed(&output));
        assert!(!printed(&output).contains(PLANTED_KEY));
    }
    let recorded = std::fs::read_to_string(Path::new(&ledger)).unwrap_or_default();
    assert!(!recorded.is_empty() && !recorded.contains(PLANTED_KEY));
    for secret in [BORROWER_BOB, COLUMN] {
        assert!(!recorded.contains(secret), "{secret} in the witness ledger");
    }
}
