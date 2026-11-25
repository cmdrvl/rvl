//! bd-3xw: in `--non-numeric` mode a blank<->numeric transition is a nullity flip, not an
//! `E_MISSINGNESS` refusal. The default numeric mode keeps its refusal contract. Raw values stay
//! redacted without `--explicit`.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

const KEY_ENV: &str = "RVL_REDACTION_KEY";
const KEY_CANARY: &str = "KEYCANARY-31";
const COL_CANARY: &str = "COLCANARY";
const BLANK_TO_NUM: &str = "7777123";
const NUM_TO_BLANK: &str = "5919191";

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "rvl-nullity-{name}-{}-{}",
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

fn run(old: &Path, new: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .env_remove(KEY_ENV)
        .arg(old)
        .arg(new)
        .args(args)
        .arg("--json")
        .arg("--no-witness")
        .output()
        .expect("run rvl")
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

fn text(output: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// A blank->numeric row, a numeric->blank row and an ordinary numeric change in one column.
fn flip_pair(scratch: &Scratch) -> (PathBuf, PathBuf) {
    let old = scratch.write(
        "old.csv",
        &format!("id,{COL_CANARY}\n{KEY_CANARY},\nB,{NUM_TO_BLANK}\nC,1\n"),
    );
    let new = scratch.write(
        "new.csv",
        &format!("id,{COL_CANARY}\n{KEY_CANARY},{BLANK_TO_NUM}\nB,\nC,2\n"),
    );
    (old, new)
}

#[test]
fn non_numeric_records_blank_to_numeric_and_numeric_to_blank_as_nullity_flips() {
    let scratch = Scratch::new("flips");
    let (old, new) = flip_pair(&scratch);
    let output = run(&old, &new, &["--key", "id", "--non-numeric"]);
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let out = json(&output);
    assert_eq!(out["outcome"], "REAL_CHANGE", "{out}");
    let flips = out["non_numeric"]["nullity_flips"]
        .as_array()
        .expect("nullity_flips");
    assert_eq!(flips.len(), 1, "{out}");
    assert_eq!(flips[0]["became_non_null"], 1);
    assert_eq!(flips[0]["became_null"], 1);
    // The ordinary numeric change in the same column is still a numeric contributor.
    assert_eq!(out["counts"]["numeric_cells_changed"], 1, "{out}");
}

#[test]
fn a_table_whose_only_change_is_a_blank_to_numeric_flip_is_a_real_change() {
    let scratch = Scratch::new("only");
    let old = scratch.write("old.csv", "id,amount\nA,\nB,3\n");
    let new = scratch.write("new.csv", "id,amount\nA,9\nB,3\n");
    let output = run(&old, &new, &["--key", "id", "--non-numeric"]);
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let out = json(&output);
    assert_eq!(out["outcome"], "REAL_CHANGE", "{out}");
    assert_eq!(out["non_numeric"]["nullity_flips"][0]["became_non_null"], 1);
}

#[test]
fn row_order_mode_also_records_the_flips() {
    let scratch = Scratch::new("roworder");
    let old = scratch.write("old.csv", "n,amount\n1,\n2,4\n");
    let new = scratch.write("new.csv", "n,amount\n1,9\n2,\n");
    let output = run(&old, &new, &["--non-numeric"]);
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let flips = &json(&output)["non_numeric"]["nullity_flips"];
    assert_eq!(flips[0]["became_non_null"], 1, "{flips}");
    assert_eq!(flips[0]["became_null"], 1, "{flips}");
}

#[test]
fn default_numeric_mode_keeps_the_e_missingness_refusal() {
    let scratch = Scratch::new("numeric");
    let (old, new) = flip_pair(&scratch);
    let output = run(&old, &new, &["--key", "id"]);
    assert_eq!(output.status.code(), Some(2), "{}", text(&output));
    let out = json(&output);
    assert_eq!(out["outcome"], "REFUSAL", "{out}");
    assert_eq!(out["refusal"]["code"], "E_MISSINGNESS", "{out}");
}

#[test]
fn flips_stay_redacted_by_default_and_show_values_with_explicit() {
    let scratch = Scratch::new("redaction");
    let (old, new) = flip_pair(&scratch);

    let default = run(&old, &new, &["--key", "id", "--non-numeric"]);
    assert_eq!(default.status.code(), Some(1));
    let all = text(&default);
    for canary in [KEY_CANARY, COL_CANARY, BLANK_TO_NUM, NUM_TO_BLANK] {
        assert!(!all.contains(canary), "{canary} leaked by default: {all}");
    }
    assert!(
        json(&default)["non_numeric"]["nullity_flips"][0]["column"] == "[REDACTED]",
        "{all}"
    );

    let explicit = run(&old, &new, &["--key", "id", "--non-numeric", "--explicit"]);
    assert_eq!(explicit.status.code(), Some(1));
    let all = text(&explicit);
    for shown in [KEY_CANARY, COL_CANARY, BLANK_TO_NUM, NUM_TO_BLANK] {
        assert!(all.contains(shown), "--explicit must show {shown}: {all}");
    }
    let out = json(&explicit);
    assert_eq!(
        out["non_numeric"]["nullity_flips"][0]["column"],
        format!("u8:{COL_CANARY}")
    );
}

#[test]
fn mixed_types_are_still_refused_in_non_numeric_mode() {
    let scratch = Scratch::new("mixed");
    // A flip makes the column numeric evidence; a later text-vs-text pair is mixed types.
    let old = scratch.write("old.csv", "id,amount\nA,\nB,abc\n");
    let new = scratch.write("new.csv", "id,amount\nA,9\nB,abc\n");
    let output = run(&old, &new, &["--key", "id", "--non-numeric"]);
    assert_eq!(output.status.code(), Some(2), "{}", text(&output));
    assert_eq!(json(&output)["refusal"]["code"], "E_MIXED_TYPES");
}
