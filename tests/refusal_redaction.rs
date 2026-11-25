//! bd-ez4: refusals never print cell values, key values or data-derived column names unless
//! `--explicit` is set. Default output uses ordinal refs (`redacted:ref:{v,k,c}#`), or keyed
//! HMAC markers when `RVL_REDACTION_KEY` is set. Canary strings are planted in every data-bearing
//! refusal; JSON and human output are both checked. Also: every `--json` stdout path ends in `\n`.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

const KEY_ENV: &str = "RVL_REDACTION_KEY";
const TEST_KEY: &str = "0123456789abcdef";

const KEY_CANARY: &str = "KEYCANARY-77";
const KEY_CANARY_2: &str = "KEYCANARY-88";
const COL_CANARY: &str = "COLCANARY";
const CELL_NUMBER: &str = "7777123";
const CELL_TEXT: &str = "CELLCANARY-abc";

// Independent known answers, computed with openssl (not with this crate):
//   printf '%s' 7777123 | openssl dgst -sha256 -hmac 0123456789abcdef   -> 6f34db1038ebf91f...
//   printf '%s' COLCANARY | openssl dgst -sha256 -hmac 0123456789abcdef -> e132ce51215d63c4...
//   printf '\x0c\x00\x00\x00\x00\x00\x00\x00KEYCANARY-77' | openssl dgst -sha256 -hmac 0123456789abcdef
//                                                                       -> 779b738c46bd51be...
// (the key tuple is hashed length-framed: 8-byte little-endian length, then the bytes.)
// Markers are the first 32 hex digits (128 bits).
const VALUE_MARKER: &str = "redacted:hmac-sha256:6f34db1038ebf91f70eda4e26e376864";
const COLUMN_MARKER: &str = "redacted:hmac-sha256:e132ce51215d63c4642d82f8f78b6046";
const KEY_MARKER: &str = "redacted:hmac-sha256:779b738c46bd51be4af111c5690bec8a";

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "rvl-refusal-redaction-{name}-{}-{}",
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

fn run(old: &Path, new: &Path, key: Option<&str>, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rvl"));
    command.env_remove(KEY_ENV);
    if let Some(key) = key {
        command.env(KEY_ENV, key);
    }
    command
        .arg(old)
        .arg(new)
        .args(args)
        .arg("--no-witness")
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

struct Case {
    label: &'static str,
    code: &'static str,
    old: String,
    new: String,
    args: Vec<&'static str>,
    /// Strings that must be absent by default and present with `--explicit`.
    canaries: Vec<&'static str>,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            label: "E_MISSINGNESS",
            code: "E_MISSINGNESS",
            old: format!("id,{COL_CANARY}\n{KEY_CANARY},\nB,1\n"),
            new: format!("id,{COL_CANARY}\n{KEY_CANARY},{CELL_NUMBER}\nB,1\n"),
            args: vec!["--key", "id"],
            canaries: vec![KEY_CANARY, COL_CANARY, CELL_NUMBER],
        },
        Case {
            label: "E_MIXED_TYPES",
            code: "E_MIXED_TYPES",
            old: format!("id,{COL_CANARY}\n{KEY_CANARY},{CELL_TEXT}\nB,1\n"),
            new: format!("id,{COL_CANARY}\n{KEY_CANARY},5\nB,1\n"),
            args: vec!["--key", "id"],
            canaries: vec![KEY_CANARY, COL_CANARY, CELL_TEXT],
        },
        Case {
            label: "E_KEY_DUP",
            code: "E_KEY_DUP",
            old: format!("id,v\n{KEY_CANARY},1\n{KEY_CANARY},2\n"),
            new: format!("id,v\n{KEY_CANARY},1\n"),
            args: vec!["--key", "id"],
            canaries: vec![KEY_CANARY],
        },
        Case {
            label: "E_KEY_MISMATCH",
            code: "E_KEY_MISMATCH",
            old: format!("id,v\n{KEY_CANARY},1\nB,1\n"),
            new: format!("id,v\nB,1\n{KEY_CANARY_2},2\n"),
            args: vec!["--key", "id"],
            canaries: vec![KEY_CANARY, KEY_CANARY_2],
        },
        Case {
            label: "E_HEADERS duplicate",
            code: "E_HEADERS",
            old: format!("id,{COL_CANARY},{COL_CANARY}\nA,1,2\n"),
            new: format!("id,{COL_CANARY}\nA,1\n"),
            args: vec!["--key", "id"],
            canaries: vec![COL_CANARY],
        },
        Case {
            label: "E_ROWCOUNT suggested keys",
            code: "E_ROWCOUNT",
            old: format!("zeta_{COL_CANARY},alpha_{COL_CANARY}\n1,x\n2,y\n"),
            new: format!("zeta_{COL_CANARY},alpha_{COL_CANARY}\n1,x\n"),
            args: vec![],
            canaries: vec![COL_CANARY],
        },
    ]
}

#[test]
fn default_refusals_contain_no_canaries_in_json_or_human() {
    let scratch = Scratch::new("default");
    for case in cases() {
        let old = scratch.write("old.csv", &case.old);
        let new = scratch.write("new.csv", &case.new);

        let as_json = run(&old, &new, None, &[&case.args[..], &["--json"]].concat());
        assert_eq!(as_json.status.code(), Some(2), "{} exit", case.label);
        assert_eq!(
            json(&as_json)["refusal"]["code"],
            case.code,
            "{} code: {}",
            case.label,
            text(&as_json)
        );
        let as_human = run(&old, &new, None, &case.args);
        assert_eq!(as_human.status.code(), Some(2), "{} human exit", case.label);

        for (mode, output) in [("json", &as_json), ("human", &as_human)] {
            let all = text(output);
            for canary in &case.canaries {
                assert!(
                    !all.contains(canary),
                    "{} ({mode}) leaked {canary}: {all}",
                    case.label
                );
            }
        }
    }
}

#[test]
fn explicit_refusals_keep_the_exact_failing_values_in_json_and_human() {
    let scratch = Scratch::new("explicit");
    for case in cases() {
        let old = scratch.write("old.csv", &case.old);
        let new = scratch.write("new.csv", &case.new);

        let as_json = run(
            &old,
            &new,
            None,
            &[&case.args[..], &["--explicit", "--json"]].concat(),
        );
        let as_human = run(
            &old,
            &new,
            None,
            &[&case.args[..], &["--explicit"]].concat(),
        );
        assert_eq!(
            json(&as_json)["refusal"]["code"],
            case.code,
            "{}",
            case.label
        );

        for (mode, output) in [("json", &as_json), ("human", &as_human)] {
            let all = text(output);
            for canary in &case.canaries {
                assert!(
                    all.contains(canary),
                    "{} ({mode}) --explicit lost {canary}: {all}",
                    case.label
                );
            }
            assert!(
                !all.contains("redacted:"),
                "{} ({mode}) --explicit must not redact: {all}",
                case.label
            );
        }
    }
}

#[test]
fn default_markers_are_ordinal_refs_per_namespace() {
    let scratch = Scratch::new("ordinals");
    let missingness = &cases()[0];
    let old = scratch.write("old.csv", &missingness.old);
    let new = scratch.write("new.csv", &missingness.new);
    let out = json(&run(&old, &new, None, &["--key", "id", "--json"]));
    let detail = &out["refusal"]["detail"];
    assert_eq!(detail["column"], "redacted:ref:c1", "{out}");
    assert_eq!(detail["value"], "redacted:ref:v1", "{out}");
    assert_eq!(detail["key"], "redacted:ref:k1", "{out}");
    assert_eq!(detail["key_values"], serde_json::json!(["redacted:ref:k1"]));
    // No value-derived hash is computed in the default mode.
    let all = out.to_string();
    assert!(!all.contains("hmac") && !all.contains("blake3") && !all.contains("sha256:"));
}

#[test]
fn ordinals_follow_emission_order_not_name_order_and_survive_reordering() {
    let scratch = Scratch::new("reorder");
    let rows_old = "1,x\n2,y\n";
    let rows_new = "1,x\n";
    // Header order zeta,alpha: emission order is zeta first, so zeta is c1 although alpha sorts
    // first by bytes. Reordering the headers flips which name gets c1.
    for (headers, first_explicit) in [
        (format!("zeta_{COL_CANARY},alpha_{COL_CANARY}"), "zeta_"),
        (format!("alpha_{COL_CANARY},zeta_{COL_CANARY}"), "alpha_"),
    ] {
        let old = scratch.write("old.csv", &format!("{headers}\n{rows_old}"));
        let new = scratch.write("new.csv", &format!("{headers}\n{rows_new}"));

        let explicit = json(&run(&old, &new, None, &["--explicit", "--json"]));
        let names = explicit["refusal"]["detail"]["suggested_keys"]
            .as_array()
            .expect("suggested_keys")
            .clone();
        assert!(
            names[0]
                .as_str()
                .is_some_and(|name| name.starts_with(&format!("u8:{first_explicit}"))),
            "emission order must follow the data: {names:?}"
        );

        let default = json(&run(&old, &new, None, &["--json"]));
        assert_eq!(
            default["refusal"]["detail"]["suggested_keys"],
            serde_json::json!(["redacted:ref:c1", "redacted:ref:c2"]),
            "{default}"
        );
    }
}

#[test]
fn equal_values_in_one_output_share_a_ref() {
    let scratch = Scratch::new("share");
    // The same text appears as the failing column name of a duplicate header and... a single
    // refusal only names one value, so check the shared-ref rule on the key tuple: the composite
    // key of equal parts is one marker, stable between two runs.
    let old = scratch.write(
        "old.csv",
        &format!("id,v\n{KEY_CANARY},1\n{KEY_CANARY},2\n"),
    );
    let new = scratch.write("new.csv", &format!("id,v\n{KEY_CANARY},1\n"));
    let first = text(&run(&old, &new, None, &["--key", "id", "--json"]));
    let second = text(&run(&old, &new, None, &["--key", "id", "--json"]));
    assert_eq!(
        first, second,
        "default output must be byte-identical on rerun"
    );
    assert!(first.contains("redacted:ref:k1"));
}

#[test]
fn keyed_markers_match_the_independent_openssl_known_answers() {
    let scratch = Scratch::new("keyed");
    let missingness = &cases()[0];
    let old = scratch.write("old.csv", &missingness.old);
    let new = scratch.write("new.csv", &missingness.new);
    let output = run(&old, &new, Some(TEST_KEY), &["--key", "id", "--json"]);
    let out = json(&output);
    let detail = &out["refusal"]["detail"];
    assert_eq!(detail["value"], VALUE_MARKER, "{out}");
    assert_eq!(detail["column"], COLUMN_MARKER, "{out}");
    assert_eq!(detail["key"], KEY_MARKER, "{out}");
    let all = text(&output);
    for canary in [KEY_CANARY, COL_CANARY, CELL_NUMBER, TEST_KEY] {
        assert!(!all.contains(canary), "{canary} leaked: {all}");
    }

    // Human output shows the same keyed markers and still no raw data.
    let human = text(&run(&old, &new, Some(TEST_KEY), &["--key", "id"]));
    assert!(
        human.contains(VALUE_MARKER) && human.contains(KEY_MARKER),
        "{human}"
    );
    for canary in [KEY_CANARY, COL_CANARY, CELL_NUMBER, TEST_KEY] {
        assert!(!human.contains(canary), "{canary} leaked: {human}");
    }
}

#[test]
fn keyed_key_mismatch_lists_markers_and_default_withholds_samples() {
    let scratch = Scratch::new("mismatch");
    let mismatch = &cases()[3];
    let old = scratch.write("old.csv", &mismatch.old);
    let new = scratch.write("new.csv", &mismatch.new);

    let keyed = json(&run(&old, &new, Some(TEST_KEY), &["--key", "id", "--json"]));
    let samples = keyed["refusal"]["detail"]["missing_key_samples"]
        .as_array()
        .expect("samples");
    assert_eq!(samples.len(), 1, "{keyed}");
    assert_eq!(samples[0], serde_json::json!([KEY_MARKER]));

    let default = json(&run(&old, &new, None, &["--key", "id", "--json"]));
    let detail = &default["refusal"]["detail"];
    assert_eq!(detail["missing_in_new"], 1);
    assert_eq!(detail["missing_key_samples"], serde_json::json!([]));
    assert_eq!(detail["samples_withheld"], true, "{default}");
}

#[test]
fn empty_or_short_key_refuses_before_printing_any_data() {
    let scratch = Scratch::new("badkey");
    let missingness = &cases()[0];
    let old = scratch.write("old.csv", &missingness.old);
    let new = scratch.write("new.csv", &missingness.new);
    for bad in ["", "0123456789abcde"] {
        let output = run(&old, &new, Some(bad), &["--key", "id", "--json"]);
        assert_eq!(output.status.code(), Some(2));
        let out = json(&output);
        assert_eq!(out["refusal"]["code"], "E_REDACTION_KEY_INVALID", "{out}");
        let all = text(&output);
        for canary in [KEY_CANARY, COL_CANARY, CELL_NUMBER] {
            assert!(!all.contains(canary), "{canary} leaked: {all}");
        }
    }
}

#[test]
fn json_stdout_ends_with_a_newline_on_every_json_path() {
    let scratch = Scratch::new("newline");
    let old = scratch.write("old.csv", "id,v\nA,1\nB,2\n");
    let changed = scratch.write("new.csv", "id,v\nA,1\nB,9\n");
    let same = scratch.write("same.csv", "id,v\nA,1\nB,2\n");
    let broken = scratch.write("broken.csv", "id,v\nA,1\nA,2\n");

    let mut outputs: Vec<(&str, Output)> = vec![
        (
            "real change",
            run(&old, &changed, None, &["--key", "id", "--json"]),
        ),
        (
            "no real change",
            run(&old, &same, None, &["--key", "id", "--json"]),
        ),
        (
            "refusal",
            run(&old, &broken, None, &["--key", "id", "--json"]),
        ),
    ];
    let rvl = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_rvl"))
            .env("EPISTEMIC_WITNESS", scratch.0.join("witness.jsonl"))
            .args(args)
            .output()
            .expect("run rvl")
    };
    // One real recorded run, so `witness last` has a record to print (an empty ledger prints
    // nothing on stdout by rvl's own contract: exit 1 and a stderr note).
    let recorded = rvl(&[
        old.to_str().expect("utf-8 path"),
        changed.to_str().expect("utf-8 path"),
        "--key",
        "id",
        "--json",
    ]);
    assert_eq!(
        recorded.status.code(),
        Some(1),
        "recorded run must be REAL_CHANGE"
    );
    outputs.push(("doctor", rvl(&["doctor", "--json"])));
    outputs.push(("doctor health", rvl(&["doctor", "health", "--json"])));
    outputs.push(("capabilities", rvl(&["capabilities", "--json"])));
    outputs.push(("witness query", rvl(&["witness", "query", "--json"])));
    outputs.push(("witness last", rvl(&["witness", "last", "--json"])));
    outputs.push(("witness count", rvl(&["witness", "count", "--json"])));
    outputs.push(("describe", rvl(&["--describe"])));
    outputs.push(("schema", rvl(&["--schema"])));

    for (label, output) in &outputs {
        assert!(
            !output.stdout.is_empty(),
            "{label}: no stdout (stderr: {})",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stdout.ends_with(b"\n"),
            "{label}: stdout must end with a newline: {:?}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}
