//! Regressions found by the cmd-6nd slice 2 property tests, with minimized counterexamples.
//! Each was committed together with its fix.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        // Unique per call: tests run in parallel, and two sharing a directory would overwrite
        // each other's inputs.
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("rvl-propreg-{}-{name}-{seq}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn rvl(dir: &Path, old: &str, new: &str, extra: &[&str]) -> Output {
    let old_path = dir.join("old.csv");
    let new_path = dir.join("new.csv");
    std::fs::write(&old_path, old).expect("write old");
    std::fs::write(&new_path, new).expect("write new");
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .arg(&old_path)
        .arg(&new_path)
        .args(extra)
        .args(["--json", "--no-witness"])
        .output()
        .expect("run rvl")
}

/// A pair of finite numbers that differ must never be reported as NO_REAL_CHANGE, and the run
/// must end in a verdict or a typed refusal (exit 1 or 2), never a panic.
fn assert_not_a_false_no_change(old: &str, new: &str) -> Result<(), String> {
    let scratch = Scratch::new("overflow");
    let output = rvl(&scratch.0, old, new, &["--key", "id", "--explicit"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stderr.contains("panicked") {
        return Err(format!("panic: {}", stderr.trim()));
    }
    if !matches!(output.status.code(), Some(1) | Some(2)) {
        return Err(format!(
            "exit {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout)
        ));
    }
    let value: Value = serde_json::from_slice(&output.stdout).map_err(|e| e.to_string())?;
    if value["outcome"] == "NO_REAL_CHANGE" {
        return Err(format!("NO_REAL_CHANGE: {value}"));
    }
    Ok(())
}

/// Minimized: one cell whose difference is not representable as a finite f64.
#[test]
fn a_difference_that_overflows_f64_is_not_a_false_no_change() {
    let result = assert_not_a_false_no_change("id,v\nk,1e308\n", "id,v\nk,-1e308\n");
    assert_eq!(result, Ok(()));
}

/// Minimized: two finite differences whose SUM overflows f64.
#[test]
fn a_total_that_overflows_f64_is_not_a_false_no_change() {
    let result = assert_not_a_false_no_change("id,v\na,0\nb,0\n", "id,v\na,1e308\nb,1e308\n");
    assert_eq!(result, Ok(()));
}

/// Seeded: every table of huge finite values with at least one differing pair.
#[test]
fn huge_finite_values_never_produce_a_false_no_change() {
    const SEED: u64 = 0xB002;
    const CASES: usize = 60;
    const VALUES: &[&str] = &[
        "0",
        "1",
        "1e308",
        "-1e308",
        "9e307",
        "-9e307",
        "1.7976931348623157e308",
    ];
    let mut state = SEED.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut below = |bound: u64| {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D) % bound
    };
    let mut failures = Vec::new();
    let mut generated = 0;
    while generated < CASES {
        let rows = 1 + below(3) as usize;
        let mut old = String::from("id,v\n");
        let mut new = old.clone();
        let mut differs = false;
        for row in 0..rows {
            let a = VALUES[below(VALUES.len() as u64) as usize];
            let b = VALUES[below(VALUES.len() as u64) as usize];
            differs |= a.parse::<f64>().expect("a") != b.parse::<f64>().expect("b");
            old.push_str(&format!("k{row},{a}\n"));
            new.push_str(&format!("k{row},{b}\n"));
        }
        // "1" against "0" is a real difference too, so every kept case has one.
        if !differs {
            continue;
        }
        if let Err(why) = assert_not_a_false_no_change(&old, &new) {
            failures.push(format!(
                "seed={SEED:#x} case={generated}: {}\n  old={old:?} new={new:?}",
                why.chars().take(160).collect::<String>()
            ));
        }
        generated += 1;
    }
    assert!(
        failures.is_empty(),
        "{} of {CASES} cases failed; first:\n{}",
        failures.len(),
        failures
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ── bd-fyj: ties at the top-25 cap ──

fn tie_table(order: &[usize], value: u32) -> String {
    let header: Vec<String> = order.iter().map(|c| format!("c{c:02}")).collect();
    let row = vec![value.to_string(); order.len()];
    format!("id,{}\nk,{}\n", header.join(","), row.join(","))
}

fn contributor_columns(order: &[usize]) -> Vec<String> {
    let scratch = Scratch::new("ties");
    let output = rvl(
        &scratch.0,
        &tie_table(order, 0),
        &tie_table(order, 1),
        &["--key", "id", "--explicit"],
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("json");
    assert_eq!(value["outcome"], "REAL_CHANGE", "{value}");
    value["contributors"]
        .as_array()
        .expect("contributors")
        .iter()
        .map(|c| c["column"].as_str().expect("column").to_owned())
        .collect()
}

/// Minimized: one row, 26 equal changes, default settings. PLAN_RVL.md "Determinism": top-K
/// selection uses the ordering contribution desc, row_id asc, raw column bytes asc, so the 25
/// reported columns are c00..c24 whatever order the header lists them in.
#[test]
fn tied_cells_at_the_top_25_cap_are_chosen_by_column_bytes_not_header_order() {
    let expected: Vec<String> = (0..25).map(|c| format!("u8:c{c:02}")).collect();
    let in_byte_order: Vec<usize> = (0..26).collect();
    assert_eq!(contributor_columns(&in_byte_order), expected, "control");
    let reversed: Vec<usize> = (0..26).rev().collect();
    assert_eq!(contributor_columns(&reversed), expected, "reversed header");
}

/// 40 tied cells over two rows, 24 reported: row id comes before column bytes, in key mode and
/// in row-order mode, for several header orders (old and new files ordered differently too).
#[test]
fn ties_across_rows_and_columns_follow_row_id_then_column_bytes() {
    let forward: Vec<usize> = (0..20).collect();
    let reversed: Vec<usize> = (0..20).rev().collect();
    // 7 is coprime with 20, so this visits every column once in a scattered order.
    let scattered: Vec<usize> = (0..20).map(|i| (i * 7) % 20).collect();
    let table = |order: &[usize], value: u32, keyed: bool| {
        let header: Vec<String> = order.iter().map(|c| format!("c{c:02}")).collect();
        let row = vec![value.to_string(); order.len()].join(",");
        if keyed {
            format!("id,{}\nr1,{row}\nr2,{row}\n", header.join(","))
        } else {
            format!("{}\n{row}\n{row}\n", header.join(","))
        }
    };
    for keyed in [true, false] {
        let (first, second) = if keyed {
            ("u8:r1", "u8:r2")
        } else {
            ("u8:1", "u8:2")
        };
        let expected: Vec<(String, String)> = (0..20)
            .map(|c| (first.to_owned(), format!("u8:c{c:02}")))
            .chain((0..4).map(|c| (second.to_owned(), format!("u8:c{c:02}"))))
            .collect();
        for (old_order, new_order) in [
            (&forward, &forward),
            (&reversed, &reversed),
            (&scattered, &forward),
            (&forward, &scattered),
        ] {
            let scratch = Scratch::new("crossties");
            let mut args = vec!["--explicit", "--threshold", "0.6"];
            if keyed {
                args.extend(["--key", "id"]);
            }
            let output = rvl(
                &scratch.0,
                &table(old_order, 0, keyed),
                &table(new_order, 1, keyed),
                &args,
            );
            let value: Value = serde_json::from_slice(&output.stdout).expect("json");
            assert_eq!(value["outcome"], "REAL_CHANGE", "{value}");
            let got: Vec<(String, String)> = value["contributors"]
                .as_array()
                .expect("contributors")
                .iter()
                .map(|c| {
                    (
                        c["row_id"].as_str().expect("row").to_owned(),
                        c["column"].as_str().expect("column").to_owned(),
                    )
                })
                .collect();
            assert_eq!(
                got, expected,
                "keyed={keyed} old={old_order:?} new={new_order:?}"
            );
        }
    }
}

// ── bd-9ss: the chosen behaviour is a typed refusal, E_NUMERIC_OVERFLOW ──

fn refusal(old: &str, new: &str, args: &[&str]) -> Value {
    let scratch = Scratch::new("refusal");
    let output = rvl(&scratch.0, old, new, args);
    let value: Value = serde_json::from_slice(&output.stdout).expect("json");
    assert_eq!(output.status.code(), Some(2), "{args:?}: {value}");
    assert_eq!(value["outcome"], "REFUSAL", "{args:?}: {value}");
    assert_eq!(
        value["refusal"]["code"], "E_NUMERIC_OVERFLOW",
        "{args:?}: {value}"
    );
    // No number computed from the overflow is ever printed.
    assert!(value["metrics"]["total_change"].is_null(), "{value}");
    assert!(value["metrics"]["max_abs_delta"].is_null(), "{value}");
    assert!(value["metrics"]["top_k_coverage"].is_null(), "{value}");
    assert!(
        value["counts"]["numeric_cells_changed"].is_null(),
        "{value}"
    );
    assert_eq!(value["contributors"], serde_json::json!([]), "{value}");
    value
}

#[test]
fn a_delta_overflow_names_the_quantity_column_and_key() {
    let value = refusal(
        "id,v\nk,1e308\n",
        "id,v\nk,-1e308\n",
        &["--key", "id", "--explicit"],
    );
    let detail = &value["refusal"]["detail"];
    assert_eq!(detail["quantity"], "delta", "{value}");
    assert_eq!(detail["column"], "u8:v");
    assert_eq!(detail["key"], "u8:k");
    assert_eq!(detail["key_values"], serde_json::json!(["u8:k"]));
}

#[test]
fn a_sum_overflow_names_the_cell_that_pushed_the_total_over() {
    let value = refusal(
        "id,v\na,0\nb,0\nc,0\n",
        "id,v\na,1e308\nb,1e308\nc,1\n",
        &["--key", "id", "--explicit"],
    );
    let detail = &value["refusal"]["detail"];
    assert_eq!(detail["quantity"], "total_change", "{value}");
    assert_eq!(detail["key"], "u8:b", "{value}");
}

/// Every alignment and output mode, including a tolerance as large as the CLI accepts.
#[test]
fn every_mode_refuses_an_overflow() {
    let (old, new) = ("id,v\nk,1e308\n", "id,v\nk,-1e308\n");
    for extra in [
        &["--key", "id"][..],
        &["--key", "id", "--explicit"][..],
        &["--key", "id", "--exhaustive"][..],
        &["--key", "id", "--non-numeric"][..],
        &["--key", "id", "--key-mode", "overlap"][..],
        &["--key", "id", "--exit-zero-on-change"][..],
        &["--key", "id", "--tolerance", "1.7976931348623157e308"][..],
        &["--key", "id", "--threshold", "0.01"][..],
    ] {
        refusal(old, new, extra);
    }
    // The same through the audit path with a limit smaller than the change count: overflow wins
    // over E_AUDIT_LIMIT, which would report an infinite total.
    refusal(
        "id,v\na,0\nb,0\n",
        "id,v\na,1e308\nb,1e308\n",
        &["--key", "id", "--exhaustive", "--max-audit-changes", "1"],
    );
    // Row-order alignment with nothing reordered.
    let value = refusal("v,w\n1e308,1\n0,2\n", "v,w\n-1e308,1\n0,2\n", &[]);
    assert_eq!(value["refusal"]["detail"]["record"], 1, "{value}");
}

/// A reordered row-order file is told to use --key first: the overflow comes from misalignment.
#[test]
fn a_reordered_row_order_file_still_gets_e_need_key_first() {
    let scratch = Scratch::new("needkey");
    let output = rvl(
        &scratch.0,
        "id,v\na,1e308\nb,-1e308\n",
        "id,v\nb,-1e308\na,1e308\n",
        &[],
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("json");
    assert_eq!(value["refusal"]["code"], "E_NEED_KEY", "{value}");
}

/// No over-refusal: results that are still finite, even at the edge of the range, are verdicts.
#[test]
fn finite_results_near_the_edge_still_get_a_verdict() {
    for (old, new, extra, outcome, code) in [
        (
            "id,v\nk,0\n",
            "id,v\nk,1.7976931348623157e308\n",
            &["--key", "id"][..],
            "REAL_CHANGE",
            1,
        ),
        (
            "id,v\na,0\nb,0\n",
            "id,v\na,8e307\nb,8e307\n",
            &["--key", "id"][..],
            "REAL_CHANGE",
            1,
        ),
        // A finite difference inside a huge tolerance is a genuine "no change".
        (
            "id,v\nk,0\n",
            "id,v\nk,1e308\n",
            &["--key", "id", "--tolerance", "1.5e308"][..],
            "NO_REAL_CHANGE",
            0,
        ),
        // Huge but equal values do not overflow anything.
        (
            "id,v\nk,1.7976931348623157e308\n",
            "id,v\nk,1.7976931348623157e308\n",
            &["--key", "id"][..],
            "NO_REAL_CHANGE",
            0,
        ),
    ] {
        let scratch = Scratch::new("edge");
        let output = rvl(&scratch.0, old, new, extra);
        let value: Value = serde_json::from_slice(&output.stdout).expect("json");
        assert_eq!(value["outcome"], outcome, "{old:?} {new:?} {value}");
        assert_eq!(output.status.code(), Some(code), "{value}");
    }
}

/// Redaction (bd-ez4 convention): by default no column name, key or value appears in JSON or
/// human output; markers are ordinal refs, or keyed HMAC known answers computed with openssl
/// (`COLCANARY`, and the length-framed key `KEYCANARY-77`, key `0123456789abcdef`).
#[test]
fn the_overflow_refusal_is_redacted_unless_explicit() {
    let old = "id,COLCANARY\nKEYCANARY-77,1e308\n";
    let new = "id,COLCANARY\nKEYCANARY-77,-1e308\n";
    let scratch = Scratch::new("redaction");
    let run = |key: Option<&str>, args: &[&str]| {
        std::fs::write(scratch.0.join("old.csv"), old).expect("write old");
        std::fs::write(scratch.0.join("new.csv"), new).expect("write new");
        let mut command = Command::new(env!("CARGO_BIN_EXE_rvl"));
        command
            .arg(scratch.0.join("old.csv"))
            .arg(scratch.0.join("new.csv"))
            .args(["--key", "id", "--no-witness"])
            .args(args)
            .env_remove("RVL_REDACTION_KEY");
        if let Some(key) = key {
            command.env("RVL_REDACTION_KEY", key);
        }
        let output = command.output().expect("run rvl");
        assert_eq!(output.status.code(), Some(2));
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    };
    for args in [&["--json"][..], &[][..]] {
        let text = run(None, args);
        for canary in ["COLCANARY", "KEYCANARY", "1e308", "e308"] {
            assert!(!text.contains(canary), "{args:?} leaked {canary}: {text}");
        }
        assert!(
            text.contains("redacted:ref:c1") && text.contains("redacted:ref:k1"),
            "{text}"
        );
        let keyed = run(Some("0123456789abcdef"), args);
        assert!(!keyed.contains("COLCANARY") && !keyed.contains("KEYCANARY"));
        assert!(
            keyed.contains("redacted:hmac-sha256:e132ce51215d63c4642d82f8f78b6046")
                && keyed.contains("redacted:hmac-sha256:779b738c46bd51be4af111c5690bec8a"),
            "{keyed}"
        );
    }
    let explicit = run(None, &["--explicit"]);
    assert!(
        explicit.contains("COLCANARY") && explicit.contains("KEYCANARY-77"),
        "{explicit}"
    );
}
