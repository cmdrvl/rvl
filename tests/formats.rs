//! JSONL, Parquet and XLSX inputs through the shared tabular reader. The same old/new loan
//! dataset exists as CSV, TSV, JSONL, Parquet and XLSX (independent writers, see
//! tests/fixtures/formats/make_format_fixtures.py). Every one of the 25 old x new pairings
//! must produce the same comparison as CSV vs CSV.

use serde_json::{Value, json};
use std::process::{Command, Output};

const EXTS: [&str; 5] = ["csv", "tsv", "jsonl", "parquet", "xlsx"];

fn fx(name: &str) -> String {
    format!(
        "{}/tests/fixtures/formats/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn rvl(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .args(args)
        .arg("--no-witness")
        .output()
        .expect("run rvl")
}

fn report(old: &str, new: &str, extra: &[&str]) -> (Option<i32>, Value) {
    let mut args = vec![old, new, "--key", "loan_id", "--json"];
    args.extend_from_slice(extra);
    let out = rvl(&args);
    let value: Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout is not JSON ({e}): {}",
            String::from_utf8_lossy(&out.stdout)
        )
    });
    (out.status.code(), value)
}

/// Drop the fields that legitimately differ by container: the file paths and the dialect
/// receipt (non-CSV inputs are rendered with a comma, TSV is tab).
fn comparison(mut v: Value) -> Value {
    let obj = v.as_object_mut().unwrap();
    obj.remove("files");
    obj.remove("dialect");
    v
}

#[test]
fn all_25_pairings_give_the_csv_comparison_redacted_and_explicit() {
    for extra in [&[][..], &["--explicit"][..]] {
        let (code, baseline) = report(&fx("old.csv"), &fx("new.csv"), extra);
        assert_eq!(code, Some(1), "REAL_CHANGE exits 1");
        assert_eq!(baseline["outcome"], "REAL_CHANGE");
        let baseline = comparison(baseline);
        let mut checked = 0;
        for o in EXTS {
            for n in EXTS {
                let (code, got) = report(&fx(&format!("old.{o}")), &fx(&format!("new.{n}")), extra);
                assert_eq!(code, Some(1), "old.{o} vs new.{n} {extra:?}");
                assert_eq!(comparison(got), baseline, "old.{o} vs new.{n} {extra:?}");
                checked += 1;
            }
        }
        assert_eq!(checked, 25);
    }
}

#[test]
fn explicit_report_shows_the_same_changed_cell_in_every_format() {
    for ext in EXTS {
        let (_, got) = report(
            &fx(&format!("old.{ext}")),
            &fx(&format!("new.{ext}")),
            &["--explicit"],
        );
        let c = &got["contributors"][0];
        assert_eq!(
            (
                c["row_id"].clone(),
                c["column"].clone(),
                c["old"].clone(),
                c["new"].clone()
            ),
            (
                json!("u8:L2"),
                json!("u8:balance"),
                json!(2000.0),
                json!(2150.0)
            ),
            "{ext}"
        );
    }
}

#[test]
fn identical_inputs_in_different_formats_are_no_real_change() {
    for a in EXTS {
        for b in EXTS {
            let (code, got) = report(&fx(&format!("new.{a}")), &fx(&format!("new.{b}")), &[]);
            assert_eq!(got["outcome"], "NO_REAL_CHANGE", "new.{a} vs new.{b}");
            assert_eq!(code, Some(0), "new.{a} vs new.{b}");
        }
    }
}

// ---- worksheets ------------------------------------------------------------

#[test]
fn multi_sheet_workbook_without_a_sheet_refuses_and_names_the_sheets() {
    let (code, got) = report(&fx("old.csv"), &fx("new_two_sheets.xlsx"), &[]);
    assert_eq!(code, Some(2));
    assert_eq!(got["refusal"]["code"], "E_INPUT_FORMAT");
    let detail = &got["refusal"]["detail"];
    assert_eq!(detail["file"], "new");
    assert_eq!(detail["tabular_code"], "E_AMBIGUOUS_SHEET");
    assert_eq!(detail["context"]["sheets"], json!(["notes", "loans"]));
}

#[test]
fn sheet_flags_select_by_name_index_and_per_side() {
    let baseline = comparison(report(&fx("old.csv"), &fx("new.csv"), &[]).1);
    for flags in [
        &["--sheet", "loans"][..],
        &["--new-sheet", "loans"][..],
        &["--sheet", "nope", "--new-sheet", "loans"][..], // per-side overrides --sheet
        &["--new-sheet", "1"][..],                        // 0-based index
    ] {
        let (code, got) = report(&fx("old.csv"), &fx("new_two_sheets.xlsx"), flags);
        assert_eq!(code, Some(1), "{flags:?}: {got}");
        assert_eq!(comparison(got), baseline, "{flags:?}");
    }
}

#[test]
fn unknown_sheet_refuses_with_the_listing() {
    let (code, got) = report(
        &fx("old.csv"),
        &fx("new_two_sheets.xlsx"),
        &["--new-sheet", "missing"],
    );
    assert_eq!(code, Some(2));
    assert_eq!(
        got["refusal"]["detail"]["tabular_code"],
        "E_SHEET_NOT_FOUND"
    );
    assert_eq!(
        got["refusal"]["detail"]["context"]["sheets"],
        json!(["notes", "loans"])
    );
}

// ---- refusals --------------------------------------------------------------

#[test]
fn unsupported_parquet_value_is_refused_without_the_value() {
    let (code, got) = report(&fx("old.csv"), &fx("binary.parquet"), &[]);
    assert_eq!(code, Some(2));
    assert_eq!(got["refusal"]["code"], "E_INPUT_FORMAT");
    assert_eq!(
        got["refusal"]["detail"]["tabular_code"],
        "E_UNSUPPORTED_VALUE"
    );
    assert!(!got.to_string().contains("secret"), "{got}");
}

#[test]
fn extension_that_disagrees_with_the_content_is_refused() {
    let (code, got) = report(&fx("old.csv"), &fx("not_really.parquet"), &[]);
    assert_eq!(code, Some(2));
    assert_eq!(got["refusal"]["detail"]["tabular_code"], "E_FORMAT");
}

#[test]
fn missing_non_csv_file_is_the_ordinary_io_refusal() {
    let (code, got) = report(&fx("old.csv"), &fx("does_not_exist.parquet"), &[]);
    assert_eq!(code, Some(2));
    assert_eq!(got["refusal"]["code"], "E_IO");
}

#[test]
fn csv_and_tsv_still_use_rvls_own_reader() {
    // The dialect receipt is how to tell: TSV is auto-detected as tab, CSV as comma, and
    // non-CSV inputs are rendered with a comma.
    let (_, tsv) = report(&fx("old.tsv"), &fx("new.tsv"), &[]);
    assert_eq!(tsv["dialect"]["old"]["delimiter"], json!("\t"));
    let (_, parquet) = report(&fx("old.parquet"), &fx("new.parquet"), &[]);
    assert_eq!(parquet["dialect"]["old"]["delimiter"], json!(","));
}
