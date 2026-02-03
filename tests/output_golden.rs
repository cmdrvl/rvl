use rvl::format::ident_human::render_identifier_human;
use rvl::output::human::header::{
    Alignment, CheckedCounts, ColumnCounts, DialectReceipt, HumanHeader, RefusalHeader, Settings,
    render_real_no_real_header, render_refusal_header,
};
use rvl::output::human::no_real::{NoRealBody, render_no_real_body};
use rvl::output::human::real_change::{
    RealChangeBody, RealChangeContributor, render_real_change_body,
};
use rvl::output::human::refusal::{RefusalBody, render_refusal_body};
use rvl::output::json::{
    Alignment as JsonAlignment, Counts, Dialect, DialectSide, Files, JsonContext, JsonOutput,
    Metrics,
};
use rvl::refusal::codes::RefusalCode;
use rvl::refusal::details::{FileSide, RefusalDetail, RefusalKind, RerunPaths};
use serde_json::json;

fn join_lines(lines: Vec<String>) -> String {
    lines.join("\n")
}

#[test]
fn golden_real_change_human_output() {
    let header = HumanHeader {
        old_name: "old.csv",
        new_name: "new.csv",
        alignment: Alignment::Key { column: "id" },
        columns: ColumnCounts {
            common: 3,
            old_only: 1,
            new_only: 0,
        },
        checked: CheckedCounts {
            rows: 2,
            numeric_columns: 1,
            cells: 2,
        },
        dialect_old: DialectReceipt {
            delimiter: b',',
            quote: b'"',
            escape: None,
        },
        dialect_new: DialectReceipt {
            delimiter: b',',
            quote: b'"',
            escape: None,
        },
        settings: Settings {
            threshold: 0.95,
            tolerance: 1e-9,
        },
    };
    let body = RealChangeBody {
        contributors: &[RealChangeContributor {
            label: "A.value".to_string(),
            old: 1.0,
            new: 6.0,
            delta: 5.0,
        }],
        coverage: 0.95,
        threshold: 0.95,
    };

    let mut lines = Vec::new();
    lines.push("RVL".to_string());
    lines.push(String::new());
    lines.push("REAL CHANGE".to_string());
    lines.push(String::new());
    lines.extend(render_real_no_real_header(&header));
    lines.push(String::new());
    lines.extend(render_real_change_body(&body));

    let actual = join_lines(lines);
    let expected = r#"RVL

REAL CHANGE

Compared: old.csv -> new.csv
Alignment: key=id
Columns: common=3 old_only=1 new_only=0
Checked: 2 rows, 1 numeric columns (2 cells)
Dialect(old): delimiter=, quote=" escape=none
Dialect(new): delimiter=, quote=" escape=none
Ranking: abs(delta) (unscaled)
Settings: threshold=95.0% tolerance=1e-9

1 cell explain 95.0% of total numeric change (threshold 95.0%):

1. A.value  +5  (1 -> 6)

Everything else in common numeric columns is <= tolerance or in the tail (not required to reach threshold)."#;
    assert_eq!(actual, expected);
}

#[test]
fn golden_no_real_change_human_output() {
    let header = HumanHeader {
        old_name: "old.csv",
        new_name: "new.csv",
        alignment: Alignment::RowOrder,
        columns: ColumnCounts {
            common: 2,
            old_only: 0,
            new_only: 0,
        },
        checked: CheckedCounts {
            rows: 2,
            numeric_columns: 2,
            cells: 4,
        },
        dialect_old: DialectReceipt {
            delimiter: b',',
            quote: b'"',
            escape: None,
        },
        dialect_new: DialectReceipt {
            delimiter: b',',
            quote: b'"',
            escape: None,
        },
        settings: Settings {
            threshold: 0.95,
            tolerance: 1e-9,
        },
    };
    let body = NoRealBody {
        max_abs_delta: 7e-10,
        tolerance: 1e-9,
    };

    let mut lines = Vec::new();
    lines.push("RVL".to_string());
    lines.push(String::new());
    lines.push("NO REAL CHANGE".to_string());
    lines.push(String::new());
    lines.extend(render_real_no_real_header(&header));
    lines.push(String::new());
    lines.extend(render_no_real_body(&body));

    let actual = join_lines(lines);
    let expected = r#"RVL

NO REAL CHANGE

Compared: old.csv -> new.csv
Alignment: row-order (no key)
Columns: common=2 old_only=0 new_only=0
Checked: 2 rows, 2 numeric columns (4 cells)
Dialect(old): delimiter=, quote=" escape=none
Dialect(new): delimiter=, quote=" escape=none
Ranking: abs(delta) (unscaled)
Settings: threshold=95.0% tolerance=1e-9

Max abs delta: 7e-10 (<= tolerance 1e-9).
No numeric deltas above tolerance in common numeric columns."#;
    assert_eq!(actual, expected);
}

#[test]
fn golden_refusal_human_output() {
    let header = RefusalHeader {
        old_name: "old.csv",
        new_name: "new.csv",
        alignment: Alignment::Key { column: "id" },
        dialect_old: Some(DialectReceipt {
            delimiter: b',',
            quote: b'"',
            escape: None,
        }),
        dialect_new: Some(DialectReceipt {
            delimiter: b',',
            quote: b'"',
            escape: None,
        }),
        settings: Settings {
            threshold: 0.95,
            tolerance: 1e-9,
        },
    };
    let detail = RefusalDetail::with_default_next(
        RefusalKind::KeyDup {
            file: FileSide::Old,
            record: 184,
            key_value: b"A123".to_vec(),
        },
        RerunPaths {
            old: "old.csv",
            new: "new.csv",
        },
    );
    let body = RefusalBody {
        code: RefusalCode::KeyDup,
        detail: &detail,
        old_name: "old.csv",
        new_name: "new.csv",
    };

    let mut lines = Vec::new();
    lines.push(format!("RVL ERROR ({})", RefusalCode::KeyDup));
    lines.push(String::new());
    lines.extend(render_refusal_header(&header));
    lines.push(String::new());
    lines.extend(render_refusal_body(&body));

    let actual = join_lines(lines);
    let expected = r#"RVL ERROR (E_KEY_DUP)

Compared: old.csv -> new.csv
Alignment: key=id
Dialect(old): delimiter=, quote=" escape=none
Dialect(new): delimiter=, quote=" escape=none
Settings: threshold=95.0% tolerance=1e-9

Reason (E_KEY_DUP): duplicate key values.
Example: old.csv data record 184 duplicates key "A123".
Next: choose a unique key column or dedupe the data, then rerun"#;
    assert_eq!(actual, expected);
}

#[test]
fn golden_json_real_change_output() {
    let ctx = JsonContext {
        files: Files {
            old: "old.csv".to_string(),
            new: "new.csv".to_string(),
        },
        alignment: JsonAlignment::key("u8:id".to_string()),
        dialect: Dialect {
            old: Some(DialectSide::new(b',', b'"', None)),
            new: Some(DialectSide::new(b',', b'"', None)),
        },
        threshold: 0.95,
        tolerance: 1e-9,
        counts: Counts {
            rows_old: Some(2),
            rows_new: Some(2),
            rows_aligned: Some(2),
            columns_old: Some(3),
            columns_new: Some(3),
            columns_common: Some(3),
            columns_old_only: Some(1),
            columns_new_only: Some(0),
            numeric_columns: Some(1),
            numeric_cells_checked: Some(2),
            numeric_cells_changed: Some(1),
        },
        metrics: Metrics {
            total_change: Some(5.0),
            max_abs_delta: Some(5.0),
            top_k_coverage: Some(1.0),
        },
    };
    let contributors = vec![rvl::output::json::Contributor::from_bytes(
        b"A",
        b"value",
        1.0,
        6.0,
        5.0,
        5.0,
        1.0,
        1.0,
    )];
    let output = JsonOutput::real_change(ctx, contributors);
    let value = serde_json::to_value(output).expect("json");

    let expected = json!({
        "version": "rvl.v0",
        "outcome": "REAL_CHANGE",
        "files": { "old": "old.csv", "new": "new.csv" },
        "alignment": { "mode": "key", "key_column": "u8:id" },
        "dialect": {
            "old": { "delimiter": ",", "quote": "\"", "escape": null },
            "new": { "delimiter": ",", "quote": "\"", "escape": null }
        },
        "threshold": 0.95,
        "tolerance": 1e-9,
        "counts": {
            "rows_old": 2,
            "rows_new": 2,
            "rows_aligned": 2,
            "columns_old": 3,
            "columns_new": 3,
            "columns_common": 3,
            "columns_old_only": 1,
            "columns_new_only": 0,
            "numeric_columns": 1,
            "numeric_cells_checked": 2,
            "numeric_cells_changed": 1
        },
        "metrics": { "total_change": 5.0, "max_abs_delta": 5.0, "top_k_coverage": 1.0 },
        "limits": { "max_contributors": 25 },
        "contributors": [{
            "row_id": "u8:A",
            "column": "u8:value",
            "old": 1.0,
            "new": 6.0,
            "delta": 5.0,
            "contribution": 5.0,
            "share": 1.0,
            "cumulative_share": 1.0
        }],
        "refusal": null
    });

    assert_eq!(value, expected);
    assert_eq!(render_identifier_human(b"A"), "A");
}
