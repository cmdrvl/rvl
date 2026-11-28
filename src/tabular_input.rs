//! Non-CSV inputs (JSONL, Parquet, XLSX/XLS) via the shared `cmdrvl-tabular` reader.
//!
//! The comparison pipeline is built around CSV bytes (dialect handling, record width rules,
//! numeric parsing, keys). Rather than fork it, a non-CSV input is read through tabular and
//! re-rendered as canonical CSV bytes IN MEMORY (comma, or the forced delimiter, `\n` line
//! ends, quotes only where needed), then handed to the unchanged CSV path. Consequences:
//!
//! - identical data in any container yields identical bytes after this step, so verdicts match
//!   across formats by construction (golden tests in `tests/formats.rs` check that);
//! - CSV and TSV inputs never come through here: they keep rvl's own reader so existing
//!   byte-deterministic reports cannot change;
//! - nothing is written to disk and tabular reads no environment variables.

use std::path::Path;

use cmdrvl_tabular::{Format, OpenOptions, SheetSel, open, open_bytes};

/// A tabular refusal reduced to its typed, value-free parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabularRefusal {
    /// `E_*` code from cmdrvl-tabular (`E_IO` is reported as rvl's own `E_IO` instead).
    pub code: String,
    pub detail: String,
    /// Sorted context (sheet listings, row/column coordinates, type names; never cell values).
    pub context: serde_json::Value,
    pub is_io: bool,
}

/// Formats that go through tabular, by extension. Anything else (including `.csv`, `.tsv`,
/// `.txt`, no extension) keeps the CSV path unchanged.
pub fn is_tabular_path(path: &Path) -> Option<&'static str> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    match extension.as_str() {
        "jsonl" | "ndjson" => Some("jsonl"),
        "parquet" => Some("parquet"),
        "xlsx" | "xlsm" | "xls" => Some("xlsx"),
        _ => None,
    }
}

pub fn is_tabular_name(name: &str) -> bool {
    let extension = name
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase());
    matches!(
        extension.as_deref(),
        Some("jsonl" | "ndjson" | "parquet" | "xlsx" | "xlsm" | "xls")
    )
}

fn format_for_name(name: &str) -> Option<Format> {
    let extension = name.rsplit_once('.')?.1.to_ascii_lowercase();
    match extension.as_str() {
        "jsonl" | "ndjson" => Some(Format::Jsonl),
        "parquet" => Some(Format::Parquet),
        "xlsx" | "xlsm" | "xls" => Some(Format::Xlsx),
        _ => None,
    }
}

/// Read `path` through tabular and render it as CSV bytes with `delimiter`.
pub fn read_as_csv(
    path: &Path,
    sheet: Option<&str>,
    delimiter: u8,
) -> Result<Vec<u8>, Box<TabularRefusal>> {
    let opts = OpenOptions {
        sheet: sheet.map(SheetSel::parse),
        ..OpenOptions::default()
    };
    let table = open(path, &opts).map_err(refusal_parts)?;
    render_table(table, delimiter)
}

/// Read a native tabular container from memory and render it as canonical CSV bytes.
pub fn render_bytes_as_csv(
    bytes: &[u8],
    name: &str,
    sheet: Option<&str>,
    delimiter: u8,
) -> Result<Vec<u8>, Box<TabularRefusal>> {
    let opts = OpenOptions {
        format: format_for_name(name),
        sheet: sheet.map(SheetSel::parse),
        ..OpenOptions::default()
    };
    let table = open_bytes(bytes.to_vec(), Some(name), &opts).map_err(refusal_parts)?;
    render_table(table, delimiter)
}

fn render_table(
    mut table: cmdrvl_tabular::Table,
    delimiter: u8,
) -> Result<Vec<u8>, Box<TabularRefusal>> {
    debug_assert!(matches!(
        table.format(),
        Format::Jsonl | Format::Parquet | Format::Xlsx
    ));

    let mut writer = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .terminator(csv::Terminator::Any(b'\n'))
        .from_writer(Vec::new());
    writer
        .write_record(table.headers())
        .map_err(|_| render_failure())?;
    for row in table.rows() {
        let row = row.map_err(refusal_parts)?;
        writer
            .write_record(row.cells.iter().map(|cell| cell.as_bytes()))
            .map_err(|_| render_failure())?;
    }
    writer.into_inner().map_err(|_| render_failure())
}

fn refusal_parts(refusal: cmdrvl_tabular::Refusal) -> Box<TabularRefusal> {
    Box::new(TabularRefusal {
        code: refusal.code.as_str().to_owned(),
        detail: refusal.detail,
        context: serde_json::to_value(&refusal.context)
            .unwrap_or_else(|_| serde_json::Value::Object(Default::default())),
        is_io: refusal.code == cmdrvl_tabular::RefusalCode::EIo,
    })
}

fn render_failure() -> Box<TabularRefusal> {
    Box::new(TabularRefusal {
        code: "E_IO".to_owned(),
        detail: "could not render the input as CSV in memory".to_owned(),
        context: serde_json::Value::Object(Default::default()),
        is_io: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_non_csv_extensions_are_routed_to_tabular() {
        for (name, want) in [
            ("a.jsonl", Some("jsonl")),
            ("a.NDJSON", Some("jsonl")),
            ("a.parquet", Some("parquet")),
            ("a.xlsx", Some("xlsx")),
            ("a.xlsm", Some("xlsx")),
            ("a.xls", Some("xlsx")),
            ("a.csv", None),
            ("a.tsv", None),
            ("a.txt", None),
            ("a", None),
        ] {
            assert_eq!(is_tabular_path(Path::new(name)), want, "{name}");
        }
    }
}
