// Human output headers (bd-2z3)

use crate::format::numbers::{
    format_float_shortest, format_int_with_commas, format_percent_one_decimal,
};

#[derive(Debug, Clone, Copy)]
pub enum Alignment<'a> {
    Key { column: &'a str },
    RowOrder,
}

impl<'a> Alignment<'a> {
    fn render(self) -> String {
        match self {
            Alignment::Key { column } => format!("key={column}"),
            Alignment::RowOrder => "row-order (no key)".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnCounts {
    pub common: u64,
    pub old_only: u64,
    pub new_only: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct CheckedCounts {
    pub rows: u64,
    pub numeric_columns: u64,
    pub cells: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct DialectReceipt {
    pub delimiter: u8,
    pub quote: u8,
    pub escape: Option<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub threshold: f64,
    pub tolerance: f64,
}

pub struct HumanHeader<'a> {
    pub old_name: &'a str,
    pub new_name: &'a str,
    pub alignment: Alignment<'a>,
    pub columns: ColumnCounts,
    pub checked: CheckedCounts,
    pub dialect_old: DialectReceipt,
    pub dialect_new: DialectReceipt,
    pub settings: Settings,
}

pub struct RefusalHeader<'a> {
    pub old_name: &'a str,
    pub new_name: &'a str,
    pub alignment: Alignment<'a>,
    pub dialect_old: Option<DialectReceipt>,
    pub dialect_new: Option<DialectReceipt>,
    pub settings: Settings,
}

pub fn render_real_no_real_header(ctx: &HumanHeader<'_>) -> Vec<String> {
    vec![
        format!("Compared: {} -> {}", ctx.old_name, ctx.new_name),
        format!("Alignment: {}", ctx.alignment.render()),
        format!(
            "Columns: common={} old_only={} new_only={}",
            format_count(ctx.columns.common),
            format_count(ctx.columns.old_only),
            format_count(ctx.columns.new_only)
        ),
        format!(
            "Checked: {} rows, {} numeric columns ({} cells)",
            format_count(ctx.checked.rows),
            format_count(ctx.checked.numeric_columns),
            format_count(ctx.checked.cells)
        ),
        format!("Dialect(old): {}", render_dialect(ctx.dialect_old)),
        format!("Dialect(new): {}", render_dialect(ctx.dialect_new)),
        "Ranking: abs(delta) (unscaled)".to_string(),
        format!(
            "Settings: threshold={} tolerance={}",
            format_percent_one_decimal(ctx.settings.threshold),
            format_float_shortest(ctx.settings.tolerance)
        ),
    ]
}

pub fn render_refusal_header(ctx: &RefusalHeader<'_>) -> Vec<String> {
    let mut lines = Vec::with_capacity(5);
    lines.push(format!("Compared: {} -> {}", ctx.old_name, ctx.new_name));
    lines.push(format!("Alignment: {}", ctx.alignment.render()));
    if let (Some(old), Some(new)) = (ctx.dialect_old, ctx.dialect_new) {
        lines.push(format!("Dialect(old): {}", render_dialect(old)));
        lines.push(format!("Dialect(new): {}", render_dialect(new)));
    }
    lines.push(format!(
        "Settings: threshold={} tolerance={}",
        format_percent_one_decimal(ctx.settings.threshold),
        format_float_shortest(ctx.settings.tolerance)
    ));
    lines
}

fn format_count(value: u64) -> String {
    match i64::try_from(value) {
        Ok(v) => format_int_with_commas(v),
        Err(_) => value.to_string(),
    }
}

fn render_dialect(dialect: DialectReceipt) -> String {
    let delimiter = format_delimiter(dialect.delimiter);
    let quote = format_quote(dialect.quote);
    let escape = format_escape(dialect.escape);
    format!("delimiter={delimiter} quote={quote} escape={escape}")
}

fn format_delimiter(delimiter: u8) -> String {
    if delimiter == b'\t' {
        return "TAB".to_string();
    }
    if is_visible_ascii(delimiter) {
        return (delimiter as char).to_string();
    }
    format!("0x{:02X}", delimiter)
}

fn format_quote(quote: u8) -> String {
    if is_visible_ascii(quote) {
        (quote as char).to_string()
    } else {
        format!("0x{:02X}", quote)
    }
}

fn format_escape(escape: Option<u8>) -> String {
    match escape {
        None => "none".to_string(),
        Some(b'\\') => "\\\\".to_string(),
        Some(byte) if is_visible_ascii(byte) => (byte as char).to_string(),
        Some(byte) => format!("0x{:02X}", byte),
    }
}

fn is_visible_ascii(byte: u8) -> bool {
    (0x21..=0x7e).contains(&byte)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_delimiter_variants() {
        assert_eq!(format_delimiter(b','), ",");
        assert_eq!(format_delimiter(b'\t'), "TAB");
        assert_eq!(format_delimiter(0x1f), "0x1F");
        assert_eq!(format_delimiter(b' '), "0x20");
    }

    #[test]
    fn renders_real_header_lines() {
        let ctx = HumanHeader {
            old_name: "old.csv",
            new_name: "new.csv",
            alignment: Alignment::Key { column: "id" },
            columns: ColumnCounts {
                common: 15,
                old_only: 2,
                new_only: 1,
            },
            checked: CheckedCounts {
                rows: 4183,
                numeric_columns: 12,
                cells: 50196,
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

        let lines = render_real_no_real_header(&ctx);
        assert_eq!(lines[0], "Compared: old.csv -> new.csv");
        assert_eq!(lines[1], "Alignment: key=id");
        assert_eq!(lines[2], "Columns: common=15 old_only=2 new_only=1");
        assert_eq!(
            lines[3],
            "Checked: 4,183 rows, 12 numeric columns (50,196 cells)"
        );
        assert_eq!(lines[4], "Dialect(old): delimiter=, quote=\" escape=none");
        assert_eq!(lines[5], "Dialect(new): delimiter=, quote=\" escape=none");
        assert_eq!(lines[6], "Ranking: abs(delta) (unscaled)");
        assert_eq!(lines[7], "Settings: threshold=95.0% tolerance=1e-9");
    }

    #[test]
    fn renders_refusal_without_dialect() {
        let ctx = RefusalHeader {
            old_name: "old.csv",
            new_name: "new.csv",
            alignment: Alignment::RowOrder,
            dialect_old: None,
            dialect_new: None,
            settings: Settings {
                threshold: 0.95,
                tolerance: 1e-9,
            },
        };

        let lines = render_refusal_header(&ctx);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "Compared: old.csv -> new.csv");
        assert_eq!(lines[1], "Alignment: row-order (no key)");
        assert_eq!(lines[2], "Settings: threshold=95.0% tolerance=1e-9");
    }
}
