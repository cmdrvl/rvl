// Human REFUSAL output formatting (bd-bk0)

use crate::format::ident_human::render_identifier_human;
use crate::format::numbers::{format_int_with_commas, format_percent_one_decimal};
use crate::refusal::codes::RefusalCode;
use crate::refusal::details::{EncodingIssue, FileSide, HeadersIssue, RefusalDetail, RefusalKind};

#[derive(Debug)]
pub struct RefusalBody<'a> {
    pub code: RefusalCode,
    pub detail: &'a RefusalDetail,
    pub old_name: &'a str,
    pub new_name: &'a str,
}

pub fn render_refusal_body(ctx: &RefusalBody<'_>) -> Vec<String> {
    let mut lines = Vec::with_capacity(4);
    lines.push("Cannot produce a verdict.".to_string());
    lines.push(format!("Reason ({}): {}.", ctx.code, ctx.code.reason()));
    lines.push(render_example_line(ctx.detail, ctx.old_name, ctx.new_name));
    lines.push(format!("Next: {}", ctx.detail.next));
    lines
}

fn render_example_line(detail: &RefusalDetail, old_name: &str, new_name: &str) -> String {
    match &detail.kind {
        RefusalKind::Io { file, error } => format!(
            "Example: {} file error: {}.",
            file_label(*file, old_name, new_name),
            error
        ),
        RefusalKind::Encoding { file, issue } => format!(
            "Example: {} contains {}.",
            file_label(*file, old_name, new_name),
            encoding_issue_label(*issue)
        ),
        RefusalKind::CsvParse { file, line, column } => {
            let file = file_label(*file, old_name, new_name);
            match (line, column) {
                (Some(line), Some(column)) => {
                    format!("Example: {file} parse error at line {line}, column {column}.")
                }
                (Some(line), None) => {
                    format!("Example: {file} parse error at line {line}.")
                }
                _ => format!("Example: {file} parse error (line unknown)."),
            }
        }
        RefusalKind::Headers { file, issue } => {
            let file = file_label(*file, old_name, new_name);
            match issue {
                HeadersIssue::MissingHeader => {
                    format!("Example: {file} has no header row.")
                }
                HeadersIssue::Duplicate { name } => {
                    let name = render_identifier_human(name);
                    format!("Example: {file} has duplicate header \"{name}\".")
                }
                HeadersIssue::ExtraFields { record } => {
                    format!(
                        "Example: {file} data record {} has non-empty extra fields.",
                        format_count_u64(*record)
                    )
                }
            }
        }
        RefusalKind::NoKey { key_column } => {
            let key = render_identifier_human(key_column);
            format!("Example: key column \"{key}\" not found in one or both files.")
        }
        RefusalKind::KeyEmpty {
            file,
            record,
            key_column,
        } => {
            let file = file_label(*file, old_name, new_name);
            let key = render_identifier_human(key_column);
            format!(
                "Example: {file} data record {} has empty key in column \"{key}\".",
                format_count_u64(*record)
            )
        }
        RefusalKind::KeyDup {
            file,
            record,
            key_value,
        } => {
            let file = file_label(*file, old_name, new_name);
            let value = render_identifier_human(key_value);
            format!(
                "Example: {file} data record {} duplicates key \"{value}\".",
                format_count_u64(*record)
            )
        }
        RefusalKind::KeyMismatch {
            missing_in_new,
            extra_in_new,
            missing_samples,
            extra_samples,
        } => {
            let missing = format_count_u64(*missing_in_new as u64);
            let extra = format_count_u64(*extra_in_new as u64);
            let missing_samples = render_samples(missing_samples);
            let extra_samples = render_samples(extra_samples);
            let mut line = format!("Example: missing_in_new={missing} extra_in_new={extra}.");
            if !missing_samples.is_empty() {
                line.push_str(&format!(" missing samples: [{missing_samples}]."));
            }
            if !extra_samples.is_empty() {
                line.push_str(&format!(" extra samples: [{extra_samples}]."));
            }
            line
        }
        RefusalKind::RowCount {
            rows_old,
            rows_new,
            suggested_keys,
        } => {
            let mut line = format!(
                "Example: row count mismatch (old={}, new={}).",
                format_count_u64(*rows_old),
                format_count_u64(*rows_new)
            );
            let keys = render_samples(suggested_keys);
            if !keys.is_empty() {
                line.push_str(&format!(" suggested keys: [{keys}]."));
            }
            line
        }
        RefusalKind::NeedKey { suggested_keys } => {
            let keys = render_samples(suggested_keys);
            if keys.is_empty() {
                "Example: detected a reorder under a perfect key candidate.".to_string()
            } else {
                format!("Example: suggested key candidates: [{keys}].")
            }
        }
        RefusalKind::Dialect {
            file,
            tied_delimiters,
            suggestion: _,
        } => {
            let file = file_label(*file, old_name, new_name);
            let list = render_delimiters(tied_delimiters);
            format!("Example: {file} delimiter ambiguous among [{list}].")
        }
        RefusalKind::MixedTypes {
            file,
            record,
            column,
            value,
            key_value,
        } => {
            let column = render_identifier_human(column);
            let value = render_identifier_human(value);
            if let Some(key) = key_value {
                let key = render_identifier_human(key);
                format!(
                    "Example: key \"{key}\" column \"{column}\" has non-numeric value \"{value}\"."
                )
            } else {
                let file = file_label(*file, old_name, new_name);
                format!(
                    "Example: {file} data record {} column \"{column}\" has non-numeric value \"{value}\".",
                    format_count_u64(*record)
                )
            }
        }
        RefusalKind::NoNumeric => "Example: no numeric columns in common.".to_string(),
        RefusalKind::Missingness {
            file,
            record,
            column,
            value,
            key_value,
        } => {
            let column = render_identifier_human(column);
            let value = render_identifier_human(value);
            if let Some(key) = key_value {
                let key = render_identifier_human(key);
                format!(
                    "Example: key \"{key}\" column \"{column}\" has numeric value \"{value}\" while the other side is missing."
                )
            } else {
                let file = file_label(*file, old_name, new_name);
                format!(
                    "Example: {file} data record {} column \"{column}\" has numeric value \"{value}\" while the other side is missing.",
                    format_count_u64(*record)
                )
            }
        }
        RefusalKind::Diffuse {
            top_k_coverage,
            threshold,
        } => format!(
            "Example: top_k_coverage={} threshold={}.",
            format_percent_one_decimal(*top_k_coverage),
            format_percent_one_decimal(*threshold)
        ),
    }
}

fn file_label<'a>(side: FileSide, old_name: &'a str, new_name: &'a str) -> &'a str {
    match side {
        FileSide::Old => old_name,
        FileSide::New => new_name,
    }
}

fn encoding_issue_label(issue: EncodingIssue) -> &'static str {
    match issue {
        EncodingIssue::Utf16 => "a UTF-16 BOM",
        EncodingIssue::Utf32 => "a UTF-32 BOM",
        EncodingIssue::NulByte => "a NUL byte in the first 8KB",
    }
}

fn render_samples(samples: &[Vec<u8>]) -> String {
    samples
        .iter()
        .map(|bytes| render_identifier_human(bytes))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_delimiters(delimiters: &[u8]) -> String {
    delimiters
        .iter()
        .map(|&byte| match byte {
            b',' => "comma".to_string(),
            b'\t' => "tab".to_string(),
            b';' => "semicolon".to_string(),
            b'|' => "pipe".to_string(),
            b'^' => "caret".to_string(),
            other => format!("0x{:02X}", other),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_count_u64(value: u64) -> String {
    match i64::try_from(value) {
        Ok(v) => format_int_with_commas(v),
        Err(_) => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refusal::details::{DialectSuggestion, RefusalDetail, RefusalKind, RerunPaths};

    #[test]
    fn renders_key_dup_example() {
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
        let ctx = RefusalBody {
            code: RefusalCode::KeyDup,
            detail: &detail,
            old_name: "old.csv",
            new_name: "new.csv",
        };
        let lines = render_refusal_body(&ctx);
        assert_eq!(lines[0], "Cannot produce a verdict.");
        assert_eq!(lines[1], "Reason (E_KEY_DUP): duplicate key values.");
        assert_eq!(
            lines[2],
            "Example: old.csv data record 184 duplicates key \"A123\"."
        );
        assert!(lines[3].starts_with("Next:"));
    }

    #[test]
    fn renders_diffuse_example() {
        let detail = RefusalDetail::new(
            RefusalKind::Diffuse {
                top_k_coverage: 0.8,
                threshold: 0.95,
            },
            "rvl old.csv new.csv --threshold 0.80".to_string(),
        );
        let ctx = RefusalBody {
            code: RefusalCode::Diffuse,
            detail: &detail,
            old_name: "old.csv",
            new_name: "new.csv",
        };
        let lines = render_refusal_body(&ctx);
        assert_eq!(lines[0], "Cannot produce a verdict.");
        assert_eq!(
            lines[1],
            "Reason (E_DIFFUSE): diffuse change below coverage threshold."
        );
        assert_eq!(lines[2], "Example: top_k_coverage=80.0% threshold=95.0%.");
    }

    #[test]
    fn renders_dialect_example_with_ties() {
        let detail = RefusalDetail::new(
            RefusalKind::Dialect {
                file: FileSide::Old,
                tied_delimiters: vec![b',', b'\t'],
                suggestion: DialectSuggestion::ForceDelimiter(
                    crate::refusal::details::DelimiterHint::Named(
                        crate::refusal::details::NamedDelimiter::Comma,
                    ),
                ),
            },
            "rvl old.csv new.csv --delimiter comma".to_string(),
        );
        let ctx = RefusalBody {
            code: RefusalCode::Dialect,
            detail: &detail,
            old_name: "old.csv",
            new_name: "new.csv",
        };
        let lines = render_refusal_body(&ctx);
        assert_eq!(
            lines[2],
            "Example: old.csv delimiter ambiguous among [comma, tab]."
        );
    }
}
