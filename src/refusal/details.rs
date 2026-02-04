//! Refusal detail payloads & Next steps (bd-2kk).
//!
//! Detail payloads carry concrete examples for each refusal code, plus a
//! deterministic "next" remediation or rerun command. Identifiers are stored as
//! raw bytes and should be rendered using the identifier formatters at output
//! time.

use crate::format::ident_json::encode_identifier_json;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileSide {
    Old,
    New,
}

impl FileSide {
    pub fn as_str(self) -> &'static str {
        match self {
            FileSide::Old => "old",
            FileSide::New => "new",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RerunPaths<'a> {
    pub old: &'a str,
    pub new: &'a str,
}

#[derive(Debug, Clone)]
pub struct RefusalDetail {
    pub kind: RefusalKind,
    /// Next-step remediation or rerun guidance (without "Next:" prefix).
    pub next: String,
}

impl RefusalDetail {
    pub fn new(kind: RefusalKind, next: String) -> Self {
        Self { kind, next }
    }

    pub fn with_default_next(kind: RefusalKind, paths: RerunPaths<'_>) -> Self {
        let next = kind.default_next(paths);
        Self { kind, next }
    }
}

#[derive(Debug, Clone)]
pub enum RefusalKind {
    Io {
        file: FileSide,
        error: String,
    },
    Encoding {
        file: FileSide,
        issue: EncodingIssue,
    },
    CsvParse {
        file: FileSide,
        line: Option<u64>,
        column: Option<u64>,
    },
    Headers {
        file: FileSide,
        issue: HeadersIssue,
    },
    NoKey {
        key_column: Vec<u8>,
    },
    KeyEmpty {
        file: FileSide,
        record: u64,
        key_column: Vec<u8>,
    },
    KeyDup {
        file: FileSide,
        record: u64,
        key_value: Vec<u8>,
    },
    KeyMismatch {
        missing_in_new: usize,
        extra_in_new: usize,
        missing_samples: Vec<Vec<u8>>,
        extra_samples: Vec<Vec<u8>>,
    },
    RowCount {
        rows_old: u64,
        rows_new: u64,
        suggested_keys: Vec<Vec<u8>>,
    },
    NeedKey {
        suggested_keys: Vec<Vec<u8>>,
    },
    Dialect {
        file: FileSide,
        tied_delimiters: Vec<u8>,
        suggestion: DialectSuggestion,
    },
    MixedTypes {
        file: FileSide,
        record: u64,
        column: Vec<u8>,
        value: Vec<u8>,
        key_value: Option<Vec<u8>>,
    },
    NoNumeric,
    Missingness {
        file: FileSide,
        record: u64,
        column: Vec<u8>,
        value: Vec<u8>,
        key_value: Option<Vec<u8>>,
    },
    Diffuse {
        top_k_coverage: f64,
        threshold: f64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingIssue {
    Utf16,
    Utf32,
    NulByte,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeadersIssue {
    MissingHeader,
    Duplicate { name: Vec<u8> },
    ExtraFields { record: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialectSuggestion {
    ForceDelimiter(DelimiterHint),
    SepDirective(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimiterHint {
    Named(NamedDelimiter),
    Byte(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamedDelimiter {
    Comma,
    Tab,
    Semicolon,
    Pipe,
    Caret,
}

impl RefusalKind {
    pub fn default_next(&self, paths: RerunPaths<'_>) -> String {
        match self {
            RefusalKind::Io { .. } => "check file paths/permissions and rerun".to_string(),
            RefusalKind::Encoding { .. } => {
                "convert/re-export both files as UTF-8 CSV and rerun".to_string()
            }
            RefusalKind::CsvParse { .. } => {
                "re-export as standard CSV (RFC4180 quoting) and rerun".to_string()
            }
            RefusalKind::Headers { issue, .. } => match issue {
                HeadersIssue::MissingHeader => {
                    "ensure the file has a header row and rerun".to_string()
                }
                HeadersIssue::Duplicate { .. } => "make header names unique and rerun".to_string(),
                HeadersIssue::ExtraFields { .. } => {
                    "remove extra columns or re-export with consistent headers, then rerun"
                        .to_string()
                }
            },
            RefusalKind::NoKey { key_column } => {
                let key = encode_identifier_json(key_column);
                format!("rvl {} {} --key {}", paths.old, paths.new, key)
            }
            RefusalKind::KeyEmpty { .. } => {
                "choose a key column with no empty values (or fill missing keys), then rerun"
                    .to_string()
            }
            RefusalKind::KeyDup { .. } => {
                "choose a unique key column or dedupe the data, then rerun".to_string()
            }
            RefusalKind::KeyMismatch { .. } => {
                "export comparable scopes or fix the join key, then rerun".to_string()
            }
            RefusalKind::RowCount { suggested_keys, .. } => {
                if let Some(key) = suggested_keys.first() {
                    let key = encode_identifier_json(key);
                    format!(
                        "rvl {} {} --key {} to get a missing/extra-keys report (or export comparable scopes)",
                        paths.old, paths.new, key
                    )
                } else {
                    "export comparable scopes or rerun with --key <column>".to_string()
                }
            }
            RefusalKind::NeedKey { suggested_keys } => {
                if let Some(key) = suggested_keys.first() {
                    let key = encode_identifier_json(key);
                    format!("rvl {} {} --key {}", paths.old, paths.new, key)
                } else {
                    "rerun with --key <column>".to_string()
                }
            }
            RefusalKind::Dialect {
                file, suggestion, ..
            } => match suggestion {
                DialectSuggestion::ForceDelimiter(hint) => format!(
                    "rvl {} {} --delimiter {}",
                    paths.old,
                    paths.new,
                    render_delimiter_hint(*hint)
                ),
                DialectSuggestion::SepDirective(delim) => {
                    if let Some(sep) = render_sep_directive(*delim) {
                        format!(
                            "add `{}` as the first non-blank line of the {} file (no whitespace), then rerun",
                            sep,
                            file.as_str()
                        )
                    } else {
                        format!(
                            "rvl {} {} --delimiter {}",
                            paths.old,
                            paths.new,
                            render_delimiter_hint(DelimiterHint::Byte(*delim))
                        )
                    }
                }
            },
            RefusalKind::MixedTypes { .. } => {
                "normalize column values to numeric (or exclude the column) and rerun".to_string()
            }
            RefusalKind::NoNumeric => {
                "ensure common numeric columns exist (or adjust inputs) and rerun".to_string()
            }
            RefusalKind::Missingness { .. } => {
                "fill missing values or remove the column, then rerun".to_string()
            }
            RefusalKind::Diffuse { .. } => {
                format!("rvl {} {} --threshold 0.80", paths.old, paths.new)
            }
        }
    }
}

fn render_delimiter_hint(hint: DelimiterHint) -> String {
    match hint {
        DelimiterHint::Named(name) => match name {
            NamedDelimiter::Comma => "comma".to_string(),
            NamedDelimiter::Tab => "tab".to_string(),
            NamedDelimiter::Semicolon => "semicolon".to_string(),
            NamedDelimiter::Pipe => "pipe".to_string(),
            NamedDelimiter::Caret => "caret".to_string(),
        },
        DelimiterHint::Byte(byte) => format!("0x{:02X}", byte),
    }
}

fn render_sep_directive(delimiter: u8) -> Option<String> {
    if delimiter == b'"' || delimiter == b'\r' || delimiter == b'\n' {
        return None;
    }
    if (0x01..=0x7F).contains(&delimiter) && is_visible_ascii(delimiter) {
        Some(format!("sep={}", delimiter as char))
    } else {
        None
    }
}

fn is_visible_ascii(byte: u8) -> bool {
    (0x21..=0x7e).contains(&byte)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_step_for_key_uses_encoded_identifier() {
        let kind = RefusalKind::NoKey {
            key_column: b"id".to_vec(),
        };
        let detail = RefusalDetail::with_default_next(
            kind,
            RerunPaths {
                old: "old.csv",
                new: "new.csv",
            },
        );
        assert_eq!(detail.next, "rvl old.csv new.csv --key u8:id");
    }

    #[test]
    fn rowcount_next_step_prefers_suggested_key() {
        let kind = RefusalKind::RowCount {
            rows_old: 10,
            rows_new: 11,
            suggested_keys: vec![b"user_id".to_vec()],
        };
        let detail = RefusalDetail::with_default_next(
            kind,
            RerunPaths {
                old: "old.csv",
                new: "new.csv",
            },
        );
        assert!(detail.next.contains("--key u8:user_id"));
    }

    #[test]
    fn dialect_next_step_forces_delimiter() {
        let kind = RefusalKind::Dialect {
            file: FileSide::Old,
            tied_delimiters: vec![b',', b'\t'],
            suggestion: DialectSuggestion::ForceDelimiter(DelimiterHint::Named(
                NamedDelimiter::Tab,
            )),
        };
        let detail = RefusalDetail::with_default_next(
            kind,
            RerunPaths {
                old: "a.csv",
                new: "b.csv",
            },
        );
        assert_eq!(detail.next, "rvl a.csv b.csv --delimiter tab");
    }
}
