use std::io::Cursor;

use csv::ByteRecord;

use rvl::csv::blank::is_blank_record;
use rvl::csv::dialect::{DialectError, auto_detect};
use rvl::csv::input::guard_input_bytes;
use rvl::csv::parser::{EscapeMode, build_reader, detect_escape_mode};
use rvl::csv::records::normalize_record;
use rvl::csv::sep::{SepScan, scan_first_non_blank_line};
use rvl::normalize::headers::normalize_headers;
use rvl::refusal::codes::RefusalCode;

mod helpers;

#[derive(Debug, Clone, Copy)]
struct ParseOkSpec {
    name: &'static str,
    delimiter: u8,
    escape: EscapeMode,
    forced_delimiter: Option<u8>,
}

#[derive(Debug, Clone, Copy)]
struct RefusalSpec {
    name: &'static str,
    code: RefusalCode,
}

fn parse_fixture(
    bytes: &[u8],
    forced_delimiter: Option<u8>,
) -> Result<(u8, EscapeMode), RefusalCode> {
    let guarded = guard_input_bytes(bytes).map_err(|_| RefusalCode::Encoding)?;

    let mut skip_sep = false;
    let mut sep_delimiter = None;
    if forced_delimiter.is_none() {
        match scan_first_non_blank_line(guarded.split(|byte| *byte == b'\n')) {
            SepScan::Directive { delimiter, .. } => {
                sep_delimiter = Some(delimiter);
                skip_sep = true;
            }
            SepScan::FirstNonBlank { .. } | SepScan::NoLines => {}
        }
    }

    let (delimiter, escape) = if let Some(forced) = forced_delimiter {
        let mut cursor = Cursor::new(guarded);
        let escape = detect_escape_mode(&mut cursor, forced).map_err(|_| RefusalCode::CsvParse)?;
        (forced, escape)
    } else if let Some(sep) = sep_delimiter {
        let mut cursor = Cursor::new(guarded);
        let escape = detect_escape_mode(&mut cursor, sep).map_err(|_| RefusalCode::CsvParse)?;
        (sep, escape)
    } else {
        match auto_detect(guarded) {
            Ok(dialect) => (dialect.delimiter, dialect.escape),
            Err(err) => {
                return Err(match err {
                    DialectError::NoHeader => RefusalCode::Headers,
                    DialectError::CsvParse { .. } => RefusalCode::CsvParse,
                    DialectError::Ambiguous { .. } | DialectError::SingleColumn { .. } => {
                        RefusalCode::Dialect
                    }
                });
            }
        }
    };

    let mut reader = build_reader(Cursor::new(guarded), delimiter, escape);
    let mut record = ByteRecord::new();
    let mut header: Option<Vec<Vec<u8>>> = None;
    let mut skipped_sep = !skip_sep;
    let mut data_index: u64 = 0;

    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {
                if header.is_none() {
                    if is_blank_record(&record) && record.len() == 1 {
                        continue;
                    }
                    if !skipped_sep {
                        skipped_sep = true;
                        continue;
                    }
                    let normalized =
                        normalize_headers(record.iter()).map_err(|_| RefusalCode::Headers)?;
                    header = Some(normalized);
                    continue;
                }

                if is_blank_record(&record) {
                    continue;
                }

                data_index += 1;
                let header_len = header.as_ref().map(|h| h.len()).unwrap_or(0);
                normalize_record(&record, header_len, data_index)
                    .map_err(|_| RefusalCode::Headers)?;
            }
            Ok(false) => break,
            Err(_) => return Err(RefusalCode::CsvParse),
        }
    }

    if header.is_none() {
        return Err(RefusalCode::Headers);
    }

    Ok((delimiter, escape))
}

#[test]
fn corpus_parse_ok_fixtures() {
    let fixtures = [
        ParseOkSpec {
            name: "corpus/accounting_parentheses.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/backslash_escape.csv",
            delimiter: b',',
            escape: EscapeMode::Backslash,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/backslash_in_quoted_rfc.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/basic_new.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/basic_old.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/blank_lines_before_header.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/blank_records.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/blank_records_between.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/bom_no_trailing_newline.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/caret_basic.csv",
            delimiter: b'^',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/caret_delimiter.csv",
            delimiter: b'^',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/caret_quoted.csv",
            delimiter: b'^',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/crlf_line_endings.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/currency_values.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/empty_fields.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/empty_header_names.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/empty_headers.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/excel_quoted_commas.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/extra_fields_empty.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/extra_trailing_empty_fields.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/header_only.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/header_with_spaces.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/leading_blank_lines.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/leading_tabs_header.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/locale_decimal.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/missing_tokens.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/missingness_numeric_vs_missing.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/mixed_line_endings.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/mixed_types_numeric_text.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/multiline_quoted.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/no_trailing_newline.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/non_ascii_header.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/numeric_thousands.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/only_header.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/percent_values.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/pipe_delimiter.csv",
            delimiter: b'|',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/pipe_quoted.csv",
            delimiter: b'|',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/pipe_with_spaces.csv",
            delimiter: b'|',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/plus_sign_numbers.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/quoted_empty_fields.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/ragged_rows_long_empty.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/ragged_rows_short.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/rfc_quote_escape.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/rfc4180_quotes.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/scientific_notation.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/semicolon_delimiter.csv",
            delimiter: b';',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/semicolon_quoted.csv",
            delimiter: b';',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/semicolon_with_spaces.csv",
            delimiter: b';',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/sep_caret.csv",
            delimiter: b'^',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/sep_equal.csv",
            delimiter: b'=',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/sep_equals.csv",
            delimiter: b'=',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/sep_pipe.csv",
            delimiter: b'|',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/sep_semicolon.csv",
            delimiter: b';',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/sep_tab.csv",
            delimiter: b'\t',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/tab_basic.csv",
            delimiter: b'\t',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/tab_delimiter.csv",
            delimiter: b'\t',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/tab_quoted.csv",
            delimiter: b'\t',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/tab_with_spaces.csv",
            delimiter: b'\t',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/trailing_blank_lines.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/trailing_spaces_fields.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/utf8_accented.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/utf8_bom_blank_lines.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/utf8_bom_sep_pipe.csv",
            delimiter: b'|',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/utf8_bom.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/wide_row_extra_empty.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/windows_crlf.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/windows_quotes_crlf.csv",
            delimiter: b',',
            escape: EscapeMode::None,
            forced_delimiter: None,
        },
        ParseOkSpec {
            name: "corpus/delim_0x1f.csv",
            delimiter: 0x1f,
            escape: EscapeMode::None,
            forced_delimiter: Some(0x1f),
        },
    ];

    for fixture in fixtures {
        let bytes = helpers::read_fixture(fixture.name);
        let result = parse_fixture(&bytes, fixture.forced_delimiter);
        match result {
            Ok((delimiter, escape)) => {
                assert_eq!(delimiter, fixture.delimiter, "{} delimiter", fixture.name);
                assert_eq!(escape, fixture.escape, "{} escape", fixture.name);
            }
            Err(code) => panic!("{} expected parse_ok, got {:?}", fixture.name, code),
        }
    }
}

#[test]
fn corpus_refusal_fixtures() {
    let fixtures = [
        RefusalSpec {
            name: "corpus/ambiguous_delimiter.csv",
            code: RefusalCode::Dialect,
        },
        RefusalSpec {
            name: "corpus/blank_only.csv",
            code: RefusalCode::Headers,
        },
        RefusalSpec {
            name: "corpus/duplicate_headers.csv",
            code: RefusalCode::Headers,
        },
        RefusalSpec {
            name: "corpus/empty_file.csv",
            code: RefusalCode::Headers,
        },
        RefusalSpec {
            name: "corpus/encoding_nul_first8k.csv",
            code: RefusalCode::Encoding,
        },
        RefusalSpec {
            name: "corpus/encoding_utf16le.csv",
            code: RefusalCode::Encoding,
        },
        RefusalSpec {
            name: "corpus/encoding_utf32be.csv",
            code: RefusalCode::Encoding,
        },
        RefusalSpec {
            name: "corpus/extra_fields_non_empty.csv",
            code: RefusalCode::Headers,
        },
        RefusalSpec {
            name: "corpus/extra_trailing_nonempty.csv",
            code: RefusalCode::Headers,
        },
        RefusalSpec {
            name: "corpus/invalid_quote.csv",
            code: RefusalCode::CsvParse,
        },
        RefusalSpec {
            name: "corpus/nul_in_8k.csv",
            code: RefusalCode::Encoding,
        },
        RefusalSpec {
            name: "corpus/only_blank_lines.csv",
            code: RefusalCode::Headers,
        },
        RefusalSpec {
            name: "corpus/sep_only.csv",
            code: RefusalCode::Headers,
        },
        RefusalSpec {
            name: "corpus/single_column.csv",
            code: RefusalCode::Dialect,
        },
        RefusalSpec {
            name: "corpus/unterminated_quote.csv",
            code: RefusalCode::CsvParse,
        },
        RefusalSpec {
            name: "corpus/utf16le_bom.csv",
            code: RefusalCode::Encoding,
        },
        RefusalSpec {
            name: "corpus/utf32be_bom.csv",
            code: RefusalCode::Encoding,
        },
        RefusalSpec {
            name: "corpus/wide_row_extra_non_empty.csv",
            code: RefusalCode::Headers,
        },
    ];

    for fixture in fixtures {
        let bytes = helpers::read_fixture(fixture.name);
        let result = parse_fixture(&bytes, None);
        match result {
            Ok(_) => panic!(
                "{} expected refusal {:?}, got parse_ok",
                fixture.name, fixture.code
            ),
            Err(code) => assert_eq!(code, fixture.code, "{} refusal", fixture.name),
        }
    }
}

#[test]
fn corpus_sep_directive_detects() {
    let fixtures = [
        "corpus/backslash_escape.csv",
        "corpus/extra_fields_non_empty.csv",
        "corpus/sep_pipe.csv",
        "corpus/sep_tab.csv",
    ];

    for name in fixtures {
        let bytes = helpers::read_fixture(name);
        let guarded = guard_input_bytes(&bytes).expect("fixture should be utf-8 safe");
        let scan = scan_first_non_blank_line(guarded.split(|byte| *byte == b'\n'));
        assert!(
            matches!(scan, SepScan::Directive { .. }),
            "{name} expected sep directive, got {scan:?}"
        );
    }
}
