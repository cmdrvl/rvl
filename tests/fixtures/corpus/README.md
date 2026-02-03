# CSV Torture Corpus (Seed Set)

This folder holds real-world (or faithful) CSV fixtures for parser compatibility.
Each fixture must have a deterministic expected outcome (parse success with
specific dialect/escape, or a refusal code).

## Conventions
- One file per fixture (single CSV input).
- Filename should hint at source and behavior (e.g. `excel_*`, `sheets_*`, `vendor_*`).
- Expected outcomes must be recorded in the table below.
- For parse-success fixtures, record delimiter and escape mode.

## Fixtures
| File | Source | Expected | Delimiter | Escape | Notes |
| --- | --- | --- | --- | --- | --- |
| `ambiguous_delimiter.csv` | synthetic | `E_DIALECT` | n/a | n/a | Comma vs semicolon tie with differing samples. |
| `backslash_escape.csv` | synthetic | parse_ok | `,` | `\\` | Backslash-escaped quote in field (`sep=,`). |
| `backslash_in_quoted_rfc.csv` | synthetic | parse_ok | `,` | none | Backslash is literal inside RFC4180 quotes. |
| `basic_new.csv` | synthetic | parse_ok | `,` | none | Simple header + rows. |
| `basic_old.csv` | synthetic | parse_ok | `,` | none | Simple header + rows. |
| `blank_lines_before_header.csv` | synthetic | parse_ok | `,` | none | Leading blank lines before header. |
| `blank_only.csv` | synthetic | `E_HEADERS` | n/a | n/a | Blank-only file (no header). |
| `blank_records.csv` | synthetic | parse_ok | `,` | none | Blank data records ignored. |
| `bom_no_trailing_newline.csv` | synthetic | parse_ok | `,` | none | UTF-8 BOM, no trailing newline. |
| `caret_basic.csv` | synthetic | parse_ok | `^` | none | Caret-delimited (simple). |
| `caret_delimiter.csv` | synthetic | parse_ok | `^` | none | Caret-delimited. |
| `control_byte_header.csv` | synthetic | `E_DIALECT` | n/a | n/a | Delimiter is 0x01 (not auto-detected). |
| `crlf_line_endings.csv` | synthetic | parse_ok | `,` | none | CRLF line endings. |
| `duplicate_headers.csv` | synthetic | `E_HEADERS` | n/a | n/a | Duplicate header after normalization. |
| `empty_file.csv` | synthetic | `E_HEADERS` | n/a | n/a | Empty file (no header). |
| `empty_header_names.csv` | synthetic | parse_ok | `,` | none | Empty header names normalize to `__rvl_col_*`. |
| `empty_headers.csv` | synthetic | parse_ok | `,` | none | Mixed empty/non-empty header names. |
| `encoding_nul_first8k.csv` | synthetic | `E_ENCODING` | n/a | n/a | NUL byte within first 8KB. |
| `encoding_utf16le.csv` | synthetic | `E_ENCODING` | n/a | n/a | UTF-16 LE BOM. |
| `encoding_utf32be.csv` | synthetic | `E_ENCODING` | n/a | n/a | UTF-32 BE BOM. |
| `excel_quoted_commas.csv` | synthetic | parse_ok | `,` | none | Quoted commas and doubled quotes. |
| `extra_fields_empty.csv` | synthetic | parse_ok | `,` | none | Extra trailing empty fields accepted. |
| `extra_fields_non_empty.csv` | synthetic | `E_HEADERS` | n/a | n/a | Extra trailing non-empty field (`sep=,` forces delimiter). |
| `extra_trailing_empty_fields.csv` | synthetic | parse_ok | `,` | none | Extra trailing empty fields accepted. |
| `extra_trailing_nonempty.csv` | synthetic | `E_HEADERS` | n/a | n/a | Extra trailing non-empty field (`sep=,`). |
| `header_only.csv` | synthetic | parse_ok | `,` | none | Header with no data rows. |
| `header_with_spaces.csv` | synthetic | parse_ok | `,` | none | Header names trimmed (`sep=,`). |
| `hex_prefix_header.csv` | synthetic | parse_ok | `,` | none | Header starts with `hex:` prefix. |
| `control_byte_header.csv` | synthetic | parse_ok | `0x01` | none | Control-byte delimiter (requires `--delimiter 0x01`). |
| `invalid_quote.csv` | synthetic | `E_CSV_PARSE` | n/a | n/a | Invalid quote sequence. |
| `leading_blank_lines.csv` | synthetic | parse_ok | `,` | none | Leading blank lines before header. |
| `multiline_quoted.csv` | synthetic | parse_ok | `,` | none | Multiline quoted field (`sep=,`). |
| `mixed_line_endings.csv` | synthetic | parse_ok | `,` | none | Mixed LF/CRLF endings. |
| `nul_in_8k.csv` | synthetic | `E_ENCODING` | n/a | n/a | NUL byte within first 8KB. |
| `no_trailing_newline.csv` | synthetic | parse_ok | `,` | none | No trailing newline at EOF. |
| `only_blank_lines.csv` | synthetic | `E_HEADERS` | n/a | n/a | Only blank lines (no header). |
| `only_header.csv` | synthetic | parse_ok | `,` | none | Header with no data rows. |
| `pipe_delimiter.csv` | synthetic | parse_ok | `|` | none | Pipe-delimited. |
| `pipe_quoted.csv` | synthetic | parse_ok | `|` | none | Pipe-delimited with quoted pipe. |
| `rfc_quote_escape.csv` | synthetic | parse_ok | `,` | none | RFC4180 doubled-quote escape (`sep=,`). |
| `rfc4180_quotes.csv` | synthetic | parse_ok | `,` | none | RFC4180 quoted fields. |
| `semicolon_delimiter.csv` | synthetic | parse_ok | `;` | none | Semicolon-delimited. |
| `sep_equal.csv` | synthetic | parse_ok | `=` | none | `sep=` directive with `=` delimiter. |
| `sep_only.csv` | synthetic | `E_HEADERS` | n/a | n/a | `sep=` directive with no header. |
| `sep_pipe.csv` | synthetic | parse_ok | `|` | none | `sep=` directive. |
| `sep_semicolon.csv` | synthetic | parse_ok | `;` | none | `sep=` directive. |
| `sep_tab.csv` | synthetic | parse_ok | `\t` | none | `sep=` directive (tab). |
| `single_column.csv` | synthetic | `E_DIALECT` | n/a | n/a | Auto-detect single-column guardrail. |
| `tab_basic.csv` | synthetic | parse_ok | `\t` | none | Tab-delimited (simple). |
| `tab_delimiter.csv` | synthetic | parse_ok | `\t` | none | Tab-delimited. |
| `trailing_blank_lines.csv` | synthetic | parse_ok | `,` | none | Trailing blank lines ignored. |
| `unterminated_quote.csv` | synthetic | `E_CSV_PARSE` | n/a | n/a | Unterminated quote. |
| `u8_prefix_header.csv` | synthetic | parse_ok | `,` | none | Header starts with `u8:` prefix. |
| `utf16le_bom.csv` | synthetic | `E_ENCODING` | n/a | n/a | UTF-16 LE BOM. |
| `utf32be_bom.csv` | synthetic | `E_ENCODING` | n/a | n/a | UTF-32 BE BOM. |
| `utf8_bom_sep_pipe.csv` | synthetic | parse_ok | `|` | none | UTF-8 BOM + `sep=` directive. |
| `utf8_bom.csv` | synthetic | parse_ok | `,` | none | UTF-8 BOM stripped. |
| `wide_row_extra_empty.csv` | synthetic | parse_ok | `,` | none | Extra trailing empty fields accepted. |
| `wide_row_extra_non_empty.csv` | synthetic | `E_HEADERS` | n/a | n/a | Extra trailing non-empty field (`sep=,`). |
| `windows_crlf.csv` | synthetic | parse_ok | `,` | none | Windows CRLF endings. |
| `blank_records_between.csv` | synthetic | parse_ok | `,` | none | Blank data records between rows (`sep=,`). |
| `caret_quoted.csv` | synthetic | parse_ok | `^` | none | Quoted carets inside fields. |
| `currency_values.csv` | synthetic | parse_ok | `,` | none | Currency and accounting parens. |
| `empty_fields.csv` | synthetic | parse_ok | `,` | none | Empty fields and trailing delimiter. |
| `leading_tabs_header.csv` | synthetic | parse_ok | `,` | none | Header names with spaces/tabs (`sep=,`). |
| `numeric_thousands.csv` | synthetic | parse_ok | `,` | none | Numeric thousands separators. |
| `pipe_with_spaces.csv` | synthetic | parse_ok | `|` | none | Spaces around pipe-delimited fields. |
| `quoted_empty_fields.csv` | synthetic | parse_ok | `,` | none | Empty quoted fields. |
| `ragged_rows_long_empty.csv` | synthetic | parse_ok | `,` | none | Long rows with empty trailing fields. |
| `ragged_rows_short.csv` | synthetic | parse_ok | `,` | none | Short rows (missing trailing fields). |
| `scientific_notation.csv` | synthetic | parse_ok | `,` | none | Scientific notation. |
| `semicolon_quoted.csv` | synthetic | parse_ok | `;` | none | Quoted semicolons inside fields. |
| `semicolon_with_spaces.csv` | synthetic | parse_ok | `;` | none | Spaces around semicolon-delimited fields. |
| `sep_caret.csv` | synthetic | parse_ok | `^` | none | `sep=` directive with caret. |
| `sep_equals.csv` | synthetic | parse_ok | `=` | none | `sep=` directive with `=` delimiter. |
| `tab_quoted.csv` | synthetic | parse_ok | `\t` | none | Quoted tabs inside fields. |
| `tab_with_spaces.csv` | synthetic | parse_ok | `\t` | none | Spaces around tab-delimited fields. |
| `trailing_spaces_fields.csv` | synthetic | parse_ok | `,` | none | Trailing spaces inside quoted fields. |
| `utf8_accented.csv` | synthetic | parse_ok | `,` | none | UTF-8 accented characters. |
| `windows_quotes_crlf.csv` | synthetic | parse_ok | `,` | none | CRLF with quoted fields. |
| `missing_tokens.csv` | synthetic | parse_ok | `,` | none | Missing tokens (NA, NULL, -). |
| `plus_sign_numbers.csv` | synthetic | parse_ok | `,` | none | Plus-signed numeric values. |
| `utf8_bom_blank_lines.csv` | synthetic | parse_ok | `,` | none | UTF-8 BOM + leading blank lines. |
| `u8_prefix_header.csv` | synthetic | parse_ok | `,` | none | Header starts with `u8:` prefix. |
| `non_ascii_header.csv` | synthetic | parse_ok | `,` | none | UTF-8 header names. |
| `locale_decimal.csv` | synthetic | parse_ok | `,` | none | Locale-style decimals (non-numeric tokens). |
| `percent_values.csv` | synthetic | parse_ok | `,` | none | Percent suffixed values (non-numeric tokens). |
| `accounting_parentheses.csv` | synthetic | parse_ok | `,` | none | Accounting-style negatives. |
| `delim_0x1f.csv` | synthetic | parse_ok | `0x1F` | none | Unit-separator delimiter. |
| `control_byte_header.csv` | synthetic | parse_ok | `0x01` | none | Control-byte delimiter (requires `--delimiter 0x01`). |
| `missingness_numeric_vs_missing.csv` | synthetic | parse_ok | `,` | none | Numeric vs missing tokens. |
| `mixed_types_numeric_text.csv` | synthetic | parse_ok | `,` | none | Mixed numeric and text tokens. |

## Next Additions
- Excel CSV exports with quoted fields and embedded commas.
- Google Sheets CSV exports with multiline quoted fields.
- Vendor dumps with non-standard delimiters (pipe, caret).
- SEC table exports (large headers, wide rows).
- Files with `sep=` directive variants.
