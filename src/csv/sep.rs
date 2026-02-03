//! sep= directive handling (bd-2s6)
//!
//! The sep= directive is honored only when it appears as the first non-blank
//! line (ASCII spaces/tabs only) and matches `sep=<single ASCII byte>` with
//! no surrounding whitespace. A trailing CR is ignored for CRLF files.

use crate::normalize::trim::is_ascii_blank_slice;

/// Result of scanning the first non-blank line for a sep= directive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SepScan<'a> {
    /// A valid sep= directive was found.
    Directive { delimiter: u8, line_index: usize },
    /// The first non-blank line is not a valid sep= directive.
    FirstNonBlank { line_index: usize, line: &'a [u8] },
    /// No non-blank lines were found.
    NoLines,
}

/// Scan an iterator of lines (without `\n`, with optional trailing `\r`)
/// and detect a valid sep= directive on the first non-blank line.
pub fn scan_first_non_blank_line<'a, I>(lines: I) -> SepScan<'a>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    for (idx, line) in lines.into_iter().enumerate() {
        let trimmed = strip_trailing_cr(line);
        if is_ascii_blank_slice(trimmed) {
            continue;
        }
        if let Some(delimiter) = parse_sep_directive(trimmed) {
            return SepScan::Directive {
                delimiter,
                line_index: idx,
            };
        }
        return SepScan::FirstNonBlank {
            line_index: idx,
            line: trimmed,
        };
    }
    SepScan::NoLines
}

/// Parse a sep= directive line. Returns Some(delimiter) if valid.
#[inline]
pub fn parse_sep_directive(line: &[u8]) -> Option<u8> {
    if line.len() != 5 || &line[..4] != b"sep=" {
        return None;
    }
    let delimiter = line[4];
    if is_valid_delimiter(delimiter) {
        Some(delimiter)
    } else {
        None
    }
}

/// Strip a single trailing CR if present (for CRLF lines split on '\n').
#[inline]
pub fn strip_trailing_cr(line: &[u8]) -> &[u8] {
    if line.ends_with(b"\r") {
        &line[..line.len().saturating_sub(1)]
    } else {
        line
    }
}

/// Valid CSV delimiters are single ASCII bytes 0x01-0x7F excluding `"`, `\r`, `\n`.
#[inline]
pub const fn is_valid_delimiter(delimiter: u8) -> bool {
    delimiter >= 0x01
        && delimiter <= 0x7F
        && delimiter != b'"'
        && delimiter != b'\r'
        && delimiter != b'\n'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sep_valid() {
        assert_eq!(parse_sep_directive(b"sep=,"), Some(b','));
        assert_eq!(parse_sep_directive(b"sep=;"), Some(b';'));
        assert_eq!(parse_sep_directive(b"sep=\t"), Some(b'\t'));
        assert_eq!(parse_sep_directive(b"sep=="), Some(b'='));
    }

    #[test]
    fn parse_sep_invalid() {
        assert_eq!(parse_sep_directive(b"sep="), None);
        assert_eq!(parse_sep_directive(b"sep=, "), None);
        assert_eq!(parse_sep_directive(b" sep=,"), None);
        assert_eq!(parse_sep_directive(b"sep=\""), None);
        assert_eq!(parse_sep_directive(b"sep=\r"), None);
        assert_eq!(parse_sep_directive(b"sep=\n"), None);
        assert_eq!(parse_sep_directive(b"sep=\x80"), None);
        assert_eq!(parse_sep_directive(b"sep=\x00"), None);
    }

    #[test]
    fn strip_trailing_cr_only() {
        assert_eq!(strip_trailing_cr(b"sep=,\r"), b"sep=,");
        assert_eq!(strip_trailing_cr(b"sep=,"), b"sep=,");
        assert_eq!(strip_trailing_cr(b"\r"), b"");
    }

    #[test]
    fn scan_first_non_blank_with_directive() {
        let lines = vec![b"   ".as_slice(), b"\t\t".as_slice(), b"sep=|".as_slice()];
        assert_eq!(
            scan_first_non_blank_line(lines),
            SepScan::Directive {
                delimiter: b'|',
                line_index: 2
            }
        );
    }

    #[test]
    fn scan_first_non_blank_without_directive() {
        let lines = vec![b"   ".as_slice(), b"sep=\"".as_slice()];
        assert_eq!(
            scan_first_non_blank_line(lines),
            SepScan::FirstNonBlank {
                line_index: 1,
                line: b"sep=\""
            }
        );
    }

    #[test]
    fn scan_no_lines() {
        let lines: Vec<&[u8]> = Vec::new();
        assert_eq!(scan_first_non_blank_line(lines), SepScan::NoLines);
    }
}
