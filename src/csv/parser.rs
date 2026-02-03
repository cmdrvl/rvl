//! CSV parsing mode: RFC4180 + backslash fallback (bd-5ez).
//!
//! Prefer RFC4180 quoting; if parsing hard-fails, retry with backslash escape.

use std::io::{Read, Seek, SeekFrom};

use csv::{ByteRecord, Reader};

/// CSV escape mode for parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EscapeMode {
    /// RFC4180 quoting (`""` for quote escapes).
    None,
    /// Backslash escape inside quoted fields.
    Backslash,
}

impl EscapeMode {
    pub fn escape_byte(self) -> Option<u8> {
        match self {
            EscapeMode::None => None,
            EscapeMode::Backslash => Some(b'\\'),
        }
    }

    /// Render the escape mode for human output.
    pub fn display_str(self) -> &'static str {
        match self {
            EscapeMode::None => "none",
            EscapeMode::Backslash => r"\\",
        }
    }
}

/// Error returned when both RFC4180 and backslash parsing fail.
#[derive(Debug)]
pub struct CsvParseError {
    pub escape_mode: EscapeMode,
    pub record: Option<u64>,
    pub line: Option<u64>,
    pub source: csv::Error,
}

impl CsvParseError {
    pub(crate) fn new(source: csv::Error, escape_mode: EscapeMode) -> Self {
        let (record, line) = source
            .position()
            .map(|pos| (Some(pos.record()), Some(pos.line())))
            .unwrap_or((None, None));
        Self {
            escape_mode,
            record,
            line,
            source,
        }
    }
}

/// Build a CSV reader with the requested delimiter and escape mode.
pub fn build_reader<R: Read>(reader: R, delimiter: u8, escape: EscapeMode) -> Reader<R> {
    csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .flexible(true)
        .has_headers(false)
        .escape(escape.escape_byte())
        .from_reader(reader)
}

/// Detect which escape mode parses the input without hard errors.
pub fn detect_escape_mode<R: Read + Seek>(
    reader: &mut R,
    delimiter: u8,
) -> Result<EscapeMode, CsvParseError> {
    let mut buffer = Vec::new();
    if let Err(err) = reader
        .seek(SeekFrom::Start(0))
        .and_then(|_| reader.read_to_end(&mut buffer))
    {
        let _ = reader.seek(SeekFrom::Start(0));
        return Err(CsvParseError::new(csv::Error::from(err), EscapeMode::None));
    }

    let result = match try_parse(&buffer, delimiter, EscapeMode::None) {
        Ok(()) => Ok(EscapeMode::None),
        Err(first_error) => match try_parse(&buffer, delimiter, EscapeMode::Backslash) {
            Ok(()) => Ok(EscapeMode::Backslash),
            Err(_second_error) => Err(first_error),
        },
    };

    let _ = reader.seek(SeekFrom::Start(0));
    result
}

fn try_parse(input: &[u8], delimiter: u8, escape: EscapeMode) -> Result<(), CsvParseError> {
    if let Err(err) = validate_quotes(input, delimiter, escape) {
        return Err(CsvParseError::new(err, escape));
    }
    let mut csv = build_reader(std::io::Cursor::new(input), delimiter, escape);
    let mut record = ByteRecord::new();
    loop {
        match csv.read_byte_record(&mut record) {
            Ok(true) => continue,
            Ok(false) => break,
            Err(err) => return Err(CsvParseError::new(err, escape)),
        }
    }
    Ok(())
}

pub(crate) fn validate_quotes(
    input: &[u8],
    delimiter: u8,
    escape: EscapeMode,
) -> Result<(), csv::Error> {
    let mut in_quotes = false;
    let mut i = 0;
    while i < input.len() {
        let b = input[i];
        if in_quotes {
            if escape == EscapeMode::Backslash
                && b == b'\\'
                && i + 1 < input.len()
                && input[i + 1] == b'"'
            {
                i += 2;
                continue;
            }
            if b == b'"' {
                if i + 1 < input.len() && input[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_quotes = false;
                if i + 1 < input.len() {
                    let next = input[i + 1];
                    if next != delimiter && next != b'\n' && next != b'\r' {
                        return Err(csv::Error::from(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid quote",
                        )));
                    }
                }
            }
        } else if b == b'"' {
            in_quotes = true;
        }
        i += 1;
    }

    if in_quotes {
        return Err(csv::Error::from(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unterminated quote",
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn detects_rfc4180() {
        let data = b"col\n\"a\"\"b\"";
        let mut cursor = Cursor::new(&data[..]);
        let mode = detect_escape_mode(&mut cursor, b',').expect("should parse");
        assert_eq!(mode, EscapeMode::None);
    }

    #[test]
    fn detects_backslash_escape() {
        let data = b"col\n\"a\\\"b\"";
        let mut cursor = Cursor::new(&data[..]);
        let mode = detect_escape_mode(&mut cursor, b',').expect("should parse");
        assert_eq!(mode, EscapeMode::Backslash);
    }

    #[test]
    fn errors_when_both_fail() {
        let data = b"col\n\"unterminated";
        let mut cursor = Cursor::new(&data[..]);
        let err = detect_escape_mode(&mut cursor, b',').expect_err("should fail");
        assert_eq!(err.escape_mode, EscapeMode::None);
    }
}
