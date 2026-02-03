//! Delimiter flag parsing & validation (bd-392).

/// Error returned when a delimiter flag cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DelimiterError {
    Empty,
    InvalidHex,
    InvalidValue,
    NonAscii,
    InvalidByte(u8),
}

impl std::fmt::Display for DelimiterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DelimiterError::Empty => write!(f, "delimiter is empty"),
            DelimiterError::InvalidHex => write!(f, "invalid hex delimiter; expected 0xNN"),
            DelimiterError::InvalidValue => write!(f, "invalid delimiter value"),
            DelimiterError::NonAscii => write!(f, "delimiter must be a single ASCII byte"),
            DelimiterError::InvalidByte(byte) => {
                write!(f, "invalid delimiter byte 0x{byte:02X}")
            }
        }
    }
}

impl std::error::Error for DelimiterError {}

/// Parse a delimiter flag into a single ASCII byte.
///
/// Accepted inputs (case-insensitive for keywords/hex):
/// - named: comma, tab, semicolon, pipe, caret
/// - hex: 0xNN (two-digit ASCII byte)
/// - single ASCII byte literal (length 1 string)
pub fn parse_delimiter_arg(raw: &str) -> Result<u8, DelimiterError> {
    if raw.is_empty() {
        return Err(DelimiterError::Empty);
    }

    let lower = raw.to_ascii_lowercase();
    match lower.as_str() {
        "comma" => return Ok(b','),
        "tab" => return Ok(b'\t'),
        "semicolon" => return Ok(b';'),
        "pipe" => return Ok(b'|'),
        "caret" => return Ok(b'^'),
        _ => {}
    }

    if let Some(hex) = lower.strip_prefix("0x") {
        if hex.len() != 2 {
            return Err(DelimiterError::InvalidHex);
        }
        let byte = u8::from_str_radix(hex, 16).map_err(|_| DelimiterError::InvalidHex)?;
        return validate_delimiter_byte(byte);
    }

    let mut chars = raw.chars();
    let first = chars.next();
    let second = chars.next();
    if first.is_none() || second.is_some() {
        return Err(DelimiterError::InvalidValue);
    }
    let ch = first.unwrap();
    if !ch.is_ascii() {
        return Err(DelimiterError::NonAscii);
    }
    validate_delimiter_byte(ch as u8)
}

#[inline]
fn validate_delimiter_byte(byte: u8) -> Result<u8, DelimiterError> {
    if !(1..=0x7F).contains(&byte) {
        return Err(DelimiterError::InvalidByte(byte));
    }
    if byte == b'"' || byte == b'\r' || byte == b'\n' {
        return Err(DelimiterError::InvalidByte(byte));
    }
    Ok(byte)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_named_delimiters() {
        assert_eq!(parse_delimiter_arg("comma"), Ok(b','));
        assert_eq!(parse_delimiter_arg("tab"), Ok(b'\t'));
        assert_eq!(parse_delimiter_arg("semicolon"), Ok(b';'));
        assert_eq!(parse_delimiter_arg("pipe"), Ok(b'|'));
        assert_eq!(parse_delimiter_arg("caret"), Ok(b'^'));
    }

    #[test]
    fn parses_named_case_insensitive() {
        assert_eq!(parse_delimiter_arg("COMMA"), Ok(b','));
        assert_eq!(parse_delimiter_arg("TaB"), Ok(b'\t'));
    }

    #[test]
    fn parses_hex_form() {
        assert_eq!(parse_delimiter_arg("0x2c"), Ok(b','));
        assert_eq!(parse_delimiter_arg("0X09"), Ok(b'\t'));
    }

    #[test]
    fn parses_single_ascii_char() {
        assert_eq!(parse_delimiter_arg(","), Ok(b','));
        assert_eq!(parse_delimiter_arg("|"), Ok(b'|'));
    }

    #[test]
    fn rejects_invalid_hex() {
        assert_eq!(parse_delimiter_arg("0x2"), Err(DelimiterError::InvalidHex));
        assert_eq!(parse_delimiter_arg("0x2g"), Err(DelimiterError::InvalidHex));
    }

    #[test]
    fn rejects_invalid_bytes() {
        assert_eq!(
            parse_delimiter_arg("\""),
            Err(DelimiterError::InvalidByte(b'"'))
        );
        assert_eq!(
            parse_delimiter_arg("\n"),
            Err(DelimiterError::InvalidByte(b'\n'))
        );
        assert_eq!(
            parse_delimiter_arg("0x00"),
            Err(DelimiterError::InvalidByte(0x00))
        );
        assert_eq!(
            parse_delimiter_arg("0x80"),
            Err(DelimiterError::InvalidByte(0x80))
        );
        assert_eq!(
            parse_delimiter_arg("0x0A"),
            Err(DelimiterError::InvalidByte(0x0A))
        );
    }

    #[test]
    fn rejects_non_ascii_single_char() {
        assert_eq!(parse_delimiter_arg("§"), Err(DelimiterError::NonAscii));
    }

    #[test]
    fn rejects_other_values() {
        assert_eq!(parse_delimiter_arg(""), Err(DelimiterError::Empty));
        assert_eq!(parse_delimiter_arg("::"), Err(DelimiterError::InvalidValue));
    }
}
