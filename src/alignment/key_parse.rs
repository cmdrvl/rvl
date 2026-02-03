//! Key column parsing and validation (bd-1xu).
//!
//! `--key` accepts:
//! - plain UTF-8 string (treated as u8:<...>)
//! - u8:<utf8-string>
//! - hex:<hex-bytes> (raw bytes; hex is case-insensitive)

/// Errors encountered while parsing a key identifier argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyParseError {
    Empty,
    InvalidHex,
    OddHexLen,
}

impl std::fmt::Display for KeyParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyParseError::Empty => write!(f, "key identifier is empty"),
            KeyParseError::InvalidHex => write!(f, "invalid hex key identifier"),
            KeyParseError::OddHexLen => write!(f, "hex key identifier must have even length"),
        }
    }
}

impl std::error::Error for KeyParseError {}

/// Parse a `--key` argument into raw normalized header bytes.
///
/// The returned bytes must be matched against normalized header names.
pub fn parse_key_identifier(raw: &str) -> Result<Vec<u8>, KeyParseError> {
    if raw.is_empty() {
        return Err(KeyParseError::Empty);
    }
    if let Some(rest) = raw.strip_prefix("u8:") {
        return Ok(rest.as_bytes().to_vec());
    }
    if let Some(hex) = raw.strip_prefix("hex:") {
        return decode_hex_bytes(hex);
    }
    Ok(raw.as_bytes().to_vec())
}

fn decode_hex_bytes(hex: &str) -> Result<Vec<u8>, KeyParseError> {
    if hex.is_empty() {
        return Err(KeyParseError::InvalidHex);
    }
    if !hex.len().is_multiple_of(2) {
        return Err(KeyParseError::OddHexLen);
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let high = from_hex_digit(bytes[i])?;
        let low = from_hex_digit(bytes[i + 1])?;
        out.push((high << 4) | low);
        i += 2;
    }
    Ok(out)
}

#[inline]
fn from_hex_digit(b: u8) -> Result<u8, KeyParseError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(KeyParseError::InvalidHex),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_as_utf8_bytes() {
        assert_eq!(parse_key_identifier("col"), Ok(b"col".to_vec()));
        assert_eq!(parse_key_identifier("u8:col"), Ok(b"col".to_vec()));
    }

    #[test]
    fn parses_hex_identifier() {
        assert_eq!(parse_key_identifier("hex:6162"), Ok(b"ab".to_vec()));
        assert_eq!(parse_key_identifier("hex:4142"), Ok(b"AB".to_vec()));
        assert_eq!(parse_key_identifier("hex:4a6b"), Ok(b"Jk".to_vec()));
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(parse_key_identifier(""), Err(KeyParseError::Empty));
        assert_eq!(parse_key_identifier("hex:"), Err(KeyParseError::InvalidHex));
    }

    #[test]
    fn rejects_invalid_hex() {
        assert_eq!(parse_key_identifier("hex:0"), Err(KeyParseError::OddHexLen));
        assert_eq!(
            parse_key_identifier("hex:zz"),
            Err(KeyParseError::InvalidHex)
        );
        assert_eq!(
            parse_key_identifier("hex:0x"),
            Err(KeyParseError::InvalidHex)
        );
    }
}
