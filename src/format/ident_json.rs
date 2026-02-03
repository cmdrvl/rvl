// Identifier encoding for JSON output (bd-2na)

/// Encode an identifier for JSON output.
///
/// Rules (v0):
/// - If the bytes are valid UTF-8 and contain no ASCII control bytes, return `u8:<utf8>`.
/// - Otherwise, return `hex:<lowercase-hex-bytes>`.
pub fn encode_identifier_json(bytes: &[u8]) -> String {
    if let Ok(utf8) = std::str::from_utf8(bytes)
        && !contains_ascii_control(bytes)
    {
        let mut out = String::with_capacity(3 + utf8.len());
        out.push_str("u8:");
        out.push_str(utf8);
        return out;
    }
    hex_encode(bytes)
}

#[inline]
fn contains_ascii_control(bytes: &[u8]) -> bool {
    bytes.iter().any(|&b| b <= 0x1F || b == 0x7F)
}

fn hex_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(4 + bytes.len() * 2);
    out.push_str("hex:");
    for &b in bytes {
        out.push(TABLE[(b >> 4) as usize] as char);
        out.push(TABLE[(b & 0x0F) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_valid_utf8_without_controls_as_u8() {
        assert_eq!(encode_identifier_json(b"abc"), "u8:abc");
        let cent = std::str::from_utf8(b"\xc2\xa2").expect("valid utf-8");
        assert_eq!(
            encode_identifier_json(cent.as_bytes()),
            format!("u8:{}", cent)
        );
    }

    #[test]
    fn encodes_utf8_with_control_bytes_as_hex() {
        assert_eq!(encode_identifier_json(b"\x00abc"), "hex:00616263");
        assert_eq!(encode_identifier_json(b"ab\x7f"), "hex:61627f");
    }

    #[test]
    fn encodes_invalid_utf8_as_hex() {
        assert_eq!(encode_identifier_json(b"\xff\xfe"), "hex:fffe");
    }

    #[test]
    fn always_prefixes_u8_in_json() {
        assert_eq!(encode_identifier_json(b"u8:foo"), "u8:u8:foo");
        assert_eq!(encode_identifier_json(b"hex:deadbeef"), "u8:hex:deadbeef");
    }
}
