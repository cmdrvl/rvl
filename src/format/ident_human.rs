//! Identifier rendering for human output (bd-1i6)
//!
//! Rules (docs/PLAN_RVL.md):
//! - If valid UTF-8 and contains no ASCII control bytes and does not start with
//!   "u8:" or "hex:", print as-is.
//! - If valid UTF-8 and contains no ASCII control bytes but starts with "u8:" or
//!   "hex:", prefix with "u8:".
//! - Otherwise, render as "hex:<lowercase-hex-bytes>".

use std::fmt::Write;

/// Render an identifier for human output.
pub fn render_identifier_human(bytes: &[u8]) -> String {
    if let Ok(utf8) = std::str::from_utf8(bytes) {
        if has_ascii_control(bytes) {
            return hex_encode_lower(bytes);
        }
        if utf8.starts_with("u8:") || utf8.starts_with("hex:") {
            let mut out = String::with_capacity(utf8.len() + 3);
            out.push_str("u8:");
            out.push_str(utf8);
            return out;
        }
        return utf8.to_string();
    }
    hex_encode_lower(bytes)
}

fn has_ascii_control(bytes: &[u8]) -> bool {
    bytes.iter().any(|b| (*b <= 0x1F_u8) || (*b == 0x7F_u8))
}

fn hex_encode_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(4 + bytes.len() * 2);
    out.push_str("hex:");
    for b in bytes {
        let _ = write!(out, "{:02x}", b);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_plain_utf8_as_is() {
        assert_eq!(render_identifier_human(b"hello"), "hello");
        assert_eq!(render_identifier_human("café".as_bytes()), "café");
    }

    #[test]
    fn prefixes_ambiguous_utf8() {
        assert_eq!(render_identifier_human(b"u8:col"), "u8:u8:col");
        assert_eq!(render_identifier_human(b"hex:dead"), "u8:hex:dead");
    }

    #[test]
    fn renders_ascii_control_as_hex() {
        assert_eq!(render_identifier_human(b"hi\x01"), "hex:686901");
        assert_eq!(render_identifier_human(b"\x7f"), "hex:7f");
    }

    #[test]
    fn renders_invalid_utf8_as_hex() {
        assert_eq!(render_identifier_human(&[0xff, 0xfe]), "hex:fffe");
    }
}
