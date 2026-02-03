//! Input bytes, BOM stripping, and encoding guardrails (bd-22j).

/// Maximum number of bytes to scan for NUL (0x00).
pub const NUL_SCAN_LIMIT: usize = 8 * 1024;

/// UTF-8 BOM bytes.
pub const UTF8_BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];
/// UTF-16 BE BOM bytes.
pub const UTF16_BE_BOM: [u8; 2] = [0xFE, 0xFF];
/// UTF-16 LE BOM bytes.
pub const UTF16_LE_BOM: [u8; 2] = [0xFF, 0xFE];
/// UTF-32 BE BOM bytes.
pub const UTF32_BE_BOM: [u8; 4] = [0x00, 0x00, 0xFE, 0xFF];
/// UTF-32 LE BOM bytes.
pub const UTF32_LE_BOM: [u8; 4] = [0xFF, 0xFE, 0x00, 0x00];

/// Encoding guardrail failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingIssue {
    /// UTF-16/UTF-32 BOM detected.
    Utf16Or32Bom,
    /// NUL byte detected within the first 8KB.
    NulByte,
}

/// Strip a UTF-8 BOM if present. Returns the stripped slice and a flag.
#[inline]
pub fn strip_utf8_bom(input: &[u8]) -> (&[u8], bool) {
    if input.starts_with(&UTF8_BOM) {
        (&input[UTF8_BOM.len()..], true)
    } else {
        (input, false)
    }
}

/// Returns true if the input begins with a UTF-16 or UTF-32 BOM.
#[inline]
pub fn has_utf16_or_utf32_bom(input: &[u8]) -> bool {
    matches!(
        input,
        [0x00, 0x00, 0xFE, 0xFF, ..]
            | [0xFF, 0xFE, 0x00, 0x00, ..]
            | [0xFE, 0xFF, ..]
            | [0xFF, 0xFE, ..]
    )
}

/// Returns true if a NUL byte (0x00) is found within the first 8KB.
#[inline]
pub fn has_nul_in_first_8k(input: &[u8]) -> bool {
    input.iter().take(NUL_SCAN_LIMIT).any(|byte| *byte == 0)
}

/// Apply encoding guardrails and strip UTF-8 BOM if present.
///
/// Order:
/// 1) UTF-16/UTF-32 BOM ⇒ refuse (E_ENCODING)
/// 2) UTF-8 BOM ⇒ strip and continue
/// 3) NUL byte in first 8KB ⇒ refuse (E_ENCODING)
#[inline]
pub fn guard_input_bytes(input: &[u8]) -> Result<&[u8], EncodingIssue> {
    if has_utf16_or_utf32_bom(input) {
        return Err(EncodingIssue::Utf16Or32Bom);
    }
    let (stripped, _had_utf8_bom) = strip_utf8_bom(input);
    if has_nul_in_first_8k(stripped) {
        return Err(EncodingIssue::NulByte);
    }
    Ok(stripped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_bom_is_stripped() {
        let input = [UTF8_BOM.as_slice(), b"abc"].concat();
        let (stripped, had_bom) = strip_utf8_bom(&input);
        assert!(had_bom);
        assert_eq!(stripped, b"abc");
        assert_eq!(guard_input_bytes(&input), Ok(b"abc".as_slice()));
    }

    #[test]
    fn utf16_bom_refused() {
        let input = [UTF16_LE_BOM.as_slice(), b"a\0"].concat();
        assert_eq!(guard_input_bytes(&input), Err(EncodingIssue::Utf16Or32Bom));
    }

    #[test]
    fn utf32_bom_refused() {
        let input = [UTF32_BE_BOM.as_slice(), b"abc"].concat();
        assert_eq!(guard_input_bytes(&input), Err(EncodingIssue::Utf16Or32Bom));
    }

    #[test]
    fn nul_in_first_8k_refused() {
        let input = b"ab\0cd";
        assert_eq!(guard_input_bytes(input), Err(EncodingIssue::NulByte));
    }

    #[test]
    fn nul_after_8k_allowed() {
        let mut input = vec![b'a'; NUL_SCAN_LIMIT + 1];
        input[NUL_SCAN_LIMIT] = 0;
        assert_eq!(guard_input_bytes(&input), Ok(input.as_slice()));
    }
}
