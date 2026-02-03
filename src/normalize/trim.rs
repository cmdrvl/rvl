/// ASCII-trim: strip ASCII spaces (0x20) and tabs (0x09) from both ends.
///
/// This is the only whitespace rule in rvl. No Unicode whitespace is trimmed.
#[inline]
pub fn ascii_trim(input: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = input.len();
    while start < end && is_ascii_blank(input[start]) {
        start += 1;
    }
    while end > start && is_ascii_blank(input[end - 1]) {
        end -= 1;
    }
    &input[start..end]
}

/// Returns true if the byte is an ASCII space or tab.
#[inline]
pub const fn is_ascii_blank(b: u8) -> bool {
    b == b' ' || b == b'\t'
}

/// Returns true if the slice is empty after ASCII-trimming.
#[inline]
pub fn is_ascii_blank_slice(input: &[u8]) -> bool {
    ascii_trim(input).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_spaces() {
        assert_eq!(ascii_trim(b"  hello  "), b"hello");
    }

    #[test]
    fn trim_tabs() {
        assert_eq!(ascii_trim(b"\thello\t"), b"hello");
    }

    #[test]
    fn trim_mixed() {
        assert_eq!(ascii_trim(b" \t hello \t "), b"hello");
    }

    #[test]
    fn trim_empty() {
        assert_eq!(ascii_trim(b""), b"");
    }

    #[test]
    fn trim_only_blanks() {
        assert_eq!(ascii_trim(b"  \t\t  "), b"");
    }

    #[test]
    fn trim_no_blanks() {
        assert_eq!(ascii_trim(b"hello"), b"hello");
    }

    #[test]
    fn trim_preserves_inner_whitespace() {
        assert_eq!(ascii_trim(b"  hello world  "), b"hello world");
    }

    #[test]
    fn trim_preserves_inner_tabs() {
        assert_eq!(ascii_trim(b"\thello\tworld\t"), b"hello\tworld");
    }

    #[test]
    fn no_unicode_trim() {
        // \xc2\xa0 is UTF-8 for non-breaking space — must NOT be trimmed
        let input = b"\xc2\xa0hello\xc2\xa0";
        assert_eq!(ascii_trim(input), input);
    }

    #[test]
    fn trim_preserves_other_control_chars() {
        // \x01 and \x7f should not be trimmed
        assert_eq!(ascii_trim(b"\x01hello\x7f"), b"\x01hello\x7f");
    }

    #[test]
    fn trim_cr_lf_not_stripped() {
        assert_eq!(ascii_trim(b"\r\nhello\r\n"), b"\r\nhello\r\n");
    }

    #[test]
    fn blank_slice_detection() {
        assert!(is_ascii_blank_slice(b""));
        assert!(is_ascii_blank_slice(b"   "));
        assert!(is_ascii_blank_slice(b"\t\t"));
        assert!(is_ascii_blank_slice(b" \t "));
        assert!(!is_ascii_blank_slice(b" x "));
    }
}
