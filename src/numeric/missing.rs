use crate::normalize::trim::ascii_trim;

/// Returns true if the input is a missing token after ASCII-trimming.
///
/// Missing tokens (case-insensitive for letters):
/// - empty string
/// - "-"
/// - "NA", "N/A", "NULL", "NAN", "NONE"
#[inline]
pub fn is_missing_token(input: &[u8]) -> bool {
    let trimmed = ascii_trim(input);
    if trimmed.is_empty() {
        return true;
    }
    if trimmed == b"-" {
        return true;
    }
    ascii_eq_ignore_case(trimmed, b"NA")
        || ascii_eq_ignore_case(trimmed, b"N/A")
        || ascii_eq_ignore_case(trimmed, b"NULL")
        || ascii_eq_ignore_case(trimmed, b"NAN")
        || ascii_eq_ignore_case(trimmed, b"NONE")
}

#[inline]
fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

#[cfg(test)]
mod tests {
    use super::is_missing_token;

    #[test]
    fn missing_empty_or_blanks() {
        assert!(is_missing_token(b""));
        assert!(is_missing_token(b"   "));
        assert!(is_missing_token(b"\t\t"));
        assert!(is_missing_token(b" \t "));
    }

    #[test]
    fn missing_dash() {
        assert!(is_missing_token(b"-"));
        assert!(is_missing_token(b"  -  "));
    }

    #[test]
    fn missing_case_insensitive_tokens() {
        assert!(is_missing_token(b"NA"));
        assert!(is_missing_token(b"na"));
        assert!(is_missing_token(b"N/A"));
        assert!(is_missing_token(b"n/a"));
        assert!(is_missing_token(b"NULL"));
        assert!(is_missing_token(b"Null"));
        assert!(is_missing_token(b"NAN"));
        assert!(is_missing_token(b"nan"));
        assert!(is_missing_token(b"NONE"));
        assert!(is_missing_token(b"none"));
    }

    #[test]
    fn missing_trim_applies() {
        assert!(is_missing_token(b"  n/a  "));
        assert!(is_missing_token(b"\tNaN\t"));
    }

    #[test]
    fn non_missing_tokens() {
        assert!(!is_missing_token(b"0"));
        assert!(!is_missing_token(b"NA_"));
        assert!(!is_missing_token(b"N/Ax"));
        assert!(!is_missing_token(b"--"));
        assert!(!is_missing_token(b"NULLS"));
    }

    #[test]
    fn non_ascii_trim_not_applied() {
        assert!(!is_missing_token(b"\r"));
        assert!(!is_missing_token(b"\r\n"));
        assert!(!is_missing_token(b"\r\nNA\r\n"));
    }
}
