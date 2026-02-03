//! Numeric parsing for allowed formats (bd-43p).
//!
//! Supported (per PLAN_RVL.md):
//! - Plain numbers with optional sign and exponent.
//! - US thousands separators (commas in 3-digit groups).
//! - Currency prefix `$` (with sign before or after `$`).
//! - Accounting parentheses to force negative (e.g., `(123.45)` or `($1,234.56)`).

use crate::normalize::trim::ascii_trim;

/// Parse a numeric token according to v0 rules.
///
/// Returns `Some(f64)` if the value is valid and finite; otherwise `None`.
pub fn parse_numeric(input: &[u8]) -> Option<f64> {
    let trimmed = ascii_trim(input);
    if trimmed.is_empty() {
        return None;
    }

    let mut token = trimmed;
    let mut force_negative = false;
    if token.len() >= 2 && token[0] == b'(' && token[token.len() - 1] == b')' {
        force_negative = true;
        token = &token[1..token.len() - 1];
    }

    if token.is_empty() {
        return None;
    }

    let (sign, rest) = parse_prefix(token)?;
    let mut value = parse_number_core(rest)?;
    value *= sign;
    if force_negative {
        value = -value.abs();
    }
    Some(value)
}

fn parse_prefix(token: &[u8]) -> Option<(f64, &[u8])> {
    let mut sign = 1.0;
    let mut seen_sign = false;
    let mut seen_dollar = false;
    let mut idx = 0;

    while idx < token.len() {
        match token[idx] {
            b'+' | b'-' if !seen_sign => {
                sign = if token[idx] == b'-' { -1.0 } else { 1.0 };
                seen_sign = true;
                idx += 1;
            }
            b'$' if !seen_dollar => {
                seen_dollar = true;
                idx += 1;
            }
            _ => break,
        }
    }

    let rest = &token[idx..];
    if rest.is_empty() {
        return None;
    }
    if matches!(rest[0], b'+' | b'-') {
        return None;
    }
    if rest.contains(&b'$') {
        return None;
    }
    Some((sign, rest))
}

fn parse_number_core(token: &[u8]) -> Option<f64> {
    if token.is_empty() {
        return None;
    }

    let mut exp_index = None;
    for (idx, b) in token.iter().enumerate() {
        if *b == b'e' || *b == b'E' {
            exp_index = Some(idx);
            break;
        }
    }

    let (mantissa, exponent) = match exp_index {
        Some(idx) => (&token[..idx], Some(&token[idx..])),
        None => (token, None),
    };

    if mantissa.is_empty() {
        return None;
    }

    if let Some(exp) = exponent {
        if exp.len() < 2 {
            return None;
        }
        if exp.contains(&b',') {
            return None;
        }
    }

    if !validate_commas(mantissa) {
        return None;
    }

    let mut normalized = Vec::with_capacity(token.len());
    for b in mantissa {
        if *b != b',' {
            normalized.push(*b);
        }
    }
    if let Some(exp) = exponent {
        normalized.extend_from_slice(exp);
    }

    let parsed = std::str::from_utf8(&normalized).ok()?.parse::<f64>().ok()?;
    if !parsed.is_finite() {
        return None;
    }
    Some(parsed)
}

fn validate_commas(mantissa: &[u8]) -> bool {
    let mut dot_index = None;
    for (idx, b) in mantissa.iter().enumerate() {
        if *b == b'.' {
            if dot_index.is_some() {
                return false;
            }
            dot_index = Some(idx);
        }
    }

    let (int_part, frac_part) = match dot_index {
        Some(idx) => (&mantissa[..idx], &mantissa[idx + 1..]),
        None => (mantissa, &mantissa[0..0]),
    };

    if frac_part.contains(&b',') {
        return false;
    }

    if !int_part.contains(&b',') {
        return has_digit(int_part) || has_digit(frac_part);
    }

    let mut groups = int_part.split(|b| *b == b',');
    let first = match groups.next() {
        Some(group) => group,
        None => return false,
    };
    if first.is_empty() || first.len() > 3 || !all_digits(first) {
        return false;
    }

    for group in groups {
        if group.len() != 3 || !all_digits(group) {
            return false;
        }
    }

    true
}

#[inline]
fn all_digits(slice: &[u8]) -> bool {
    slice.iter().all(|b| b.is_ascii_digit())
}

#[inline]
fn has_digit(slice: &[u8]) -> bool {
    slice.iter().any(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_numbers() {
        assert_eq!(parse_numeric(b"123"), Some(123.0));
        assert_eq!(parse_numeric(b"-123"), Some(-123.0));
        assert_eq!(parse_numeric(b"+123"), Some(123.0));
        assert_eq!(parse_numeric(b"123.45"), Some(123.45));
        assert_eq!(parse_numeric(b"-123.45"), Some(-123.45));
        assert_eq!(parse_numeric(b"1e6"), Some(1e6));
        assert_eq!(parse_numeric(b"-1.2E-3"), Some(-1.2e-3));
    }

    #[test]
    fn parses_thousands_separators() {
        assert_eq!(parse_numeric(b"1,234"), Some(1234.0));
        assert_eq!(parse_numeric(b"-1,234"), Some(-1234.0));
        assert_eq!(parse_numeric(b"+1,234"), Some(1234.0));
        assert_eq!(parse_numeric(b"1,234,567.89"), Some(1234567.89));
        assert_eq!(parse_numeric(b"-1,234,567.89"), Some(-1234567.89));
    }

    #[test]
    fn parses_currency_prefix() {
        assert_eq!(parse_numeric(b"$123.45"), Some(123.45));
        assert_eq!(parse_numeric(b"$1,234.56"), Some(1234.56));
        assert_eq!(parse_numeric(b"-$1,234.56"), Some(-1234.56));
        assert_eq!(parse_numeric(b"$-1,234.56"), Some(-1234.56));
        assert_eq!(parse_numeric(b"+$1,234.56"), Some(1234.56));
        assert_eq!(parse_numeric(b"$+1,234.56"), Some(1234.56));
    }

    #[test]
    fn parses_accounting_parentheses() {
        assert_eq!(parse_numeric(b"(123.45)"), Some(-123.45));
        assert_eq!(parse_numeric(b"(1,234.56)"), Some(-1234.56));
        assert_eq!(parse_numeric(b"($1,234.56)"), Some(-1234.56));
        assert_eq!(parse_numeric(b"($-1,234.56)"), Some(-1234.56));
    }

    #[test]
    fn rejects_invalid_commas() {
        assert_eq!(parse_numeric(b"12,34"), None);
        assert_eq!(parse_numeric(b"1,23,456"), None);
        assert_eq!(parse_numeric(b"1,234,56.78"), None);
        assert_eq!(parse_numeric(b",123"), None);
        assert_eq!(parse_numeric(b"123,"), None);
    }

    #[test]
    fn rejects_invalid_tokens() {
        assert_eq!(parse_numeric(b""), None);
        assert_eq!(parse_numeric(b"$"), None);
        assert_eq!(parse_numeric(b"sep=,"), None);
        assert_eq!(parse_numeric(b"1,234.5.6"), None);
        assert_eq!(parse_numeric(b"+$-1"), None);
        assert_eq!(parse_numeric(b"--1"), None);
        assert_eq!(parse_numeric(b"NaN"), None);
        assert_eq!(parse_numeric(b"inf"), None);
        assert_eq!(parse_numeric(b"+inf"), None);
        assert_eq!(parse_numeric(b"-inf"), None);
    }

    #[test]
    fn trims_ascii_whitespace() {
        assert_eq!(parse_numeric(b"  123  "), Some(123.0));
        assert_eq!(parse_numeric(b"\t$1,234.00\t"), Some(1234.0));
    }
}
