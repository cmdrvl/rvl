//! Header normalization & duplicate detection (bd-s0x).
//!
//! Rules:
//! - ASCII-trim header bytes (spaces + tabs only).
//! - Empty headers become `__rvl_col_<1-based index>`.
//! - Normalized headers must be unique (byte-for-byte); duplicates are errors.

use std::collections::HashMap;

use crate::normalize::trim::ascii_trim;

/// Duplicate header error (after normalization).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateHeader {
    pub name: Vec<u8>,
    pub first_index: usize,
    pub second_index: usize,
}

/// Normalize headers according to v0 rules.
pub fn normalize_headers<'a, I>(headers: I) -> Result<Vec<Vec<u8>>, DuplicateHeader>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut normalized = Vec::new();
    let mut seen: HashMap<Vec<u8>, usize> = HashMap::new();

    for (idx, header) in headers.into_iter().enumerate() {
        let name = normalize_header_name(header, idx + 1);
        if let Some(first) = seen.get(&name).copied() {
            return Err(DuplicateHeader {
                name,
                first_index: first,
                second_index: idx + 1,
            });
        }
        seen.insert(name.clone(), idx + 1);
        normalized.push(name);
    }

    Ok(normalized)
}

/// Normalize a single header name.
pub fn normalize_header_name(header: &[u8], index: usize) -> Vec<u8> {
    let trimmed = ascii_trim(header);
    if trimmed.is_empty() {
        format!("__rvl_col_{index}").into_bytes()
    } else {
        trimmed.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_headers_and_preserves_bytes() {
        let headers = vec![b" foo ".as_slice(), b"\tbar\t".as_slice()];
        let normalized = normalize_headers(headers).expect("normalize headers");
        assert_eq!(normalized, vec![b"foo".to_vec(), b"bar".to_vec()]);
    }

    #[test]
    fn empty_headers_are_numbered() {
        let headers = vec![b" ".as_slice(), b"".as_slice()];
        let normalized = normalize_headers(headers).expect("normalize headers");
        assert_eq!(
            normalized,
            vec![b"__rvl_col_1".to_vec(), b"__rvl_col_2".to_vec()]
        );
    }

    #[test]
    fn detects_duplicates_after_trim() {
        let headers = vec![b" foo ".as_slice(), b"foo".as_slice()];
        let err = normalize_headers(headers).expect_err("duplicate");
        assert_eq!(err.name, b"foo".to_vec());
        assert_eq!(err.first_index, 1);
        assert_eq!(err.second_index, 2);
    }

    #[test]
    fn case_sensitive_uniqueness() {
        let headers = vec![b"Foo".as_slice(), b"foo".as_slice()];
        let normalized = normalize_headers(headers).expect("normalize headers");
        assert_eq!(normalized, vec![b"Foo".to_vec(), b"foo".to_vec()]);
    }
}
