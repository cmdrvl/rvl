use rvl::normalize::headers::{DuplicateHeader, normalize_header_name, normalize_headers};

#[test]
fn trims_and_preserves_header_bytes() {
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
fn duplicate_headers_refuse_after_trim() {
    let headers = vec![b" foo ".as_slice(), b"foo".as_slice()];
    let err = normalize_headers(headers).expect_err("duplicate");
    assert_eq!(
        err,
        DuplicateHeader {
            name: b"foo".to_vec(),
            first_index: 1,
            second_index: 2
        }
    );
}

#[test]
fn normalization_is_case_sensitive() {
    let headers = vec![b"Foo".as_slice(), b"foo".as_slice()];
    let normalized = normalize_headers(headers).expect("normalize headers");
    assert_eq!(normalized, vec![b"Foo".to_vec(), b"foo".to_vec()]);
}

#[test]
fn normalize_header_name_empty_uses_index() {
    assert_eq!(normalize_header_name(b" \t ", 3), b"__rvl_col_3".to_vec());
}
