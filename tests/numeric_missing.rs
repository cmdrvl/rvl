use rvl::numeric::columns::{ColumnTypingError, CommonColumn, Side, detect_numeric_columns};
use rvl::numeric::missing::is_missing_token;
use rvl::numeric::no_numeric::{NoNumericError, ensure_numeric_columns};

#[test]
fn missing_tokens_case_insensitive_and_trimmed() {
    assert!(is_missing_token(b""));
    assert!(is_missing_token(b"  \t  "));
    assert!(is_missing_token(b"NA"));
    assert!(is_missing_token(b"n/a"));
    assert!(is_missing_token(b"Null"));
    assert!(is_missing_token(b"nan"));
    assert!(is_missing_token(b"none"));
    assert!(is_missing_token(b"-"));
    assert!(is_missing_token(b"  -  "));
}

#[test]
fn non_missing_tokens_rejected() {
    assert!(!is_missing_token(b"0"));
    assert!(!is_missing_token(b"NA_"));
    assert!(!is_missing_token(b"NULLS"));
    assert!(!is_missing_token(b"--"));
}

fn column(name: &str, old_index: usize, new_index: usize) -> CommonColumn {
    CommonColumn {
        name: name.as_bytes().to_vec(),
        old_index,
        new_index,
    }
}

fn record(fields: &[&[u8]]) -> Vec<Vec<u8>> {
    fields.iter().map(|field| field.to_vec()).collect()
}

#[test]
fn mixed_types_refused_when_numeric_then_text() {
    let columns = vec![column("amount", 0, 0)];
    let rows = vec![
        (1u64, record(&[b"10"]), record(&[b"11"])),
        (2u64, record(&[b"oops"]), record(&[b"12"])),
    ];
    let err = detect_numeric_columns(&columns, rows).expect_err("mixed types");
    match err {
        ColumnTypingError::MixedTypes(detail) => {
            assert_eq!(detail.row_id, 2);
            assert_eq!(detail.side, Side::Old);
            assert_eq!(detail.value, b"oops".to_vec());
        }
        _ => panic!("expected mixed types"),
    }
}

#[test]
fn missingness_refused_when_one_side_missing_numeric() {
    let columns = vec![column("amount", 0, 0)];
    let rows = vec![(7u64, record(&[b""]), record(&[b"9"]))];
    let err = detect_numeric_columns(&columns, rows).expect_err("missingness");
    match err {
        ColumnTypingError::Missingness(detail) => {
            assert_eq!(detail.row_id, 7);
            assert_eq!(detail.missing_side, Side::Old);
            assert_eq!(detail.present_value, b"9".to_vec());
        }
        _ => panic!("expected missingness"),
    }
}

#[test]
fn no_numeric_error_when_none_numeric() {
    let common = vec![b"alpha".to_vec(), b"beta".to_vec()];
    let err = ensure_numeric_columns(0, &common).expect_err("no numeric");
    assert_eq!(
        err,
        NoNumericError {
            columns_common: 2,
            sample_columns: vec![b"alpha".to_vec(), b"beta".to_vec()]
        }
    );
}
