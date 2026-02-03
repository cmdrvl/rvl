use rvl::alignment::key_join::{KeyJoinError, build_key_map, join_key_maps};
use rvl::alignment::key_parse::parse_key_identifier;
use rvl::refusal::details::{RefusalDetail, RefusalKind, RerunPaths};

fn record(fields: &[&[u8]]) -> Vec<Vec<u8>> {
    fields.iter().map(|field| field.to_vec()).collect()
}

#[test]
fn parses_key_identifier_hex_and_utf8() {
    assert_eq!(parse_key_identifier("hex:616263").unwrap(), b"abc".to_vec());
    assert_eq!(parse_key_identifier("u8:col").unwrap(), b"col".to_vec());
    assert_eq!(parse_key_identifier("plain").unwrap(), b"plain".to_vec());
}

#[test]
fn default_next_for_no_key_uses_encoded_identifier() {
    let detail = RefusalDetail::with_default_next(
        RefusalKind::NoKey {
            key_column: b"hex:dead".to_vec(),
        },
        RerunPaths {
            old: "old.csv",
            new: "new.csv",
        },
    );
    assert_eq!(detail.next, "rvl old.csv new.csv --key u8:hex:dead");
}

#[test]
fn key_empty_is_detected() {
    let records = vec![(1, record(&[b"", b"1"]))];
    let err = build_key_map(records, 0).expect_err("empty key");
    assert_eq!(err, KeyJoinError::EmptyKey { record_number: 1 });
}

#[test]
fn key_duplicates_are_detected() {
    let records = vec![(1, record(&[b"A"])), (2, record(&[b"A"]))];
    let err = build_key_map(records, 0).expect_err("duplicate key");
    assert_eq!(
        err,
        KeyJoinError::DuplicateKey {
            key: b"A".to_vec(),
            first_record: 1,
            second_record: 2,
        }
    );
}

#[test]
fn key_set_mismatch_reports_samples() {
    let old = build_key_map(vec![(1, record(&[b"A"])), (2, record(&[b"B"]))], 0).expect("old map");
    let new = build_key_map(vec![(1, record(&[b"A"])), (2, record(&[b"C"]))], 0).expect("new map");
    let err = join_key_maps(old, new).expect_err("mismatch");
    assert_eq!(
        err,
        KeyJoinError::KeySetMismatch {
            missing_count: 1,
            extra_count: 1,
            missing_samples: vec![b"B".to_vec()],
            extra_samples: vec![b"C".to_vec()],
        }
    );
}
