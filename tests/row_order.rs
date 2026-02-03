use rvl::alignment::key_discovery::KeyRow;
use rvl::alignment::row_order::row_order_aligner;
use rvl::alignment::shuffle::detect_shuffle;

#[derive(Debug)]
struct Row(Vec<Vec<u8>>);

impl KeyRow for Row {
    fn field(&self, index: usize) -> &[u8] {
        self.0.get(index).map(|v| v.as_slice()).unwrap_or(b"")
    }
}

#[test]
fn row_order_reports_rowcount_mismatch() {
    let old_rows = vec![1, 2, 3];
    let new_rows = vec![10, 20];
    let mut aligner = row_order_aligner(old_rows, new_rows);

    assert!(aligner.next().unwrap().is_ok());
    assert!(aligner.next().unwrap().is_ok());

    let err = aligner.next().unwrap().unwrap_err();
    assert_eq!(err.rows_old, 3);
    assert_eq!(err.rows_new, 2);
}

#[test]
fn shuffle_detection_flags_reorder_under_perfect_key() {
    let old_headers = vec![b"id".to_vec(), b"value".to_vec()];
    let new_headers = vec![b"id".to_vec(), b"value".to_vec()];
    let old_rows = vec![
        Row(vec![b"a".to_vec(), b"1".to_vec()]),
        Row(vec![b"b".to_vec(), b"2".to_vec()]),
    ];
    let new_rows = vec![
        Row(vec![b"b".to_vec(), b"3".to_vec()]),
        Row(vec![b"a".to_vec(), b"4".to_vec()]),
    ];

    let detection = detect_shuffle(&old_headers, &new_headers, &old_rows, &new_rows);
    assert!(detection.reordered);
    assert_eq!(
        detection.suggested_keys,
        vec![b"id".to_vec(), b"value".to_vec()]
    );
}
