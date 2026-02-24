use rvl::repro::minimize::{
    ColumnSelectionInput, ReproOutcome, RowAnchor, RowSelectionInput, select_columns, select_rows,
};

#[test]
fn real_change_rows_follow_contributor_order_and_limits() {
    let candidate_rows = vec![
        RowAnchor::RowIndex(1),
        RowAnchor::RowIndex(2),
        RowAnchor::RowIndex(3),
    ];
    let contributor_rows = vec![
        RowAnchor::RowIndex(3),
        RowAnchor::RowIndex(1),
        RowAnchor::RowIndex(3),
        RowAnchor::RowIndex(2),
    ];

    let selection = select_rows(
        ReproOutcome::RealChange,
        RowSelectionInput {
            candidate_rows: &candidate_rows,
            contributor_rows: &contributor_rows,
            refusal_rows: &[],
            max_rows: 2,
        },
    );

    assert_eq!(
        selection.items,
        vec![RowAnchor::RowIndex(3), RowAnchor::RowIndex(1)]
    );
    assert!(selection.truncated);
}

#[test]
fn refusal_rows_prefer_refusal_samples() {
    let candidate_rows = vec![RowAnchor::RowIndex(1), RowAnchor::RowIndex(2)];
    let refusal_rows = vec![RowAnchor::RowIndex(7), RowAnchor::RowIndex(7)];

    let selection = select_rows(
        ReproOutcome::Refusal,
        RowSelectionInput {
            candidate_rows: &candidate_rows,
            contributor_rows: &[],
            refusal_rows: &refusal_rows,
            max_rows: 8,
        },
    );

    assert_eq!(
        selection.items,
        vec![
            RowAnchor::RowIndex(7),
            RowAnchor::RowIndex(1),
            RowAnchor::RowIndex(2)
        ]
    );
    assert!(!selection.truncated);
}

#[test]
fn real_change_columns_keep_key_then_ranked_contributors_then_context() {
    let numeric = vec![b"amount".to_vec(), b"price".to_vec(), b"qty".to_vec()];
    let contributors = vec![b"qty".to_vec(), b"amount".to_vec(), b"qty".to_vec()];
    let key = b"id".to_vec();

    let selection = select_columns(
        ReproOutcome::RealChange,
        ColumnSelectionInput {
            key_column: Some(&key),
            numeric_columns: &numeric,
            contributor_columns: &contributors,
            refusal_columns: &[],
            max_columns: 4,
        },
    );

    assert_eq!(
        selection.items,
        vec![
            b"id".to_vec(),
            b"qty".to_vec(),
            b"amount".to_vec(),
            b"price".to_vec(),
        ]
    );
    assert!(!selection.truncated);
}

#[test]
fn no_real_change_columns_apply_guardrail_limit() {
    let numeric = vec![
        b"c1".to_vec(),
        b"c2".to_vec(),
        b"c3".to_vec(),
        b"c4".to_vec(),
    ];

    let selection = select_columns(
        ReproOutcome::NoRealChange,
        ColumnSelectionInput {
            key_column: None,
            numeric_columns: &numeric,
            contributor_columns: &[],
            refusal_columns: &[],
            max_columns: 2,
        },
    );

    assert_eq!(selection.items, vec![b"c1".to_vec(), b"c2".to_vec()]);
    assert!(selection.truncated);
}

#[test]
fn real_change_rows_fill_with_candidates_when_headroom_exists() {
    let candidate_rows = vec![
        RowAnchor::RowIndex(1),
        RowAnchor::RowIndex(2),
        RowAnchor::RowIndex(3),
    ];
    let contributor_rows = vec![RowAnchor::RowIndex(3)];

    let selection = select_rows(
        ReproOutcome::RealChange,
        RowSelectionInput {
            candidate_rows: &candidate_rows,
            contributor_rows: &contributor_rows,
            refusal_rows: &[],
            max_rows: 8,
        },
    );

    assert_eq!(
        selection.items,
        vec![
            RowAnchor::RowIndex(3),
            RowAnchor::RowIndex(1),
            RowAnchor::RowIndex(2),
        ]
    );
    assert!(!selection.truncated);
}

#[test]
fn no_real_change_rows_use_default_guardrail_when_limit_zero() {
    let candidate_rows: Vec<RowAnchor> = (1_u64..=100).map(RowAnchor::RowIndex).collect();
    let selection = select_rows(
        ReproOutcome::NoRealChange,
        RowSelectionInput {
            candidate_rows: &candidate_rows,
            contributor_rows: &[],
            refusal_rows: &[],
            max_rows: 0,
        },
    );

    assert_eq!(selection.items.len(), 64);
    assert_eq!(selection.items.first(), Some(&RowAnchor::RowIndex(1)));
    assert_eq!(selection.items.last(), Some(&RowAnchor::RowIndex(64)));
    assert!(selection.truncated);
}

#[test]
fn no_real_change_columns_honor_hard_max_guardrail() {
    let numeric_columns: Vec<Vec<u8>> = (1_u16..=200)
        .map(|index| format!("col_{index}").into_bytes())
        .collect();
    let selection = select_columns(
        ReproOutcome::NoRealChange,
        ColumnSelectionInput {
            key_column: None,
            numeric_columns: &numeric_columns,
            contributor_columns: &[],
            refusal_columns: &[],
            max_columns: 9999,
        },
    );

    assert_eq!(selection.items.len(), 128);
    assert_eq!(selection.items.first(), Some(&b"col_1".to_vec()));
    assert_eq!(selection.items.last(), Some(&b"col_128".to_vec()));
    assert!(selection.truncated);
}
