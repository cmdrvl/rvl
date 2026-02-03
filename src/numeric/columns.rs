//! Column typing & numeric intersection (bd-3hn).
//!
//! Determines which common columns are numeric and enforces refusal rules
//! for mixed types and missingness mismatches.

use std::collections::{HashMap, HashSet};

use crate::csv::records::NormalizedRecord;
use crate::numeric::missing::is_missing_token;
use crate::numeric::parse::parse_numeric;

/// Column present in both files (after header normalization).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonColumn {
    pub name: Vec<u8>,
    pub old_index: usize,
    pub new_index: usize,
}

/// Header intersection results (excluding the key column, if provided).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnIntersection {
    pub common: Vec<CommonColumn>,
    pub old_only: Vec<Vec<u8>>,
    pub new_only: Vec<Vec<u8>>,
}

/// File side for error reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Old,
    New,
}

/// Error returned when a column contains both numeric and non-numeric tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MixedTypesError<RowId> {
    pub row_id: RowId,
    pub column: Vec<u8>,
    pub side: Side,
    pub value: Vec<u8>,
}

/// Error returned when one side is missing and the other is numeric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingnessError<RowId> {
    pub row_id: RowId,
    pub column: Vec<u8>,
    pub missing_side: Side,
    pub present_value: Vec<u8>,
}

/// Column typing failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnTypingError<RowId> {
    MixedTypes(MixedTypesError<RowId>),
    Missingness(MissingnessError<RowId>),
}

/// Minimal access to CSV fields for column typing.
pub trait FieldAccess {
    fn field(&self, index: usize) -> &[u8];
}

impl<'a> FieldAccess for NormalizedRecord<'a> {
    fn field(&self, index: usize) -> &[u8] {
        NormalizedRecord::field(self, index)
    }
}

impl FieldAccess for Vec<Vec<u8>> {
    fn field(&self, index: usize) -> &[u8] {
        self.get(index).map(|v| v.as_slice()).unwrap_or(b"")
    }
}

impl FieldAccess for &[Vec<u8>] {
    fn field(&self, index: usize) -> &[u8] {
        self.get(index).map(|v| v.as_slice()).unwrap_or(b"")
    }
}

/// Compute the common columns and old/new-only lists.
pub fn intersect_headers(
    old_headers: &[Vec<u8>],
    new_headers: &[Vec<u8>],
    key: Option<&[u8]>,
) -> ColumnIntersection {
    let key = key.unwrap_or(b"");
    let mut new_index: HashMap<&[u8], usize> = HashMap::new();
    for (idx, name) in new_headers.iter().enumerate() {
        if name.as_slice() == key {
            continue;
        }
        new_index.insert(name.as_slice(), idx);
    }

    let mut common = Vec::new();
    let mut old_only = Vec::new();
    let mut old_seen: HashSet<&[u8]> = HashSet::new();

    for (idx, name) in old_headers.iter().enumerate() {
        if name.as_slice() == key {
            continue;
        }
        old_seen.insert(name.as_slice());
        if let Some(new_idx) = new_index.get(name.as_slice()) {
            common.push(CommonColumn {
                name: name.clone(),
                old_index: idx,
                new_index: *new_idx,
            });
        } else {
            old_only.push(name.clone());
        }
    }

    let mut new_only = Vec::new();
    for name in new_headers {
        if name.as_slice() == key {
            continue;
        }
        if !old_seen.contains(name.as_slice()) {
            new_only.push(name.clone());
        }
    }

    ColumnIntersection {
        common,
        old_only,
        new_only,
    }
}

/// Determine numeric columns and refuse mixed/missingness cases.
pub fn detect_numeric_columns<RowId, Old, New, I>(
    columns: &[CommonColumn],
    rows: I,
) -> Result<Vec<CommonColumn>, ColumnTypingError<RowId>>
where
    RowId: Clone,
    Old: FieldAccess,
    New: FieldAccess,
    I: IntoIterator<Item = (RowId, Old, New)>,
{
    let mut states: Vec<ColumnState<RowId>> = columns
        .iter()
        .cloned()
        .map(|column| ColumnState::new(column))
        .collect();

    for (row_id, old, new) in rows {
        for state in &mut states {
            let old_raw = old.field(state.column.old_index);
            let new_raw = new.field(state.column.new_index);

            let old_missing = is_missing_token(old_raw);
            let new_missing = is_missing_token(new_raw);

            if old_missing && new_missing {
                continue;
            }

            if old_missing || new_missing {
                let (present_raw, present_side, missing_side) = if old_missing {
                    (new_raw, Side::New, Side::Old)
                } else {
                    (old_raw, Side::Old, Side::New)
                };

                if parse_numeric(present_raw).is_some() {
                    return Err(ColumnTypingError::Missingness(MissingnessError {
                        row_id: row_id.clone(),
                        column: state.column.name.clone(),
                        missing_side,
                        present_value: present_raw.to_vec(),
                    }));
                }

                if state.saw_numeric {
                    return Err(ColumnTypingError::MixedTypes(MixedTypesError {
                        row_id: row_id.clone(),
                        column: state.column.name.clone(),
                        side: present_side,
                        value: present_raw.to_vec(),
                    }));
                }

                state.record_non_numeric(row_id.clone(), present_side, present_raw);
                continue;
            }

            let old_num = parse_numeric(old_raw);
            let new_num = parse_numeric(new_raw);

            match (old_num.is_some(), new_num.is_some()) {
                (true, true) => {
                    if let Some(non_numeric) = state.first_non_numeric.take() {
                        return Err(ColumnTypingError::MixedTypes(MixedTypesError {
                            row_id: non_numeric.row_id,
                            column: state.column.name.clone(),
                            side: non_numeric.side,
                            value: non_numeric.value,
                        }));
                    }
                    state.saw_numeric = true;
                }
                (true, false) | (false, true) => {
                    let (non_numeric_raw, non_numeric_side) = if old_num.is_some() {
                        (new_raw, Side::New)
                    } else {
                        (old_raw, Side::Old)
                    };

                    if state.saw_numeric {
                        return Err(ColumnTypingError::MixedTypes(MixedTypesError {
                            row_id: row_id.clone(),
                            column: state.column.name.clone(),
                            side: non_numeric_side,
                            value: non_numeric_raw.to_vec(),
                        }));
                    }

                    state.record_non_numeric(row_id.clone(), non_numeric_side, non_numeric_raw);
                }
                (false, false) => {
                    if state.saw_numeric {
                        return Err(ColumnTypingError::MixedTypes(MixedTypesError {
                            row_id: row_id.clone(),
                            column: state.column.name.clone(),
                            side: Side::Old,
                            value: old_raw.to_vec(),
                        }));
                    }
                    state.record_non_numeric(row_id.clone(), Side::Old, old_raw);
                }
            }
        }
    }

    let numeric = states
        .into_iter()
        .filter(|state| state.saw_numeric)
        .map(|state| state.column)
        .collect();

    Ok(numeric)
}

#[derive(Debug)]
struct ColumnState<RowId> {
    column: CommonColumn,
    saw_numeric: bool,
    first_non_numeric: Option<NonNumeric<RowId>>,
}

impl<RowId> ColumnState<RowId> {
    fn new(column: CommonColumn) -> Self {
        Self {
            column,
            saw_numeric: false,
            first_non_numeric: None,
        }
    }

    fn record_non_numeric(&mut self, row_id: RowId, side: Side, value: &[u8]) {
        if self.first_non_numeric.is_none() {
            self.first_non_numeric = Some(NonNumeric {
                row_id,
                side,
                value: value.to_vec(),
            });
        }
    }
}

#[derive(Debug)]
struct NonNumeric<RowId> {
    row_id: RowId,
    side: Side,
    value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn intersect_headers_excludes_key() {
        let old = vec![b"id".to_vec(), b"a".to_vec(), b"b".to_vec()];
        let new = vec![b"a".to_vec(), b"id".to_vec(), b"c".to_vec()];
        let intersection = intersect_headers(&old, &new, Some(b"id"));
        assert_eq!(
            intersection.common,
            vec![CommonColumn {
                name: b"a".to_vec(),
                old_index: 1,
                new_index: 0
            }]
        );
        assert_eq!(intersection.old_only, vec![b"b".to_vec()]);
        assert_eq!(intersection.new_only, vec![b"c".to_vec()]);
    }

    #[test]
    fn numeric_column_detected() {
        let columns = vec![column("a", 0, 0)];
        let rows = vec![
            (1u64, record(&[b"1"]), record(&[b"2"])),
            (2u64, record(&[b""]), record(&[b""])),
        ];
        let numeric = detect_numeric_columns(&columns, rows).expect("numeric");
        assert_eq!(numeric.len(), 1);
        assert_eq!(numeric[0].name, b"a".to_vec());
    }

    #[test]
    fn non_numeric_column_is_ignored() {
        let columns = vec![column("a", 0, 0)];
        let rows = vec![(1u64, record(&[b"foo"]), record(&[b"bar"]))];
        let numeric = detect_numeric_columns(&columns, rows).expect("ok");
        assert!(numeric.is_empty());
    }

    #[test]
    fn mixed_types_numeric_then_text() {
        let columns = vec![column("a", 0, 0)];
        let rows = vec![
            (1u64, record(&[b"1"]), record(&[b"2"])),
            (2u64, record(&[b"foo"]), record(&[b"bar"])),
        ];
        let err = detect_numeric_columns(&columns, rows).unwrap_err();
        match err {
            ColumnTypingError::MixedTypes(detail) => {
                assert_eq!(detail.row_id, 2);
                assert_eq!(detail.side, Side::Old);
                assert_eq!(detail.value, b"foo".to_vec());
            }
            _ => panic!("expected mixed types"),
        }
    }

    #[test]
    fn mixed_types_text_then_numeric_reports_first_text() {
        let columns = vec![column("a", 0, 0)];
        let rows = vec![
            (1u64, record(&[b"foo"]), record(&[b"bar"])),
            (2u64, record(&[b"1"]), record(&[b"2"])),
        ];
        let err = detect_numeric_columns(&columns, rows).unwrap_err();
        match err {
            ColumnTypingError::MixedTypes(detail) => {
                assert_eq!(detail.row_id, 1);
                assert_eq!(detail.side, Side::Old);
                assert_eq!(detail.value, b"foo".to_vec());
            }
            _ => panic!("expected mixed types"),
        }
    }

    #[test]
    fn missingness_is_refused() {
        let columns = vec![column("a", 0, 0)];
        let rows = vec![(7u64, record(&[b""]), record(&[b"9"]))];
        let err = detect_numeric_columns(&columns, rows).unwrap_err();
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
    fn missing_vs_text_without_numeric_is_ignored() {
        let columns = vec![column("a", 0, 0)];
        let rows = vec![
            (1u64, record(&[b""]), record(&[b"foo"])),
            (2u64, record(&[b""]), record(&[b"bar"])),
        ];
        let numeric = detect_numeric_columns(&columns, rows).expect("ok");
        assert!(numeric.is_empty());
    }

    #[test]
    fn missing_vs_text_then_numeric_is_mixed_types() {
        let columns = vec![column("a", 0, 0)];
        let rows = vec![
            (1u64, record(&[b""]), record(&[b"foo"])),
            (2u64, record(&[b"1"]), record(&[b"2"])),
        ];
        let err = detect_numeric_columns(&columns, rows).unwrap_err();
        match err {
            ColumnTypingError::MixedTypes(detail) => {
                assert_eq!(detail.row_id, 1);
                assert_eq!(detail.side, Side::New);
                assert_eq!(detail.value, b"foo".to_vec());
            }
            _ => panic!("expected mixed types"),
        }
    }
}
