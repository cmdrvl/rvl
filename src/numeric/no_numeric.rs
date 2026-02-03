//! No numeric columns refusal E_NO_NUMERIC (bd-dii).
//!
//! Provides helpers to detect the no-numeric-columns condition and to produce
//! deterministic sample lists for refusal details.

use crate::refusal::details::{RefusalDetail, RefusalKind, RerunPaths};

pub const MAX_NO_NUMERIC_SAMPLES: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoNumericError {
    pub columns_common: usize,
    pub sample_columns: Vec<Vec<u8>>,
}

/// Returns Ok(()) if at least one numeric column exists, otherwise a refusal detail.
pub fn ensure_numeric_columns(
    numeric_columns: usize,
    common_columns: &[Vec<u8>],
) -> Result<(), NoNumericError> {
    if numeric_columns > 0 {
        return Ok(());
    }

    let sample_columns = sample_column_names(common_columns, MAX_NO_NUMERIC_SAMPLES);
    Err(NoNumericError {
        columns_common: common_columns.len(),
        sample_columns,
    })
}

/// Deterministically sample column names (sorted by raw bytes, truncated).
pub fn sample_column_names(columns: &[Vec<u8>], limit: usize) -> Vec<Vec<u8>> {
    let mut names = columns.to_vec();
    names.sort();
    if names.len() > limit {
        names.truncate(limit);
    }
    names
}

/// Build a refusal detail for E_NO_NUMERIC using default Next guidance.
pub fn build_no_numeric_refusal(paths: RerunPaths<'_>) -> RefusalDetail {
    RefusalDetail::with_default_next(RefusalKind::NoNumeric, paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_error_when_no_numeric_columns() {
        let columns = vec![b"b".to_vec(), b"a".to_vec(), b"c".to_vec()];
        let err = ensure_numeric_columns(0, &columns).expect_err("no numeric");
        assert_eq!(err.columns_common, 3);
        assert_eq!(
            err.sample_columns,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn returns_ok_when_numeric_columns_exist() {
        let columns = vec![b"only".to_vec()];
        assert!(ensure_numeric_columns(1, &columns).is_ok());
    }

    #[test]
    fn sample_truncates_deterministically() {
        let columns = vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()];
        let sample = sample_column_names(&columns, 2);
        assert_eq!(sample, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn builds_refusal_detail() {
        let detail = build_no_numeric_refusal(RerunPaths {
            old: "old.csv",
            new: "new.csv",
        });
        matches!(detail.kind, RefusalKind::NoNumeric);
    }
}
