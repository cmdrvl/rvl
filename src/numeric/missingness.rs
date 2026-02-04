// Missingness mismatch refusal E_MISSINGNESS (bd-1yk)

use crate::numeric::columns::{MissingnessError, Side};
use crate::refusal::details::{FileSide, RefusalDetail, RefusalKind, RerunPaths};

/// Convert a missingness error into a refusal detail with default Next guidance.
pub fn build_missingness_refusal(
    error: MissingnessError<u64>,
    paths: RerunPaths<'_>,
) -> RefusalDetail {
    let file = match error.missing_side {
        Side::Old => FileSide::New,
        Side::New => FileSide::Old,
    };
    let kind = RefusalKind::Missingness {
        file,
        record: error.row_id,
        column: error.column,
        value: error.present_value,
        key_value: None,
    };
    RefusalDetail::with_default_next(kind, paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_missing_side_to_present_side() {
        let err = MissingnessError {
            row_id: 7u64,
            column: b"amount".to_vec(),
            missing_side: Side::Old,
            present_value: b"9".to_vec(),
        };
        let detail = build_missingness_refusal(
            err,
            RerunPaths {
                old: "old.csv",
                new: "new.csv",
            },
        );
        if let RefusalKind::Missingness { file, record, .. } = detail.kind {
            assert_eq!(file, FileSide::New);
            assert_eq!(record, 7);
        } else {
            panic!("expected missingness kind");
        }
    }
}
