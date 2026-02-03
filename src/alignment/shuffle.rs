//! Shuffle detection and E_NEED_KEY gating (bd-ykb).
//!
//! Detects row reordering under perfect key candidates to prevent misleading
//! row-order diffs. The caller is responsible for running this only when
//! `total_change > 0` in row-order mode.

use crate::alignment::key_discovery::{
    CandidateKind, KeyCandidate, KeyRow, discover_key_candidates,
};
use crate::normalize::trim::ascii_trim;

#[derive(Debug, Clone)]
pub struct ShuffleDetection {
    pub reordered: bool,
    pub suggested_keys: Vec<Vec<u8>>,
}

impl ShuffleDetection {
    pub fn needs_key(&self) -> bool {
        self.reordered
    }
}

/// Detect whether rows were reordered under any perfect key candidate.
///
/// - `old_rows` and `new_rows` must exclude blank records.
/// - Suggested keys are returned in candidate order, capped at 3.
pub fn detect_shuffle<OldRow, NewRow>(
    old_headers: &[Vec<u8>],
    new_headers: &[Vec<u8>],
    old_rows: &[OldRow],
    new_rows: &[NewRow],
) -> ShuffleDetection
where
    OldRow: KeyRow,
    NewRow: KeyRow,
{
    let candidates =
        discover_key_candidates(old_headers, new_headers, old_rows.iter(), new_rows.iter());
    let suggested_keys = candidate_names(&candidates, 3);

    for candidate in candidates
        .iter()
        .filter(|c| c.kind == CandidateKind::Perfect)
    {
        if has_reorder(candidate, old_rows, new_rows) {
            return ShuffleDetection {
                reordered: true,
                suggested_keys,
            };
        }
    }

    ShuffleDetection {
        reordered: false,
        suggested_keys,
    }
}

fn candidate_names(candidates: &[KeyCandidate], limit: usize) -> Vec<Vec<u8>> {
    candidates
        .iter()
        .take(limit)
        .map(|candidate| candidate.name.clone())
        .collect()
}

fn has_reorder<OldRow, NewRow>(
    candidate: &KeyCandidate,
    old_rows: &[OldRow],
    new_rows: &[NewRow],
) -> bool
where
    OldRow: KeyRow,
    NewRow: KeyRow,
{
    let old_keys = key_sequence(old_rows, candidate.old_index);
    let new_keys = key_sequence(new_rows, candidate.new_index);
    old_keys != new_keys
}

fn key_sequence<Row: KeyRow>(rows: &[Row], index: usize) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| ascii_trim(row.field(index)).to_vec())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Row(Vec<Vec<u8>>);

    impl KeyRow for Row {
        fn field(&self, index: usize) -> &[u8] {
            self.0.get(index).map(|v| v.as_slice()).unwrap_or(b"")
        }
    }

    #[test]
    fn detects_reorder_for_perfect_key() {
        let old_headers = vec![b"id".to_vec(), b"value".to_vec()];
        let new_headers = vec![b"id".to_vec(), b"value".to_vec()];
        let old_rows = vec![
            Row(vec![b"a".to_vec(), b"1".to_vec()]),
            Row(vec![b"b".to_vec(), b"2".to_vec()]),
        ];
        let new_rows = vec![
            Row(vec![b"b".to_vec(), b"4".to_vec()]),
            Row(vec![b"a".to_vec(), b"3".to_vec()]),
        ];

        let detection = detect_shuffle(&old_headers, &new_headers, &old_rows, &new_rows);
        assert!(detection.reordered);
        assert_eq!(
            detection.suggested_keys,
            vec![b"id".to_vec(), b"value".to_vec()]
        );
    }

    #[test]
    fn no_reorder_when_order_is_identical() {
        let old_headers = vec![b"id".to_vec()];
        let new_headers = vec![b"id".to_vec()];
        let old_rows = vec![Row(vec![b"a".to_vec()]), Row(vec![b"b".to_vec()])];
        let new_rows = vec![Row(vec![b"a".to_vec()]), Row(vec![b"b".to_vec()])];

        let detection = detect_shuffle(&old_headers, &new_headers, &old_rows, &new_rows);
        assert!(!detection.reordered);
        assert_eq!(detection.suggested_keys, vec![b"id".to_vec()]);
    }

    #[test]
    fn returns_empty_suggestions_when_no_candidates() {
        let old_headers = vec![b"a".to_vec()];
        let new_headers = vec![b"b".to_vec()];
        let old_rows = vec![Row(vec![b"a".to_vec()])];
        let new_rows = vec![Row(vec![b"a".to_vec()])];

        let detection = detect_shuffle(&old_headers, &new_headers, &old_rows, &new_rows);
        assert!(!detection.reordered);
        assert!(detection.suggested_keys.is_empty());
    }
}
