//! Key discovery for suggested reruns (bd-22n).
//!
//! Identifies candidate key columns shared by both files. The caller must
//! provide rows with blank records already filtered out.

use std::collections::HashSet;

use crate::csv::records::NormalizedRecord;
use crate::normalize::trim::ascii_trim;

/// Represents a candidate key column for rerun suggestions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyCandidate {
    pub name: Vec<u8>,
    pub old_index: usize,
    pub new_index: usize,
    pub kind: CandidateKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateKind {
    Perfect,
    Joinable,
}

/// A minimal row view for key discovery.
pub trait KeyRow {
    fn field(&self, index: usize) -> &[u8];
}

impl<'a> KeyRow for NormalizedRecord<'a> {
    fn field(&self, index: usize) -> &[u8] {
        NormalizedRecord::field(self, index)
    }
}

impl<T> KeyRow for &T
where
    T: KeyRow + ?Sized,
{
    fn field(&self, index: usize) -> &[u8] {
        (*self).field(index)
    }
}

#[derive(Debug, Default)]
struct ColumnStats {
    values: HashSet<Vec<u8>>,
    has_empty: bool,
    has_dup: bool,
}

impl ColumnStats {
    fn observe(&mut self, raw: &[u8]) {
        let trimmed = ascii_trim(raw);
        if trimmed.is_empty() {
            self.has_empty = true;
            return;
        }
        if !self.values.insert(trimmed.to_vec()) {
            self.has_dup = true;
        }
    }

    fn is_joinable(&self) -> bool {
        !self.has_empty && !self.has_dup
    }
}

struct CandidateWork {
    name: Vec<u8>,
    old_index: usize,
    new_index: usize,
    old_stats: ColumnStats,
    new_stats: ColumnStats,
}

/// Discover key candidates for rerun suggestions.
///
/// The returned list is ordered with perfect candidates first (header order),
/// then remaining joinable candidates (header order). Non-joinable columns
/// are excluded.
pub fn discover_key_candidates<OldIter, NewIter, OldRow, NewRow>(
    old_headers: &[Vec<u8>],
    new_headers: &[Vec<u8>],
    old_rows: OldIter,
    new_rows: NewIter,
) -> Vec<KeyCandidate>
where
    OldIter: IntoIterator<Item = OldRow>,
    NewIter: IntoIterator<Item = NewRow>,
    OldRow: KeyRow,
    NewRow: KeyRow,
{
    let mut candidates = Vec::new();
    for (old_idx, name) in old_headers.iter().enumerate() {
        if let Some(new_idx) = new_headers.iter().position(|n| n == name) {
            candidates.push(CandidateWork {
                name: name.clone(),
                old_index: old_idx,
                new_index: new_idx,
                old_stats: ColumnStats::default(),
                new_stats: ColumnStats::default(),
            });
        }
    }

    if candidates.is_empty() {
        return Vec::new();
    }

    for row in old_rows {
        for candidate in &mut candidates {
            candidate.old_stats.observe(row.field(candidate.old_index));
        }
    }

    for row in new_rows {
        for candidate in &mut candidates {
            candidate.new_stats.observe(row.field(candidate.new_index));
        }
    }

    let mut perfect = Vec::new();
    let mut joinable = Vec::new();

    for candidate in candidates {
        if candidate.old_stats.is_joinable() && candidate.new_stats.is_joinable() {
            if candidate.old_stats.values == candidate.new_stats.values {
                perfect.push(KeyCandidate {
                    name: candidate.name,
                    old_index: candidate.old_index,
                    new_index: candidate.new_index,
                    kind: CandidateKind::Perfect,
                });
            } else {
                joinable.push(KeyCandidate {
                    name: candidate.name,
                    old_index: candidate.old_index,
                    new_index: candidate.new_index,
                    kind: CandidateKind::Joinable,
                });
            }
        }
    }

    perfect.extend(joinable);
    perfect
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
    fn discovers_perfect_then_joinable() {
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

        let candidates = discover_key_candidates(&old_headers, &new_headers, old_rows, new_rows);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].name, b"id".to_vec());
        assert_eq!(candidates[0].kind, CandidateKind::Perfect);
        assert_eq!(candidates[1].name, b"value".to_vec());
        assert_eq!(candidates[1].kind, CandidateKind::Joinable);
    }

    #[test]
    fn rejects_empty_or_duplicate_keys() {
        let headers = vec![b"id".to_vec()];
        let old_rows = vec![Row(vec![b"a".to_vec()]), Row(vec![b" ".to_vec()])];
        let new_rows = vec![Row(vec![b"a".to_vec()])];

        let candidates = discover_key_candidates(&headers, &headers, old_rows, new_rows);
        assert!(candidates.is_empty());

        let old_rows = vec![Row(vec![b"a".to_vec()]), Row(vec![b"a".to_vec()])];
        let new_rows = vec![Row(vec![b"a".to_vec()]), Row(vec![b"b".to_vec()])];
        let candidates = discover_key_candidates(&headers, &headers, old_rows, new_rows);
        assert!(candidates.is_empty());
    }

    #[test]
    fn ignores_non_intersecting_headers() {
        let old_headers = vec![b"id".to_vec(), b"a".to_vec()];
        let new_headers = vec![b"id".to_vec(), b"b".to_vec()];
        let old_rows = vec![Row(vec![b"x".to_vec(), b"1".to_vec()])];
        let new_rows = vec![Row(vec![b"x".to_vec(), b"2".to_vec()])];

        let candidates = discover_key_candidates(&old_headers, &new_headers, old_rows, new_rows);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].name, b"id".to_vec());
    }
}
