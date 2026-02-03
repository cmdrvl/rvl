use std::cmp::Ordering;

use super::heap::Contributor;

/// Row identifier used for deterministic ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowId {
    RowIndex(usize),
    Key(Vec<u8>),
}

impl RowId {
    pub fn row_index(index: usize) -> Self {
        Self::RowIndex(index)
    }

    pub fn key(bytes: Vec<u8>) -> Self {
        Self::Key(bytes)
    }
}

impl Ord for RowId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (RowId::RowIndex(left), RowId::RowIndex(right)) => left.cmp(right),
            (RowId::Key(left), RowId::Key(right)) => left.cmp(right),
            (RowId::RowIndex(_), RowId::Key(_)) => Ordering::Less,
            (RowId::Key(_), RowId::RowIndex(_)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for RowId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Deterministic identifier for a single numeric cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellId {
    pub row_id: RowId,
    pub column: Vec<u8>,
}

impl CellId {
    pub fn new(row_id: RowId, column: Vec<u8>) -> Self {
        Self { row_id, column }
    }
}

impl Ord for CellId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.row_id.cmp(&other.row_id) {
            Ordering::Equal => self.column.cmp(&other.column),
            ord => ord,
        }
    }
}

impl PartialOrd for CellId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Monotonic tie-breaker for stable top-K selection.
#[derive(Debug, Default)]
pub struct TieBreaker {
    next: u64,
}

impl TieBreaker {
    pub fn next_value(&mut self) -> u64 {
        let current = self.next;
        self.next = self.next.wrapping_add(1);
        current
    }
}

/// Sort contributors by contribution desc, row_id asc, column asc.
pub fn sort_contributors(items: &mut [Contributor<CellId>]) {
    items.sort_by(
        |left, right| match right.contribution.total_cmp(&left.contribution) {
            Ordering::Equal => left.id.cmp(&right.id),
            ord => ord,
        },
    );
}

/// Sort a list of byte-like values lexicographically (raw bytes asc).
pub fn sort_bytes<T: AsRef<[u8]>>(items: &mut [T]) {
    items.sort_by(|left, right| left.as_ref().cmp(right.as_ref()));
}

/// Sort a list of byte-like values and truncate deterministically.
pub fn sort_and_truncate_bytes<T: AsRef<[u8]>>(items: &mut Vec<T>, limit: usize) {
    sort_bytes(items);
    if items.len() > limit {
        items.truncate(limit);
    }
}

#[cfg(test)]
mod tests {
    use super::{CellId, RowId, sort_and_truncate_bytes, sort_contributors};
    use crate::diff::heap::Contributor;

    fn contributor(row: usize, column: &str, contribution: f64) -> Contributor<CellId> {
        Contributor::new(
            CellId::new(RowId::row_index(row), column.as_bytes().to_vec()),
            0.0,
            contribution,
            0,
        )
    }

    #[test]
    fn row_id_orders_numeric_then_column() {
        let mut items = vec![
            contributor(2, "b", 1.0),
            contributor(1, "b", 1.0),
            contributor(1, "a", 1.0),
        ];
        sort_contributors(&mut items);
        assert_eq!(items[0].id.row_id, RowId::row_index(1));
        assert_eq!(items[0].id.column, b"a");
        assert_eq!(items[1].id.row_id, RowId::row_index(1));
        assert_eq!(items[1].id.column, b"b");
        assert_eq!(items[2].id.row_id, RowId::row_index(2));
    }

    #[test]
    fn contributors_sort_by_contribution_desc() {
        let mut items = vec![contributor(1, "a", 2.0), contributor(1, "b", 5.0)];
        sort_contributors(&mut items);
        assert_eq!(items[0].contribution, 5.0);
        assert_eq!(items[1].contribution, 2.0);
    }

    #[test]
    fn sort_and_truncate_is_deterministic() {
        let mut items = vec![b"b".to_vec(), b"a".to_vec(), b"c".to_vec()];
        sort_and_truncate_bytes(&mut items, 2);
        assert_eq!(items, vec![b"a".to_vec(), b"b".to_vec()]);
    }
}
