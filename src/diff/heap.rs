// Total change (L1) and top-K contributor heap (bd-37b)

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub const MAX_CONTRIBUTORS: usize = 25;

#[derive(Debug, Clone)]
pub struct Contributor<T> {
    pub id: T,
    pub delta: f64,
    pub contribution: f64,
    pub tie_break: u64,
}

impl<T> Contributor<T> {
    pub fn new(id: T, delta: f64, contribution: f64, tie_break: u64) -> Self {
        Self {
            id,
            delta,
            contribution,
            tie_break,
        }
    }
}

#[derive(Debug)]
pub struct DiffAccumulator<T> {
    pub total_change: f64,
    pub max_abs_delta: f64,
    pub top: TopContributors<T>,
}

impl<T> DiffAccumulator<T> {
    pub fn new(max: usize) -> Self {
        Self {
            total_change: 0.0,
            max_abs_delta: 0.0,
            top: TopContributors::new(max),
        }
    }

    pub fn with_default_max() -> Self {
        Self::new(MAX_CONTRIBUTORS)
    }

    pub fn observe(&mut self, id: T, delta: f64, contribution: f64, tie_break: u64) {
        debug_assert!(delta.is_finite(), "delta must be finite");
        debug_assert!(contribution.is_finite(), "contribution must be finite");
        debug_assert!(contribution >= 0.0, "contribution must be non-negative");

        let abs_delta = delta.abs();
        if abs_delta > self.max_abs_delta {
            self.max_abs_delta = abs_delta;
        }

        self.total_change += contribution;

        if contribution > 0.0 {
            self.top
                .push(Contributor::new(id, delta, contribution, tie_break));
        }
    }
}

#[derive(Debug)]
pub struct TopContributors<T> {
    max: usize,
    heap: BinaryHeap<Reverse<HeapItem<T>>>,
}

impl<T> TopContributors<T> {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            heap: BinaryHeap::new(),
        }
    }

    pub fn max(&self) -> usize {
        self.max
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn min_contribution(&self) -> Option<f64> {
        self.heap
            .peek()
            .map(|entry| entry.0.contributor.contribution)
    }

    pub fn push(&mut self, contributor: Contributor<T>) {
        if self.max == 0 {
            return;
        }

        self.heap.push(Reverse(HeapItem { contributor }));
        if self.heap.len() > self.max {
            self.heap.pop();
        }
    }

    pub fn into_vec(self) -> Vec<Contributor<T>> {
        self.heap
            .into_iter()
            .map(|entry| entry.0.contributor)
            .collect()
    }
}

#[derive(Debug)]
struct HeapItem<T> {
    contributor: Contributor<T>,
}

impl<T> PartialEq for HeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.contributor.contribution.to_bits() == other.contributor.contribution.to_bits()
            && self.contributor.tie_break == other.contributor.tie_break
    }
}

impl<T> Eq for HeapItem<T> {}

impl<T> PartialOrd for HeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for HeapItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self
            .contributor
            .contribution
            .total_cmp(&other.contributor.contribution)
        {
            Ordering::Equal => other.contributor.tie_break.cmp(&self.contributor.tie_break),
            ord => ord,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contributor(
        id: &'static str,
        delta: f64,
        contribution: f64,
        tie_break: u64,
    ) -> Contributor<&'static str> {
        Contributor::new(id, delta, contribution, tie_break)
    }

    #[test]
    fn topk_keeps_largest_contributions() {
        let mut top = TopContributors::new(2);
        top.push(contributor("a", 1.0, 1.0, 1));
        top.push(contributor("b", 5.0, 5.0, 2));
        top.push(contributor("c", 3.0, 3.0, 3));

        let mut values: Vec<f64> = top.into_vec().into_iter().map(|c| c.contribution).collect();
        values.sort_by(|a, b| a.total_cmp(b));
        assert_eq!(values, vec![3.0, 5.0]);
    }

    #[test]
    fn topk_tie_break_keeps_earlier_entry() {
        let mut top = TopContributors::new(1);
        top.push(contributor("first", 2.0, 2.0, 1));
        top.push(contributor("second", 2.0, 2.0, 2));

        let kept = top.into_vec();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, "first");
    }

    #[test]
    fn accumulator_tracks_totals_and_max() {
        let mut acc = DiffAccumulator::new(2);
        acc.observe("a", 1.5, 1.5, 1);
        acc.observe("b", -3.0, 3.0, 2);
        acc.observe("c", 0.0, 0.0, 3);

        assert_eq!(acc.total_change, 4.5);
        assert_eq!(acc.max_abs_delta, 3.0);
        assert_eq!(acc.top.len(), 2);
    }
}
