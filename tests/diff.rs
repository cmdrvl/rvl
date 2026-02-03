use rvl::diff::coverage::{CoverageDecision, evaluate_coverage};
use rvl::diff::heap::DiffAccumulator;
use rvl::diff::order::{CellId, RowId, TieBreaker, sort_contributors};
use rvl::diff::tolerance::ToleranceTracker;

fn cell_id(row: usize, column: &str) -> CellId {
    CellId::new(RowId::row_index(row), column.as_bytes().to_vec())
}

#[test]
fn total_change_and_max_abs_delta_track_contributions() {
    let mut acc = DiffAccumulator::new(3);
    let mut tie = TieBreaker::default();
    let mut tol = ToleranceTracker::new(0.5);

    let (d1, c1) = tol.apply(10.0, 10.2); // within tolerance
    acc.observe(cell_id(1, "a"), d1, c1, tie.next_value());

    let (d2, c2) = tol.apply(5.0, 7.0); // +2.0
    acc.observe(cell_id(2, "b"), d2, c2, tie.next_value());

    let (d3, c3) = tol.apply(3.0, 0.0); // -3.0
    acc.observe(cell_id(3, "c"), d3, c3, tie.next_value());

    assert_eq!(acc.total_change, 5.0);
    assert_eq!(acc.max_abs_delta, 3.0);
}

#[test]
fn topk_ordering_is_deterministic() {
    let mut acc = DiffAccumulator::new(2);
    let mut tie = TieBreaker::default();
    let mut tol = ToleranceTracker::new(0.0);

    let (d1, c1) = tol.apply(1.0, 6.0); // +5
    acc.observe(cell_id(1, "b"), d1, c1, tie.next_value());
    let (d2, c2) = tol.apply(1.0, 4.0); // +3
    acc.observe(cell_id(2, "a"), d2, c2, tie.next_value());
    let (d3, c3) = tol.apply(1.0, 2.0); // +1
    acc.observe(cell_id(3, "c"), d3, c3, tie.next_value());

    let mut top = acc.top.into_vec();
    sort_contributors(&mut top);
    assert_eq!(top.len(), 2);
    assert_eq!(top[0].contribution, 5.0);
    assert_eq!(top[1].contribution, 3.0);
}

#[test]
fn diffuse_when_topk_below_threshold() {
    let mut acc = DiffAccumulator::new(2);
    let mut tie = TieBreaker::default();
    let mut tol = ToleranceTracker::new(0.0);

    let (d1, c1) = tol.apply(0.0, 10.0);
    acc.observe(cell_id(1, "a"), d1, c1, tie.next_value());
    let (d2, c2) = tol.apply(0.0, 5.0);
    acc.observe(cell_id(2, "b"), d2, c2, tie.next_value());
    let (d3, c3) = tol.apply(0.0, 3.0);
    acc.observe(cell_id(3, "c"), d3, c3, tie.next_value());

    let mut top = acc.top.into_vec();
    sort_contributors(&mut top);
    let contributions: Vec<f64> = top.iter().map(|c| c.contribution).collect();

    let decision = evaluate_coverage(&contributions, acc.total_change, 0.9);
    assert_eq!(
        decision,
        CoverageDecision::Diffuse {
            top_k_coverage: 15.0 / 18.0
        }
    );
}

#[test]
fn explainable_when_prefix_reaches_threshold() {
    let mut acc = DiffAccumulator::new(3);
    let mut tie = TieBreaker::default();
    let mut tol = ToleranceTracker::new(0.0);

    let (d1, c1) = tol.apply(0.0, 6.0);
    acc.observe(cell_id(1, "a"), d1, c1, tie.next_value());
    let (d2, c2) = tol.apply(0.0, 3.0);
    acc.observe(cell_id(2, "b"), d2, c2, tie.next_value());
    let (d3, c3) = tol.apply(0.0, 1.0);
    acc.observe(cell_id(3, "c"), d3, c3, tie.next_value());

    let mut top = acc.top.into_vec();
    sort_contributors(&mut top);
    let contributions: Vec<f64> = top.iter().map(|c| c.contribution).collect();

    let decision = evaluate_coverage(&contributions, acc.total_change, 0.9);
    assert_eq!(
        decision,
        CoverageDecision::Explainable {
            cutoff: 2,
            coverage: 0.9
        }
    );
}
