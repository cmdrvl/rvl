//! Coverage calculation and E_DIFFUSE handling (bd-2ug).
//!
//! Contributions must be ordered by contribution descending (top-K).

/// Coverage decision derived from total_change and top-K contributions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CoverageDecision {
    /// No change (total_change == 0).
    NoChange,
    /// Top-K contributors cannot reach threshold.
    Diffuse { top_k_coverage: f64 },
    /// Smallest prefix reaching threshold.
    Explainable { cutoff: usize, coverage: f64 },
}

/// Evaluate coverage against a threshold.
///
/// - `contributions_desc` must be sorted descending (top-K).
/// - `total_change` is the L1 sum across all numeric deltas.
pub fn evaluate_coverage(
    contributions_desc: &[f64],
    total_change: f64,
    threshold: f64,
) -> CoverageDecision {
    if !total_change.is_finite() || total_change <= 0.0 {
        return CoverageDecision::NoChange;
    }

    let top_k_total: f64 = contributions_desc.iter().copied().sum();
    let top_k_coverage = top_k_total / total_change;

    if top_k_coverage < threshold {
        return CoverageDecision::Diffuse { top_k_coverage };
    }

    let mut cumulative = 0.0;
    for (idx, contribution) in contributions_desc.iter().enumerate() {
        cumulative += contribution;
        let coverage = cumulative / total_change;
        if coverage >= threshold {
            return CoverageDecision::Explainable {
                cutoff: idx + 1,
                coverage,
            };
        }
    }

    CoverageDecision::Explainable {
        cutoff: contributions_desc.len(),
        coverage: top_k_coverage,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_change_when_total_is_zero() {
        let decision = evaluate_coverage(&[1.0, 2.0], 0.0, 0.95);
        assert_eq!(decision, CoverageDecision::NoChange);
    }

    #[test]
    fn diffuse_when_top_k_below_threshold() {
        let decision = evaluate_coverage(&[5.0, 3.0], 10.0, 0.95);
        assert_eq!(
            decision,
            CoverageDecision::Diffuse {
                top_k_coverage: 0.8
            }
        );
    }

    #[test]
    fn explainable_returns_smallest_prefix() {
        let decision = evaluate_coverage(&[6.0, 3.0, 1.0], 10.0, 0.9);
        assert_eq!(
            decision,
            CoverageDecision::Explainable {
                cutoff: 2,
                coverage: 0.9
            }
        );
    }

    #[test]
    fn explainable_when_top_k_reaches_threshold() {
        let decision = evaluate_coverage(&[5.0, 3.0, 2.0], 10.0, 0.95);
        assert_eq!(
            decision,
            CoverageDecision::Explainable {
                cutoff: 3,
                coverage: 1.0
            }
        );
    }
}
