// Tolerance application & max_abs_delta tracking (bd-tvf)

/// Tracks tolerance and the maximum absolute delta observed (pre-zeroing).
#[derive(Debug, Clone, Copy)]
pub struct ToleranceTracker {
    tolerance: f64,
    max_abs_delta: f64,
}

impl ToleranceTracker {
    pub fn new(tolerance: f64) -> Self {
        Self {
            tolerance,
            max_abs_delta: 0.0,
        }
    }

    /// Returns (delta, contribution). Contribution is zeroed when within tolerance.
    #[inline]
    pub fn apply(&mut self, old: f64, new: f64) -> (f64, f64) {
        let delta = new - old;
        let abs = delta.abs();
        if abs > self.max_abs_delta {
            self.max_abs_delta = abs;
        }
        let contribution = if abs <= self.tolerance { 0.0 } else { abs };
        (delta, contribution)
    }

    #[inline]
    pub fn max_abs_delta(&self) -> f64 {
        self.max_abs_delta
    }
}

#[cfg(test)]
mod tests {
    use super::ToleranceTracker;

    #[test]
    fn zeros_within_tolerance() {
        let mut tracker = ToleranceTracker::new(1e-3);
        let (delta, contrib) = tracker.apply(1.0, 1.0005);
        assert!((delta - 0.0005).abs() < 1e-12);
        assert_eq!(contrib, 0.0);
    }

    #[test]
    fn contributes_outside_tolerance() {
        let mut tracker = ToleranceTracker::new(1e-3);
        let (_delta, contrib) = tracker.apply(1.0, 1.01);
        assert!((contrib - 0.01).abs() < 1e-12);
    }

    #[test]
    fn tracks_max_abs_delta_pre_zeroing() {
        let mut tracker = ToleranceTracker::new(1.0);
        tracker.apply(10.0, 10.5);
        tracker.apply(10.0, 8.0);
        assert!((tracker.max_abs_delta() - 2.0).abs() < 1e-12);
    }
}
