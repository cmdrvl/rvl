// Human NO REAL CHANGE body formatting (bd-7eg)

use crate::format::numbers::format_float_shortest;

#[derive(Debug, Clone, Copy)]
pub struct NoRealBody {
    pub max_abs_delta: f64,
    pub tolerance: f64,
}

pub fn render_no_real_body(ctx: &NoRealBody) -> Vec<String> {
    vec![
        format!(
            "Max abs delta: {} (<= tolerance {}).",
            format_float_shortest(ctx.max_abs_delta),
            format_float_shortest(ctx.tolerance)
        ),
        "No numeric deltas above tolerance in common numeric columns.".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_no_real_body_lines() {
        let ctx = NoRealBody {
            max_abs_delta: 7e-10,
            tolerance: 1e-9,
        };
        let lines = render_no_real_body(&ctx);
        assert_eq!(lines[0], "Max abs delta: 7e-10 (<= tolerance 1e-9).");
        assert_eq!(
            lines[1],
            "No numeric deltas above tolerance in common numeric columns."
        );
    }
}
