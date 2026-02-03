// Human REAL CHANGE body formatting (bd-3kb)

use crate::format::numbers::{
    format_delta, format_float_shortest, format_int_with_commas, format_percent_one_decimal,
};

#[derive(Debug, Clone)]
pub struct RealChangeContributor {
    pub label: String,
    pub old: f64,
    pub new: f64,
    pub delta: f64,
}

#[derive(Debug)]
pub struct RealChangeBody<'a> {
    pub contributors: &'a [RealChangeContributor],
    pub coverage: f64,
    pub threshold: f64,
}

pub fn render_real_change_body(ctx: &RealChangeBody<'_>) -> Vec<String> {
    let count = ctx.contributors.len();
    let cells_word = if count == 1 { "cell" } else { "cells" };
    let mut lines = Vec::with_capacity(count + 3);
    lines.push(format!(
        "{} {} explain {} of total numeric change (threshold {}):",
        count,
        cells_word,
        format_percent_one_decimal(ctx.coverage),
        format_percent_one_decimal(ctx.threshold)
    ));
    lines.push(String::new());
    for (idx, contributor) in ctx.contributors.iter().enumerate() {
        let delta = format_delta(contributor.delta);
        let old = format_value(contributor.old);
        let new = format_value(contributor.new);
        lines.push(format!(
            "{}. {}  {}  ({} -> {})",
            idx + 1,
            contributor.label,
            delta,
            old,
            new
        ));
    }
    lines.push(String::new());
    lines.push(
        "Everything else in common numeric columns is <= tolerance or in the tail (not required to reach threshold)."
            .to_string(),
    );
    lines
}

fn format_value(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        return format_int_with_commas(value as i64);
    }
    format_float_shortest(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_real_change_body_lines() {
        let contributors = [RealChangeContributor {
            label: "NVDA.market_value".to_string(),
            old: 123.0,
            new: 1842223.0,
            delta: 1842100.0,
        }];
        let ctx = RealChangeBody {
            contributors: &contributors,
            coverage: 0.952,
            threshold: 0.95,
        };
        let lines = render_real_change_body(&ctx);
        assert_eq!(
            lines[0],
            "1 cell explain 95.2% of total numeric change (threshold 95.0%):"
        );
        assert_eq!(lines[1], "");
        assert_eq!(
            lines[2],
            "1. NVDA.market_value  +1842100  (123 -> 1,842,223)"
        );
        assert_eq!(lines[3], "");
        assert_eq!(
            lines[4],
            "Everything else in common numeric columns is <= tolerance or in the tail (not required to reach threshold)."
        );
    }

    #[test]
    fn formats_values_with_commas_when_integer() {
        assert_eq!(format_value(0.0), "0");
        assert_eq!(format_value(12.0), "12");
        assert_eq!(format_value(1234.0), "1,234");
        assert_eq!(format_value(-1234.0), "-1,234");
        assert_eq!(format_value(12.5), "12.5");
    }
}
