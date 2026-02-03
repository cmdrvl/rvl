use std::path::PathBuf;

use clap::Parser;

use super::delimiter::parse_delimiter_arg;

const DEFAULT_THRESHOLD: f64 = 0.95;
const DEFAULT_TOLERANCE: f64 = 1e-9;

/// CLI argument parsing & validation (bd-l7j).
#[derive(Debug, Clone, Parser)]
#[command(
    name = "rvl",
    about = "Reveal the smallest set of numeric changes that explain what actually changed.",
    override_usage = "rvl <old.csv> <new.csv> [--key <column>] [--threshold <float>] [--tolerance <float>] [--delimiter <delim>] [--json]"
)]
pub struct Args {
    /// Old CSV path.
    #[arg(value_name = "OLD_CSV")]
    pub old: PathBuf,

    /// New CSV path.
    #[arg(value_name = "NEW_CSV")]
    pub new: PathBuf,

    /// Align rows by this key column (otherwise align by row order).
    #[arg(long, value_name = "COLUMN")]
    pub key: Option<String>,

    /// Coverage target: 0 < x <= 1 (default: 0.95).
    #[arg(
        long,
        value_name = "FLOAT",
        default_value_t = DEFAULT_THRESHOLD,
        value_parser = parse_threshold
    )]
    pub threshold: f64,

    /// Per-cell noise floor: x >= 0 (default: 1e-9).
    #[arg(
        long,
        value_name = "FLOAT",
        default_value_t = DEFAULT_TOLERANCE,
        value_parser = parse_tolerance
    )]
    pub tolerance: f64,

    /// Force a CSV delimiter (comma/tab/semicolon/pipe/caret, 0xNN, or single ASCII byte).
    #[arg(long, value_name = "DELIM", value_parser = parse_delimiter)]
    pub delimiter: Option<u8>,

    /// Emit JSON output (single object).
    #[arg(long)]
    pub json: bool,
}

impl Args {
    pub fn parse() -> Result<Self, clap::Error> {
        Self::try_parse()
    }
}

fn parse_threshold(raw: &str) -> Result<f64, String> {
    let value = parse_finite(raw, "threshold")?;
    if value <= 0.0 || value > 1.0 {
        return Err("threshold must be 0 < x <= 1".to_string());
    }
    Ok(value)
}

fn parse_tolerance(raw: &str) -> Result<f64, String> {
    let value = parse_finite(raw, "tolerance")?;
    if value < 0.0 {
        return Err("tolerance must be >= 0".to_string());
    }
    Ok(value)
}

fn parse_finite(raw: &str, label: &str) -> Result<f64, String> {
    let value = raw
        .parse::<f64>()
        .map_err(|_| format!("{label} must be a valid number"))?;
    if !value.is_finite() {
        return Err(format!("{label} must be a finite number"));
    }
    Ok(value)
}

fn parse_delimiter(raw: &str) -> Result<u8, String> {
    parse_delimiter_arg(raw).map_err(|err| err.to_string())
}
