use std::path::PathBuf;

use clap::{Parser, Subcommand};

use super::delimiter::parse_delimiter_arg;

const DEFAULT_THRESHOLD: f64 = 0.95;
const DEFAULT_TOLERANCE: f64 = 1e-9;

/// CLI argument parsing & validation (bd-l7j).
#[derive(Debug, Clone, Parser)]
#[command(
    name = "rvl",
    about = "Reveal the smallest set of numeric changes that explain what actually changed.",
    override_usage = "rvl <old.csv> <new.csv> [OPTIONS]\n       rvl witness <query|last|count> [OPTIONS]",
    subcommand_negates_reqs = true
)]
pub struct Args {
    /// Old CSV path.
    #[arg(value_name = "OLD_CSV")]
    pub old: Option<PathBuf>,

    /// New CSV path.
    #[arg(value_name = "NEW_CSV")]
    pub new: Option<PathBuf>,

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

    /// Write deterministic repro capsule artifacts to this directory (default: disabled).
    #[arg(long, value_name = "DIR")]
    pub capsule_out: Option<PathBuf>,

    /// Emit JSON output (single object).
    #[arg(long)]
    pub json: bool,

    /// Suppress witness ledger recording.
    #[arg(long)]
    pub no_witness: bool,

    #[command(subcommand)]
    pub command: Option<RvlCommand>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum RvlCommand {
    /// Query the witness ledger.
    Witness {
        #[command(subcommand)]
        action: WitnessAction,
    },
}

#[derive(Debug, Clone, Subcommand)]
pub enum WitnessAction {
    /// Search witness records with filters.
    Query(WitnessQueryArgs),
    /// Show the most recent witness record.
    Last(WitnessLastArgs),
    /// Count matching witness records.
    Count(WitnessQueryArgs),
}

#[derive(Debug, Clone, clap::Args)]
pub struct WitnessQueryArgs {
    /// Filter by tool name.
    #[arg(long)]
    pub tool: Option<String>,

    /// Filter records on or after this ISO 8601 timestamp.
    #[arg(long)]
    pub since: Option<String>,

    /// Filter records on or before this ISO 8601 timestamp.
    #[arg(long)]
    pub until: Option<String>,

    /// Filter by outcome (REAL_CHANGE, NO_REAL_CHANGE, REFUSAL).
    #[arg(long)]
    pub outcome: Option<String>,

    /// Filter by input file hash (substring match).
    #[arg(long)]
    pub input_hash: Option<String>,

    /// Maximum number of records to return (default: 20).
    #[arg(long, default_value_t = 20)]
    pub limit: usize,

    /// Emit JSON output.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, clap::Args)]
pub struct WitnessLastArgs {
    /// Emit JSON output.
    #[arg(long)]
    pub json: bool,
}

impl Args {
    pub fn parse() -> Result<Self, clap::Error> {
        Self::try_parse()
    }

    /// Create Args directly (for API/library use).
    pub fn new(
        old: PathBuf,
        new: PathBuf,
        key: Option<String>,
        threshold: f64,
        tolerance: f64,
        delimiter: Option<u8>,
        json: bool,
    ) -> Self {
        Self {
            old: Some(old),
            new: Some(new),
            key,
            threshold,
            tolerance,
            delimiter,
            capsule_out: None,
            json,
            no_witness: false,
            command: None,
        }
    }

    /// Get the old path, panics if not set (only valid in comparison mode).
    pub fn old_path(&self) -> &PathBuf {
        self.old.as_ref().expect("old path required for comparison")
    }

    /// Get the new path, panics if not set (only valid in comparison mode).
    pub fn new_path(&self) -> &PathBuf {
        self.new.as_ref().expect("new path required for comparison")
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
