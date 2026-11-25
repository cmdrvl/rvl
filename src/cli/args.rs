use std::path::PathBuf;

use clap::{Parser, Subcommand};

use super::delimiter::parse_delimiter_arg;

const DEFAULT_THRESHOLD: f64 = 0.95;
const DEFAULT_TOLERANCE: f64 = 1e-9;
const DEFAULT_MAX_AUDIT_CHANGES: u64 = 10_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyMode {
    #[default]
    Exact,
    Overlap,
}

/// CLI argument parsing & validation (bd-l7j).
#[derive(Debug, Clone, Parser)]
#[command(
    name = "rvl",
    about = "Reveal the smallest set of numeric changes that explain what actually changed.",
    override_usage = "rvl <old.csv> <new.csv> [OPTIONS]\n       rvl capabilities --json\n       rvl robot-docs guide\n       rvl witness <query|last|count> [OPTIONS]\n       rvl doctor [health|capabilities|triage|robot-docs] [--json]",
    after_help = "Output is redacted by default (zero-retention safe): labels, counts and shares only, no raw values.\nPass --explicit to show raw values (old/new/delta, key samples). Use it for local, non-sensitive runs; omit it\nfor logged, shared, or veil/airlock zero-retention workflows.",
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

    /// Key-set policy: exact requires equal sets; overlap compares continuing rows only.
    #[arg(long, value_enum, default_value = "exact")]
    pub key_mode: KeyMode,

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

    /// Emit every changed numeric cell above tolerance instead of the smallest explanation prefix.
    #[arg(long)]
    pub exhaustive: bool,

    /// Emit exact changes in profile-scoped non-numeric fields during exhaustive audit.
    #[arg(long)]
    pub audit_fields: bool,

    /// Maximum changed cells to emit in audit modes.
    #[arg(
        long,
        value_name = "N",
        default_value_t = DEFAULT_MAX_AUDIT_CHANGES,
        value_parser = parse_max_audit_changes
    )]
    pub max_audit_changes: u64,

    /// Use profile YAML at this path for key derivation and column scoping.
    #[arg(long, value_name = "PATH")]
    pub profile: Option<PathBuf>,

    /// Resolve profile by ID from ~/.cmdrvl/config/profile/profiles/*.yaml (or from a direct path).
    #[arg(long = "profile-id", value_name = "ID")]
    pub profile_id: Option<String>,

    /// Write deterministic repro capsule artifacts to this directory (default: disabled).
    /// `--capsule-out` is a deprecated alias.
    #[arg(long = "capsule-dir", alias = "capsule-out", value_name = "DIR")]
    pub capsule_out: Option<PathBuf>,

    /// Exit 0 instead of 1 on REAL_CHANGE, for `set -e` pipelines. Refusals still exit 2; never affects `doctor`.
    #[arg(long)]
    pub exit_zero_on_change: bool,

    /// Emit JSON output (single object).
    #[arg(long)]
    pub json: bool,

    /// Suppress witness ledger recording.
    #[arg(long)]
    pub no_witness: bool,

    /// Show raw data values in output (default: redacted for zero-retention safety).
    #[arg(long)]
    pub explicit: bool,

    /// Print compiled operator.json and exit 0.
    #[arg(long)]
    pub describe: bool,

    /// Print JSON Schema for rvl.v0 output format and exit 0.
    #[arg(long)]
    pub schema: bool,

    /// Print version and exit 0.
    #[arg(long)]
    pub version: bool,

    /// Emit one-call machine triage for headless agents.
    #[arg(long = "robot-triage")]
    pub robot_triage: bool,

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
    /// Print the machine-readable rvl capability contract.
    Capabilities(TopLevelCapabilitiesArgs),
    /// Print paste-ready operating notes for agents.
    RobotDocs {
        #[command(subcommand)]
        action: Option<RobotDocsAction>,
    },
    /// Inspect rvl's read-only diagnostic surface.
    Doctor(DoctorArgs),
}

#[derive(Debug, Clone, clap::Args)]
pub struct TopLevelCapabilitiesArgs {
    /// Emit JSON output.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Subcommand)]
pub enum RobotDocsAction {
    /// Print the agent operating guide.
    Guide,
}

#[derive(Debug, Clone, clap::Args)]
pub struct DoctorArgs {
    /// Emit one-call machine triage for headless agents.
    #[arg(long = "robot-triage")]
    pub robot_triage: bool,

    /// Emit the doctor report as JSON (cmdrvl.doctor.v1, mode health).
    #[arg(long)]
    pub json: bool,

    /// Refuse safely; repair mode is not available in this release.
    #[arg(long, hide = true)]
    pub fix: bool,

    #[command(subcommand)]
    pub action: Option<DoctorAction>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum DoctorAction {
    /// Print a cheap one-line health summary.
    Health(DoctorHealthArgs),
    /// Print the doctor report carrying the operator contract (cmdrvl.doctor.v1, mode capabilities).
    Capabilities(DoctorCapabilitiesArgs),
    /// Print the doctor report with next actions (cmdrvl.doctor.v1, mode triage).
    Triage(DoctorTriageArgs),
    /// Print paste-ready operating notes for agents.
    RobotDocs,
}

#[derive(Debug, Clone, clap::Args)]
pub struct DoctorTriageArgs {
    /// Emit JSON output.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, clap::Args)]
pub struct DoctorHealthArgs {
    /// Emit JSON output.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, clap::Args)]
pub struct DoctorCapabilitiesArgs {
    /// Emit JSON output.
    #[arg(long)]
    pub json: bool,
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
        Self::parse_from(std::env::args_os())
    }

    pub fn parse_from<I, T>(itr: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        use clap::{CommandFactory, FromArgMatches};

        let mut command = Self::command();
        if color_disabled_by_env(
            std::env::var_os("NO_COLOR").as_deref(),
            std::env::var_os("TERM").as_deref(),
        ) {
            // clap/anstream already strips ANSI for non-TTY and NO_COLOR, but a forced-color
            // variable (CLICOLOR_FORCE) would still win over TERM=dumb; make "never" explicit.
            command = command.color(clap::ColorChoice::Never);
        }
        let matches = command.try_get_matches_from(itr)?;
        Self::from_arg_matches(&matches)
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
            key_mode: KeyMode::Exact,
            threshold,
            tolerance,
            delimiter,
            exhaustive: false,
            audit_fields: false,
            max_audit_changes: DEFAULT_MAX_AUDIT_CHANGES,
            profile: None,
            profile_id: None,
            capsule_out: None,
            exit_zero_on_change: false,
            json,
            no_witness: false,
            explicit: false,
            describe: false,
            schema: false,
            version: false,
            robot_triage: false,
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

/// NO_COLOR (any non-empty value) or TERM=dumb means no ANSI styling.
pub(crate) fn color_disabled_by_env(
    no_color: Option<&std::ffi::OsStr>,
    term: Option<&std::ffi::OsStr>,
) -> bool {
    no_color.is_some_and(|value| !value.is_empty()) || term.is_some_and(|value| value == "dumb")
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

fn parse_max_audit_changes(raw: &str) -> Result<u64, String> {
    let value = raw
        .parse::<u64>()
        .map_err(|_| "max audit changes must be a positive integer".to_string())?;
    if value == 0 {
        return Err("max audit changes must be greater than zero".to_string());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::Args;

    #[test]
    fn parse_accepts_profile_flags_without_clap_conflict() {
        let args = Args::parse_from([
            "rvl",
            "old.csv",
            "new.csv",
            "--profile",
            "draft.yaml",
            "--profile-id",
            "csv.demo.v0",
        ])
        .expect("flags should parse (conflict handled in orchestrator)");

        assert_eq!(
            args.profile
                .as_ref()
                .map(|path| path.to_string_lossy().to_string()),
            Some("draft.yaml".to_string())
        );
        assert_eq!(args.profile_id.as_deref(), Some("csv.demo.v0"));
    }
}
