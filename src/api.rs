//! Filesystem-free comparison API.
//!
//! `compare_bytes` accepts already-loaded input bytes and returns the same rendered comparison
//! result as the command-line pipeline. The API never reads environment variables or performs
//! filesystem, witness, lock, profile, capsule, or clock I/O. Native builds may pass JSONL,
//! Parquet, or XLSX name hints; those formats are decoded through `cmdrvl-tabular` when the
//! `tabular` feature is enabled. The `--no-default-features` build intentionally supports only
//! CSV/TSV bytes so it remains suitable for wasm32.

use crate::orchestrator;
use crate::output::json::ColumnChange;
use crate::redaction::RedactionKey;
#[cfg(feature = "cli")]
use std::path::PathBuf;

/// Domain outcome produced by the comparison engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    NoRealChange,
    RealChange,
    Refusal,
}

/// Row alignment policy for an in-memory comparison.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyMode {
    #[default]
    Exact,
    Overlap,
}

/// Options shared by the CLI comparison engine and [`compare_bytes`].
#[derive(Debug, Clone)]
pub struct CompareOptions {
    /// Stable names used in the returned report and refusal guidance.
    pub old_name: String,
    pub new_name: String,
    pub key: Option<String>,
    pub key_mode: KeyMode,
    pub threshold: f64,
    pub tolerance: f64,
    pub delimiter: Option<u8>,
    pub exhaustive: bool,
    pub audit_fields: bool,
    pub non_numeric: bool,
    pub max_audit_changes: u64,
    pub json: bool,
    pub explicit: bool,
    /// A validated operator key. `None` selects deterministic ordinal redaction.
    pub redaction_key: Option<RedactionKey>,
    pub(crate) redaction_key_error: Option<&'static str>,

    #[cfg(feature = "cli")]
    pub(crate) old_path: PathBuf,
    #[cfg(feature = "cli")]
    pub(crate) new_path: PathBuf,
    #[cfg(feature = "cli")]
    pub(crate) sheet: Option<String>,
    #[cfg(feature = "cli")]
    pub(crate) old_sheet: Option<String>,
    #[cfg(feature = "cli")]
    pub(crate) new_sheet: Option<String>,
    #[cfg(feature = "cli")]
    pub(crate) profile: Option<PathBuf>,
    #[cfg(feature = "cli")]
    pub(crate) profile_id: Option<String>,
    #[cfg(feature = "cli")]
    pub(crate) lock: Vec<PathBuf>,
    #[cfg(feature = "cli")]
    pub(crate) capsule_out: Option<PathBuf>,
    #[cfg(feature = "cli")]
    pub(crate) no_witness: bool,
}

impl Default for CompareOptions {
    fn default() -> Self {
        Self {
            old_name: "old.csv".to_owned(),
            new_name: "new.csv".to_owned(),
            key: None,
            key_mode: KeyMode::Exact,
            threshold: 0.95,
            tolerance: 1e-9,
            delimiter: None,
            exhaustive: false,
            audit_fields: false,
            non_numeric: false,
            max_audit_changes: 10_000,
            json: true,
            explicit: false,
            redaction_key: None,
            redaction_key_error: None,
            #[cfg(feature = "cli")]
            old_path: PathBuf::from("old.csv"),
            #[cfg(feature = "cli")]
            new_path: PathBuf::from("new.csv"),
            #[cfg(feature = "cli")]
            sheet: None,
            #[cfg(feature = "cli")]
            old_sheet: None,
            #[cfg(feature = "cli")]
            new_sheet: None,
            #[cfg(feature = "cli")]
            profile: None,
            #[cfg(feature = "cli")]
            profile_id: None,
            #[cfg(feature = "cli")]
            lock: Vec::new(),
            #[cfg(feature = "cli")]
            capsule_out: None,
            #[cfg(feature = "cli")]
            no_witness: true,
        }
    }
}

impl CompareOptions {
    pub(crate) fn sheet_for(&self, side: crate::refusal::details::FileSide) -> Option<&str> {
        #[cfg(feature = "cli")]
        {
            let specific = match side {
                crate::refusal::details::FileSide::Old => self.old_sheet.as_deref(),
                crate::refusal::details::FileSide::New => self.new_sheet.as_deref(),
            };
            specific.or(self.sheet.as_deref())
        }
        #[cfg(not(feature = "cli"))]
        {
            let _ = side;
            None
        }
    }
}

#[cfg(feature = "cli")]
impl CompareOptions {
    pub(crate) fn from_cli(args: &crate::cli::args::Args) -> Self {
        let (redaction_key, redaction_key_error) = match RedactionKey::from_env() {
            Ok(key) => (key, None),
            Err(error) => (None, Some(error.reason())),
        };
        Self {
            old_name: args.old_path().to_string_lossy().into_owned(),
            new_name: args.new_path().to_string_lossy().into_owned(),
            key: args.key.clone(),
            key_mode: match args.key_mode {
                crate::cli::args::KeyMode::Exact => KeyMode::Exact,
                crate::cli::args::KeyMode::Overlap => KeyMode::Overlap,
            },
            threshold: args.threshold,
            tolerance: args.tolerance,
            delimiter: args.delimiter,
            exhaustive: args.exhaustive,
            audit_fields: args.audit_fields,
            non_numeric: args.non_numeric,
            max_audit_changes: args.max_audit_changes,
            json: args.json,
            explicit: args.explicit,
            redaction_key,
            redaction_key_error,
            old_path: args.old_path().clone(),
            new_path: args.new_path().clone(),
            sheet: args.sheet.clone(),
            old_sheet: args.old_sheet.clone(),
            new_sheet: args.new_sheet.clone(),
            profile: args.profile.clone(),
            profile_id: args.profile_id.clone(),
            lock: args.lock.clone(),
            capsule_out: args.capsule_out.clone(),
            no_witness: args.no_witness,
        }
    }
}

/// Result returned by [`compare_bytes`].
#[derive(Debug, Clone)]
pub struct CompareResult {
    pub outcome: Outcome,
    pub output: String,
    pub column_changes: Vec<ColumnChange>,
}

/// Compare two in-memory inputs without touching the filesystem.
pub fn compare_bytes(
    old: &[u8],
    new: &[u8],
    options: &CompareOptions,
) -> Result<CompareResult, Box<dyn std::error::Error>> {
    let result = orchestrator::run_bytes(old, new, options)?;
    Ok(CompareResult {
        outcome: result.outcome,
        output: result.output,
        column_changes: result.column_changes,
    })
}
