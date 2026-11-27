#![forbid(unsafe_code)]
#![recursion_limit = "512"]

pub mod alignment;
pub mod cli;
pub mod csv;
pub mod diff;
pub mod doctor;
pub mod format;
pub mod lock;
pub mod normalize;
pub mod numeric;
pub mod orchestrator;
pub mod output;
pub(crate) mod paths;
pub mod profile;
pub mod redaction;
pub mod refusal;
pub mod repro;
pub mod tabular_input;
pub mod witness;

pub(crate) const OPERATOR_JSON: &str = include_str!("../operator.json");

/// Run the rvl pipeline. Returns exit code (0, 1, or 2).
pub fn run() -> Result<u8, Box<dyn std::error::Error>> {
    if let Some(display_mode) = detect_display_mode(std::env::args_os()) {
        return handle_display_mode(display_mode);
    }

    let args = match cli::args::Args::parse() {
        Ok(args) => args,
        Err(err) => {
            let code = match err.kind() {
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion => 0,
                _ => 2,
            };
            err.print()?;
            if let Some(candidate) = cli::typo::did_you_mean::<cli::args::Args>(&err) {
                eprintln!("{}", cli::typo::hint_line(&candidate));
            }
            return Ok(code);
        }
    };

    if args.version {
        return handle_display_mode(DisplayMode::Version);
    }

    if args.describe {
        return handle_display_mode(DisplayMode::Describe);
    }

    if args.schema {
        return handle_display_mode(DisplayMode::Schema);
    }

    if args.robot_triage {
        return doctor::emit_robot_triage();
    }

    if let Some(ref cmd) = args.command {
        return run_command(cmd, args.json);
    }

    if args.old.is_none() || args.new.is_none() {
        // One bare word that is one edit from a subcommand (`rvl witnes`) is a typo, not a file.
        let subcommand_hint = match (&args.old, &args.new) {
            (Some(word), None) => word
                .to_str()
                .and_then(cli::typo::did_you_mean_subcommand::<cli::args::Args>),
            _ => None,
        };
        eprintln!(
            "error: the following required arguments were not provided:\n  <OLD_CSV>\n  <NEW_CSV>\n\nUsage: rvl <OLD_CSV> <NEW_CSV> [OPTIONS]\n       rvl capabilities --json\n       rvl robot-docs guide\n       rvl witness <query|last|count> [OPTIONS]\n       rvl doctor [health|capabilities|triage|robot-docs] [--json]\n\nFor more information, try '--help'."
        );
        if let Some(candidate) = subcommand_hint {
            eprintln!("{}", cli::typo::hint_line(&candidate));
        }
        return Ok(2);
    }

    run_comparison(args)
}

fn run_command(
    cmd: &cli::args::RvlCommand,
    json_output: bool,
) -> Result<u8, Box<dyn std::error::Error>> {
    match cmd {
        cli::args::RvlCommand::Witness { action } => run_witness(action),
        cli::args::RvlCommand::Capabilities(args) => {
            doctor::emit_capabilities(args.json || json_output)
        }
        cli::args::RvlCommand::RobotDocs { action } => doctor::emit_robot_docs(action.as_ref()),
        cli::args::RvlCommand::Doctor(args) => doctor::run(args, json_output),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DisplayMode {
    Version,
    Describe,
    Schema,
}

fn detect_display_mode<I, T>(args: I) -> Option<DisplayMode>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString>,
{
    let args = args
        .into_iter()
        .map(Into::into)
        .collect::<Vec<std::ffi::OsString>>();

    if args.iter().skip(1).any(|arg| arg == "--version") {
        Some(DisplayMode::Version)
    } else if args.iter().skip(1).any(|arg| arg == "--describe") {
        Some(DisplayMode::Describe)
    } else if args.iter().skip(1).any(|arg| arg == "--schema") {
        Some(DisplayMode::Schema)
    } else {
        None
    }
}

fn handle_display_mode(mode: DisplayMode) -> Result<u8, Box<dyn std::error::Error>> {
    match mode {
        DisplayMode::Version => {
            println!("rvl {}", env!("CARGO_PKG_VERSION"));
            Ok(0)
        }
        DisplayMode::Describe => {
            println!("{OPERATOR_JSON}");
            Ok(0)
        }
        DisplayMode::Schema => {
            let schema = serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "https://rvl.v0/schema.json",
                "title": "rvl Output Schema",
                "description": "JSON schema for rvl.v0 output format",
                "$defs": {
                    "key_samples": {
                        "type": "array", "maxItems": 10,
                        "items": { "type": "array", "minItems": 1, "items": { "type": "string" } }
                    }
                },
                "type": "object",
                "required": ["version", "outcome", "files", "alignment", "dialect", "threshold", "tolerance", "counts", "metrics", "limits", "contributors", "column_changes"],
                "properties": {
                    "version": { "type": "string", "const": "rvl.v0" },
                    "outcome": { "type": "string", "enum": ["REAL_CHANGE", "NO_REAL_CHANGE", "REFUSAL"] },
                    "mode": { "type": "string", "enum": ["exhaustive_numeric", "non_numeric"] },
                    "profile_id": { "type": ["string", "null"] },
                    "profile_sha256": { "type": ["string", "null"] },
                    "lock": {
                        "type": "array",
                        "description": "Present only when --lock verified the inputs: the lockfiles that vouched for them.",
                        "items": {
                            "type": "object",
                            "required": ["lockfile", "lock_hash"],
                            "properties": {
                                "lockfile": { "type": "string" },
                                "lock_hash": { "type": ["string", "null"] }
                            },
                            "additionalProperties": false
                        }
                    },
                    "files": {
                        "type": "object",
                        "properties": {
                            "old": { "type": "string" },
                            "new": { "type": "string" }
                        },
                        "required": ["old", "new"]
                    },
                    "alignment": {
                        "type": "object",
                        "properties": {
                            "mode": { "type": "string", "enum": ["key", "row_order"] },
                            "key_column": { "type": ["string", "null"] },
                            "key_columns": {
                                "type": "array",
                                "items": { "type": "string" }
                            },
                            "key_membership": {
                                "type": "object",
                                "description": "Present only in overlap mode. Verdict and metrics cover continuing rows only. Samples contain at most ten byte-sorted encoded key tuples and are omitted unless --explicit is active.",
                                "properties": {
                                    "verdict_scope": { "const": "continuing_rows" },
                                    "overlap_count": { "type": "integer", "minimum": 0 },
                                    "old_only_count": { "type": "integer", "minimum": 0 },
                                    "new_only_count": { "type": "integer", "minimum": 0 },
                                    "old_only_samples": { "$ref": "#/$defs/key_samples" },
                                    "new_only_samples": { "$ref": "#/$defs/key_samples" }
                                },
                                "required": ["verdict_scope", "overlap_count", "old_only_count", "new_only_count"],
                                "additionalProperties": false
                            }
                        },
                        "required": ["mode", "key_column", "key_columns"]
                    },
                    "dialect": {
                        "type": "object",
                        "properties": {
                            "old": { "type": ["object", "null"] },
                            "new": { "type": ["object", "null"] }
                        }
                    },
                    "threshold": { "type": "number" },
                    "tolerance": { "type": "number" },
                    "counts": { "type": "object" },
                    "metrics": { "type": "object" },
                    "audit": {
                        "type": "object",
                        "properties": {
                            "numeric_changes_emitted": { "type": "integer" },
                            "field_changes_emitted": { "type": "integer" },
                            "truncated": { "type": "boolean" }
                        },
                        "required": ["numeric_changes_emitted", "field_changes_emitted", "truncated"]
                    },
                    "limits": {
                        "type": "object",
                        "properties": {
                            "max_contributors": { "type": "integer" }
                        },
                        "required": ["max_contributors"]
                    },
                    "contributors": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "row_id": { "type": "string" },
                                "row_key": {
                                    "type": "array",
                                    "minItems": 1,
                                    "items": { "type": "string" }
                                },
                                "column": { "type": "string" },
                                "old": { "type": "number" },
                                "new": { "type": "number" },
                                "delta": { "type": "number" },
                                "contribution": { "type": "number" },
                                "share": { "type": "number" },
                                "cumulative_share": { "type": "number" }
                            },
                            "required": ["row_id", "column", "share", "cumulative_share"]
                        }
                    },
                    "column_changes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": { "type": "string" },
                                "changed_cells": { "type": "integer", "minimum": 0 },
                                "max_abs_delta": { "type": "number" }
                            },
                            "required": ["column", "changed_cells"]
                        }
                    },
                    "field_changes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "row_id": { "type": "string" },
                                "row_key": {
                                    "type": "array",
                                    "minItems": 1,
                                    "items": { "type": "string" }
                                },
                                "column": { "type": "string" },
                                "old": { "type": "string" },
                                "new": { "type": "string" }
                            },
                            "required": ["row_id", "column"]
                        }
                    },
                    "non_numeric": {
                        "type": "object",
                        "properties": {
                            "date_shifts": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "column": { "type": "string" },
                                        "delta_days": { "type": "integer" },
                                        "changed_rows": { "type": "integer", "minimum": 0 }
                                    },
                                    "required": ["column", "delta_days", "changed_rows"],
                                    "additionalProperties": false
                                }
                            },
                            "categorical_transitions": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "column": { "type": "string" },
                                        "old": { "type": "string" },
                                        "new": { "type": "string" },
                                        "count": { "type": "integer", "minimum": 0 },
                                        "affected_rows": { "type": "array", "items": { "type": "string" } }
                                    },
                                    "required": ["column", "old", "new", "count", "affected_rows"],
                                    "additionalProperties": false
                                }
                            },
                            "text_churn": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "column": { "type": "string" },
                                        "changed_rows": { "type": "integer", "minimum": 0 },
                                        "byte_length_delta": { "type": "integer" }
                                    },
                                    "required": ["column", "changed_rows", "byte_length_delta"],
                                    "additionalProperties": false
                                }
                            },
                            "nullity_flips": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "column": { "type": "string" },
                                        "became_null": { "type": "integer", "minimum": 0 },
                                        "became_non_null": { "type": "integer", "minimum": 0 }
                                    },
                                    "required": ["column", "became_null", "became_non_null"],
                                    "additionalProperties": false
                                }
                            },
                            "row_churn": {
                                "type": ["object", "null"],
                                "properties": {
                                    "removed": { "type": "integer", "minimum": 0 },
                                    "added": { "type": "integer", "minimum": 0 }
                                },
                                "required": ["removed", "added"],
                                "additionalProperties": false
                            }
                        },
                        "required": ["date_shifts", "categorical_transitions", "text_churn", "nullity_flips"],
                        "additionalProperties": false
                    },
                    "refusal": {
                        "type": ["object", "null"],
                        "properties": {
                            "code": { "type": "string" },
                            "message": { "type": "string" },
                            "next_command": { "type": "string", "description": "Runnable overlap rerun for E_KEY_MISMATCH; preserves profile and comparison settings." },
                            "detail": {}
                        },
                        "required": ["code", "message", "detail"]
                    }
                }
            });
            println!("{}", serde_json::to_string_pretty(&schema)?);
            Ok(0)
        }
    }
}

/// Run the CSV comparison pipeline (the default mode).
fn run_comparison(args: cli::args::Args) -> Result<u8, Box<dyn std::error::Error>> {
    use std::io::{self, Write};

    let result = orchestrator::run(&args)?;
    let mode = if args.json {
        cli::exit::OutputMode::Json
    } else {
        cli::exit::OutputMode::Human
    };
    let stream = cli::exit::output_stream(result.outcome, mode);

    match stream {
        cli::exit::OutputStream::Stdout => {
            let mut stdout = io::stdout();
            stdout.write_all(result.output.as_bytes())?;
            if args.json && !result.output.ends_with('\n') {
                stdout.write_all(b"\n")?;
            }
            stdout.flush()?;
        }
        cli::exit::OutputStream::Stderr => {
            let mut stderr = io::stderr();
            stderr.write_all(result.output.as_bytes())?;
            if args.json && !result.output.ends_with('\n') {
                stderr.write_all(b"\n")?;
            }
            stderr.flush()?;
        }
    }

    if !args.no_witness {
        witness::record_run(&args, &result);
    }

    Ok(cli::exit::process_exit_code(
        result.outcome,
        args.exit_zero_on_change,
    ))
}

/// Run witness subcommand (query/last/count).
/// Exit codes: 0 = success (including an empty or absent ledger), 2 = error (including an
/// unreadable ledger).
fn run_witness(action: &cli::args::WitnessAction) -> Result<u8, Box<dyn std::error::Error>> {
    use std::io::{self, Write};

    let reader = witness::reader::LedgerReader::open()?;

    match action {
        cli::args::WitnessAction::Last(last_args) => {
            let record = reader.last_record().map_err(witness_read_error)?;
            match record {
                Some(rec) => {
                    let output = if last_args.json {
                        witness::query::format_record_json(&rec)
                    } else {
                        witness::query::format_record_human(&rec)
                    };
                    let mut stdout = io::stdout();
                    stdout.write_all(output.as_bytes())?;
                    stdout.write_all(b"\n")?;
                    stdout.flush()?;
                    Ok(0)
                }
                None => {
                    // An empty or absent ledger is a valid read that found nothing: exit 0
                    // with `null` (JSON), like query's `[]` and count's `{"count":0}`.
                    if last_args.json {
                        let mut stdout = io::stdout();
                        stdout.write_all(b"null\n")?;
                        stdout.flush()?;
                    }
                    eprintln!("rvl: witness ledger is empty");
                    Ok(0)
                }
            }
        }
        cli::args::WitnessAction::Query(query_args) => {
            let filter = build_filter(query_args);
            let all_records = reader.records().map_err(witness_read_error)?;
            let matched: Vec<_> = all_records
                .into_iter()
                .filter(|r| filter.matches(r))
                .take(filter.limit)
                .collect();

            let output = if query_args.json {
                witness::query::format_records_json(&matched)
            } else {
                witness::query::format_records_human(&matched)
            };
            let mut stdout = io::stdout();
            stdout.write_all(output.as_bytes())?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;
            Ok(0)
        }
        cli::args::WitnessAction::Count(query_args) => {
            let filter = build_filter(query_args);
            let all_records = reader.records().map_err(witness_read_error)?;
            let count = all_records.iter().filter(|r| filter.matches(r)).count();

            let output = if query_args.json {
                witness::query::format_count_json(count)
            } else {
                witness::query::format_count_human(count)
            };
            let mut stdout = io::stdout();
            stdout.write_all(output.as_bytes())?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;
            Ok(0)
        }
    }
}

fn witness_read_error(error: std::io::Error) -> String {
    format!("witness: failed to read ledger {error}")
}

fn build_filter(args: &cli::args::WitnessQueryArgs) -> witness::query::QueryFilter {
    witness::query::QueryFilter {
        tool: args.tool.clone(),
        since: args.since.clone(),
        until: args.until.clone(),
        outcome: args.outcome.clone(),
        input_hash: args.input_hash.clone(),
        limit: args.limit,
    }
}
