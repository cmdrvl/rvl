#![forbid(unsafe_code)]

pub mod alignment;
pub mod cli;
pub mod csv;
pub mod diff;
pub mod format;
pub mod normalize;
pub mod numeric;
pub mod orchestrator;
pub mod output;
pub mod profile;
pub mod refusal;
pub mod repro;
pub mod witness;

const OPERATOR_JSON: &str = include_str!("../operator.json");

/// Run the rvl pipeline. Returns exit code (0, 1, or 2).
pub fn run() -> Result<u8, Box<dyn std::error::Error>> {
    let args = match cli::args::Args::parse() {
        Ok(args) => args,
        Err(err) => {
            err.print()?;
            return Ok(2);
        }
    };

    if args.describe {
        println!("{OPERATOR_JSON}");
        return Ok(0);
    }

    if let Some(ref cmd) = args.command {
        return run_witness(cmd);
    }

    if args.old.is_none() || args.new.is_none() {
        eprintln!(
            "error: the following required arguments were not provided:\n  <OLD_CSV>\n  <NEW_CSV>\n\nUsage: rvl <OLD_CSV> <NEW_CSV> [OPTIONS]\n       rvl witness <query|last|count> [OPTIONS]\n\nFor more information, try '--help'."
        );
        return Ok(2);
    }

    run_comparison(args)
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
            stdout.flush()?;
        }
        cli::exit::OutputStream::Stderr => {
            let mut stderr = io::stderr();
            stderr.write_all(result.output.as_bytes())?;
            stderr.flush()?;
        }
    }

    if !args.no_witness {
        witness::record_run(&args, &result);
    }

    Ok(cli::exit::exit_code(result.outcome))
}

/// Run witness subcommand (query/last/count).
/// Exit codes: 0 = success, 1 = no record for `last`, 2 = error.
fn run_witness(cmd: &cli::args::RvlCommand) -> Result<u8, Box<dyn std::error::Error>> {
    use std::io::{self, Write};

    let cli::args::RvlCommand::Witness { action } = cmd;

    let reader = witness::reader::LedgerReader::open()?;

    match action {
        cli::args::WitnessAction::Last(last_args) => {
            let record = reader.last_record();
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
                    eprintln!("rvl: witness ledger is empty");
                    Ok(1)
                }
            }
        }
        cli::args::WitnessAction::Query(query_args) => {
            let filter = build_filter(query_args);
            let all_records = reader.records();
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
            let all_records = reader.records();
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
