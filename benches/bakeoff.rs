// Opt-in parser bakeoff harness. Run with: cargo bench --bench bakeoff
use std::io::Cursor;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use csv::ByteRecord as CsvByteRecord;
use simd_csv::{ByteRecord as SimdByteRecord, ReaderBuilder as SimdReaderBuilder};

use rvl::cli::delimiter::parse_delimiter_arg;
use rvl::csv::dialect::auto_detect;
use rvl::csv::input::guard_input_bytes;
use rvl::csv::parser::{EscapeMode, build_reader, detect_escape_mode};
use rvl::csv::sep::{SepScan, scan_first_non_blank_line};

struct Case {
    name: String,
    path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
enum ParserKind {
    Csv,
    SimdCsv,
}

fn main() {
    let iterations = env_u64("RVL_BAKEOFF_ITERS", 5);
    let warmup = env_u64("RVL_BAKEOFF_WARMUP", 1);
    let forced_delimiter =
        env_string("RVL_BAKEOFF_DELIMITER").and_then(|raw| parse_delimiter_arg(&raw).ok());
    let parser = env_string("RVL_BAKEOFF_PARSER")
        .as_deref()
        .and_then(ParserKind::parse)
        .unwrap_or(ParserKind::Csv);

    let inputs = env_string("RVL_BAKEOFF_INPUTS")
        .map(split_inputs)
        .unwrap_or_else(default_inputs);

    println!("rvl bakeoff harness");
    println!("iters={iterations} warmup={warmup}");
    println!("parser={}", parser.label());
    if let Some(raw) = env_string("RVL_BAKEOFF_DELIMITER") {
        println!("forced_delimiter={raw}");
    }

    let mut cases = Vec::new();
    for path in inputs {
        let name = path
            .file_name()
            .map(|os| os.to_string_lossy().to_string())
            .unwrap_or_else(|| path.display().to_string());
        cases.push(Case { name, path });
    }

    for case in &cases {
        let avg_ms = run_case(case, iterations, warmup, forced_delimiter, parser);
        if let Some(avg_ms) = avg_ms {
            println!("case {}: avg_ms={avg_ms:.3}", case.name);
        } else {
            println!("case {}: skipped (parse error)", case.name);
        }
    }
}

fn run_case(
    case: &Case,
    iterations: u64,
    warmup: u64,
    forced_delimiter: Option<u8>,
    parser: ParserKind,
) -> Option<f64> {
    let bytes = std::fs::read(&case.path).ok()?;
    let input = guard_input_bytes(&bytes).ok()?;

    let mut row_count = None;
    for _ in 0..warmup {
        row_count = parse_count(input, forced_delimiter, parser);
        row_count?;
    }

    let mut total = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        row_count = parse_count(input, forced_delimiter, parser);
        row_count?;
        total += start.elapsed();
    }

    let total_ms = total.as_secs_f64() * 1000.0;
    let avg_ms = if iterations == 0 {
        0.0
    } else {
        total_ms / iterations as f64
    };

    if let Some(rows) = row_count {
        let mb = bytes.len() as f64 / 1_000_000.0;
        let secs = avg_ms / 1000.0;
        if secs > 0.0 {
            println!(
                "  rows={} rows/sec={:.1} MB/sec={:.2}",
                rows,
                rows as f64 / secs,
                mb / secs
            );
        }
    }

    Some(avg_ms)
}

fn parse_count(input: &[u8], forced_delimiter: Option<u8>, parser: ParserKind) -> Option<u64> {
    let (delimiter, escape, skip_sep) = choose_dialect(input, forced_delimiter)?;

    match parser {
        ParserKind::Csv => parse_count_csv(input, delimiter, escape, skip_sep),
        ParserKind::SimdCsv => parse_count_simd(input, delimiter, escape, skip_sep),
    }
}

fn parse_count_csv(input: &[u8], delimiter: u8, escape: EscapeMode, skip_sep: bool) -> Option<u64> {
    let mut reader = build_reader(Cursor::new(input), delimiter, escape);
    let mut record = CsvByteRecord::new();
    let mut count = 0u64;
    let mut skipped_sep = !skip_sep;

    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {
                if !skipped_sep {
                    skipped_sep = true;
                    continue;
                }
                count += 1;
            }
            Ok(false) => break,
            Err(_) => return None,
        }
    }

    Some(count)
}

fn parse_count_simd(
    input: &[u8],
    delimiter: u8,
    escape: EscapeMode,
    skip_sep: bool,
) -> Option<u64> {
    if matches!(escape, EscapeMode::Backslash) {
        return None;
    }

    let mut reader = SimdReaderBuilder::new()
        .delimiter(delimiter)
        .quote(b'"')
        .flexible(true)
        .has_headers(false)
        .from_reader(Cursor::new(input));
    let mut record = SimdByteRecord::new();
    let mut count = 0u64;
    let mut skipped_sep = !skip_sep;

    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {
                if !skipped_sep {
                    skipped_sep = true;
                    continue;
                }
                count += 1;
            }
            Ok(false) => break,
            Err(_) => return None,
        }
    }

    Some(count)
}

fn choose_dialect(input: &[u8], forced_delimiter: Option<u8>) -> Option<(u8, EscapeMode, bool)> {
    if let Some(forced) = forced_delimiter {
        let mut cursor = Cursor::new(input);
        let escape = detect_escape_mode(&mut cursor, forced).ok()?;
        return Some((forced, escape, false));
    }

    let mut skip_sep = false;
    let mut sep_delimiter = None;
    match scan_first_non_blank_line(input.split(|byte| *byte == b'\n')) {
        SepScan::Directive { delimiter, .. } => {
            sep_delimiter = Some(delimiter);
            skip_sep = true;
        }
        SepScan::FirstNonBlank { .. } | SepScan::NoLines => {}
    }

    if let Some(delimiter) = sep_delimiter {
        let mut cursor = Cursor::new(input);
        let escape = detect_escape_mode(&mut cursor, delimiter).ok()?;
        return Some((delimiter, escape, skip_sep));
    }

    let dialect = auto_detect(input).ok()?;
    Some((dialect.delimiter, dialect.escape, false))
}

fn default_inputs() -> Vec<PathBuf> {
    vec![
        PathBuf::from("tests/fixtures/corpus/basic_old.csv"),
        PathBuf::from("tests/fixtures/corpus/basic_new.csv"),
    ]
}

fn split_inputs(raw: String) -> Vec<PathBuf> {
    raw.split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(PathBuf::from)
        .collect()
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

impl ParserKind {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "csv" => Some(Self::Csv),
            "simd_csv" | "simd-csv" => Some(Self::SimdCsv),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::SimdCsv => "simd_csv",
        }
    }
}
