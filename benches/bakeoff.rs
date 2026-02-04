// Opt-in parser bakeoff harness. Run with: cargo bench --bench bakeoff
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_csv::reader::ReaderBuilder as ArrowReaderBuilder;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use csv::ByteRecord as CsvByteRecord;
use polars::prelude::{CsvParseOptions, CsvReadOptions, SerReader};
use simd_csv::{ByteRecord as SimdByteRecord, ReaderBuilder as SimdReaderBuilder};

use rvl::cli::delimiter::parse_delimiter_arg;
use rvl::csv::blank::is_blank_record;
use rvl::csv::dialect::auto_detect;
use rvl::csv::input::guard_input_bytes;
use rvl::csv::parser::{EscapeMode, build_reader, detect_escape_mode};
use rvl::csv::sep::{SepScan, scan_first_non_blank_line};
use rvl::normalize::trim::ascii_trim;

struct Case {
    name: String,
    path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
enum ParserKind {
    Csv,
    SimdCsv,
    Arrow,
    Polars,
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
        row_count = parse_count(input, &case.path, forced_delimiter, parser);
        row_count?;
    }

    let mut total = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        row_count = parse_count(input, &case.path, forced_delimiter, parser);
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

fn parse_count(
    input: &[u8],
    path: &Path,
    forced_delimiter: Option<u8>,
    parser: ParserKind,
) -> Option<u64> {
    let (delimiter, escape, skip_sep) = choose_dialect(input, forced_delimiter)?;

    match parser {
        ParserKind::Csv => parse_count_csv(input, delimiter, escape, skip_sep),
        ParserKind::SimdCsv => parse_count_simd(input, delimiter, escape, skip_sep),
        ParserKind::Arrow => parse_count_arrow(input, delimiter, escape, skip_sep),
        ParserKind::Polars => parse_count_polars(input, path, delimiter, escape, skip_sep),
    }
}

fn parse_count_csv(input: &[u8], delimiter: u8, escape: EscapeMode, skip_sep: bool) -> Option<u64> {
    let mut reader = build_reader(Cursor::new(input), delimiter, escape);
    let mut record = CsvByteRecord::new();
    let mut count = 0u64;
    let mut skipped_sep = !skip_sep;
    let mut pre_header = true;

    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {
                if pre_header {
                    if record.len() == 1 && is_blank_record(&record) {
                        continue;
                    }
                    if !skipped_sep {
                        skipped_sep = true;
                        continue;
                    }
                    pre_header = false;
                } else if is_blank_record(&record) {
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
    let mut pre_header = true;

    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {
                if pre_header {
                    if record.len() == 1 && is_blank_record_simd(&record) {
                        continue;
                    }
                    if !skipped_sep {
                        skipped_sep = true;
                        continue;
                    }
                    pre_header = false;
                } else if is_blank_record_simd(&record) {
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

fn parse_count_arrow(
    input: &[u8],
    delimiter: u8,
    escape: EscapeMode,
    skip_sep: bool,
) -> Option<u64> {
    let input = slice_after_preface(input, skip_sep);
    let header = read_header_record(input, delimiter, escape)?;
    let schema = schema_from_header(&header);

    let mut builder = ArrowReaderBuilder::new(schema)
        .with_delimiter(delimiter)
        .with_header(true)
        .with_quote(b'"')
        .with_truncated_rows(true);
    if let Some(escape_byte) = escape.escape_byte() {
        builder = builder.with_escape(escape_byte);
    }
    let reader = builder.build(Cursor::new(input)).ok()?;
    let mut count = 0u64;
    for batch in reader {
        let batch = batch.ok()?;
        count += batch.num_rows() as u64;
    }
    Some(count + 1)
}

fn parse_count_polars(
    input: &[u8],
    path: &Path,
    delimiter: u8,
    escape: EscapeMode,
    skip_sep: bool,
) -> Option<u64> {
    if matches!(escape, EscapeMode::Backslash) {
        return None;
    }

    let header = read_header_record(slice_after_preface(input, skip_sep), delimiter, escape)?;
    let skip_lines = count_skip_lines(input, skip_sep);
    let parse_options = CsvParseOptions::default()
        .with_separator(delimiter)
        .with_quote_char(Some(b'"'));

    let reader = CsvReadOptions::default()
        .with_has_header(true)
        .with_skip_lines(skip_lines)
        .with_parse_options(parse_options)
        .try_into_reader_with_file_path(Some(path.to_path_buf()))
        .ok()?;
    let df = reader.finish().ok()?;
    Some(df.height() as u64 + header_count(&header))
}

fn choose_dialect(input: &[u8], forced_delimiter: Option<u8>) -> Option<(u8, EscapeMode, bool)> {
    let mut skip_sep = false;
    let mut sep_delimiter = None;
    match scan_first_non_blank_line(input.split(|byte| *byte == b'\n')) {
        SepScan::Directive { delimiter, .. } => {
            sep_delimiter = Some(delimiter);
            skip_sep = true;
        }
        SepScan::FirstNonBlank { .. } | SepScan::NoLines => {}
    }

    if let Some(forced) = forced_delimiter {
        let mut cursor = Cursor::new(input);
        let escape = detect_escape_mode(&mut cursor, forced).ok()?;
        return Some((forced, escape, skip_sep));
    }

    if let Some(delimiter) = sep_delimiter {
        let mut cursor = Cursor::new(input);
        let escape = detect_escape_mode(&mut cursor, delimiter).ok()?;
        return Some((delimiter, escape, skip_sep));
    }

    let dialect = auto_detect(input).ok()?;
    Some((dialect.delimiter, dialect.escape, false))
}

fn is_blank_record_simd(record: &SimdByteRecord) -> bool {
    if record.is_empty() {
        return true;
    }
    record.iter().all(|field| ascii_trim(field).is_empty())
}

fn slice_after_preface(input: &[u8], skip_sep: bool) -> &[u8] {
    let mut offset = 0usize;
    let mut skipped_sep = false;
    for raw_line in input.split_inclusive(|byte| *byte == b'\n') {
        let mut line = raw_line;
        if line.ends_with(b"\n") {
            line = &line[..line.len() - 1];
        }
        if line.ends_with(b"\r") {
            line = &line[..line.len() - 1];
        }
        if ascii_trim(line).is_empty() {
            offset += raw_line.len();
            continue;
        }
        if skip_sep && !skipped_sep {
            skipped_sep = true;
            offset += raw_line.len();
            continue;
        }
        return &input[offset..];
    }
    &input[input.len()..]
}

fn count_skip_lines(input: &[u8], skip_sep: bool) -> usize {
    let mut skipped = 0usize;
    let mut skipped_sep = false;
    for raw_line in input.split_inclusive(|byte| *byte == b'\n') {
        let mut line = raw_line;
        if line.ends_with(b"\n") {
            line = &line[..line.len() - 1];
        }
        if line.ends_with(b"\r") {
            line = &line[..line.len() - 1];
        }
        if ascii_trim(line).is_empty() {
            skipped += 1;
            continue;
        }
        if skip_sep && !skipped_sep {
            skipped += 1;
            skipped_sep = true;
            continue;
        }
        break;
    }
    skipped
}

fn read_header_record(input: &[u8], delimiter: u8, escape: EscapeMode) -> Option<Vec<Vec<u8>>> {
    let mut reader = build_reader(Cursor::new(input), delimiter, escape);
    let mut record = CsvByteRecord::new();

    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {
                if record.len() == 1 && is_blank_record(&record) {
                    continue;
                }
                return Some(record.iter().map(|field| field.to_vec()).collect());
            }
            Ok(false) => return None,
            Err(_) => return None,
        }
    }
}

fn schema_from_header(header: &[Vec<u8>]) -> SchemaRef {
    let mut fields = Vec::with_capacity(header.len());
    for (idx, name) in header.iter().enumerate() {
        let trimmed = ascii_trim(name);
        let field_name = if trimmed.is_empty() {
            format!("__rvl_col_{}", idx + 1)
        } else {
            String::from_utf8_lossy(trimmed).to_string()
        };
        fields.push(Field::new(field_name, DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

fn header_count(header: &[Vec<u8>]) -> u64 {
    if header.is_empty() { 0 } else { 1 }
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
            "arrow" => Some(Self::Arrow),
            "polars" => Some(Self::Polars),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::SimdCsv => "simd_csv",
            Self::Arrow => "arrow",
            Self::Polars => "polars",
        }
    }
}
