//! Delimiter auto-detection scoring & tie-break (bd-6ms).

use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Cursor;

use csv::ByteRecord;

use crate::csv::blank::{is_blank_line, is_blank_record};
use crate::csv::parser::{CsvParseError, EscapeMode, build_reader, validate_quotes};
use crate::normalize::trim::is_ascii_blank_slice;

/// Candidate delimiters for auto-detection (in priority order).
pub const CANDIDATE_DELIMITERS: [u8; 5] = [b',', b'\t', b';', b'|', b'^'];

const MAX_DATA_RECORDS: usize = 200;
const MAX_SAMPLE_BYTES: u64 = 64 * 1024;

/// Scoring tuple for delimiter candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DialectScore {
    pub records_parsed: u64,
    pub mode_count: u64,
    pub mode_fields: usize,
}

impl DialectScore {
    fn cmp_tuple(self, other: Self) -> Ordering {
        (self.records_parsed, self.mode_count, self.mode_fields).cmp(&(
            other.records_parsed,
            other.mode_count,
            other.mode_fields,
        ))
    }
}

/// Detected CSV dialect (delimiter + escape mode).
#[derive(Debug, Clone)]
pub struct Dialect {
    pub delimiter: u8,
    pub quote: u8,
    pub escape: EscapeMode,
    pub header_fields: usize,
    pub score: DialectScore,
}

/// Auto-detection failures.
#[derive(Debug)]
pub enum DialectError {
    /// Could not parse a header under any candidate delimiter.
    CsvParse { error: Option<CsvParseError> },
    /// Ambiguous delimiter choice (tied scores with differing samples).
    Ambiguous { tied: Vec<u8> },
    /// Auto-detect produced a single-column header (guardrail refusal).
    SingleColumn { delimiter: u8 },
    /// No header line found after leading blank lines.
    NoHeader,
}

type NormalizedRecord = Vec<Vec<u8>>;

#[derive(Debug)]
struct SampleParse {
    escape: EscapeMode,
    header_fields: usize,
    score: DialectScore,
    records: Vec<NormalizedRecord>,
    error: Option<CsvParseError>,
}

#[derive(Debug, Clone)]
struct CandidateSample {
    delimiter: u8,
    escape: EscapeMode,
    header_fields: usize,
    score: DialectScore,
    records: Vec<NormalizedRecord>,
}

/// Auto-detect the delimiter and escape mode for a CSV input.
pub fn auto_detect(input: &[u8]) -> Result<Dialect, DialectError> {
    let trimmed = skip_leading_blank_lines(input);
    if trimmed.is_empty() {
        return Err(DialectError::NoHeader);
    }

    let mut candidates = Vec::new();
    let mut first_error: Option<CsvParseError> = None;

    for delimiter in CANDIDATE_DELIMITERS {
        if let Some(sample) = score_delimiter(trimmed, delimiter, &mut first_error) {
            candidates.push(sample);
        }
    }

    if candidates.is_empty() {
        return Err(DialectError::CsvParse { error: first_error });
    }

    let best_score = candidates
        .iter()
        .map(|candidate| candidate.score)
        .max_by(|left, right| left.cmp_tuple(*right))
        .unwrap();

    let mut tied: Vec<&CandidateSample> = candidates
        .iter()
        .filter(|candidate| candidate.score == best_score)
        .collect();

    let chosen = if tied.len() == 1 {
        tied[0]
    } else if samples_identical(&tied) {
        tied.sort_by_key(|candidate| delimiter_rank(candidate.delimiter));
        tied[0]
    } else {
        let mut tied_delimiters: Vec<u8> = tied.iter().map(|c| c.delimiter).collect();
        tied_delimiters.sort_by_key(|delimiter| delimiter_rank(*delimiter));
        return Err(DialectError::Ambiguous {
            tied: tied_delimiters,
        });
    };

    if chosen.header_fields == 1 {
        return Err(DialectError::SingleColumn {
            delimiter: chosen.delimiter,
        });
    }

    Ok(Dialect {
        delimiter: chosen.delimiter,
        quote: b'"',
        escape: chosen.escape,
        header_fields: chosen.header_fields,
        score: chosen.score,
    })
}

fn score_delimiter(
    input: &[u8],
    delimiter: u8,
    first_error: &mut Option<CsvParseError>,
) -> Option<CandidateSample> {
    let mut rfc = sample_with_escape(input, delimiter, EscapeMode::None);
    let rfc_failed = rfc.error.is_some();
    if let Some(err) = rfc.error.take()
        && first_error.is_none()
    {
        *first_error = Some(err);
    }

    let chosen = if rfc_failed {
        let mut backslash = sample_with_escape(input, delimiter, EscapeMode::Backslash);
        if let Some(err) = backslash.error.take()
            && first_error.is_none()
        {
            *first_error = Some(err);
        }
        choose_best(rfc, backslash)
    } else {
        rfc
    };

    if chosen.score.records_parsed == 0 {
        return None;
    }

    Some(CandidateSample {
        delimiter,
        escape: chosen.escape,
        header_fields: chosen.header_fields,
        score: chosen.score,
        records: chosen.records,
    })
}

fn choose_best(left: SampleParse, right: SampleParse) -> SampleParse {
    if right.score.cmp_tuple(left.score) == Ordering::Greater {
        right
    } else {
        left
    }
}

fn sample_with_escape(input: &[u8], delimiter: u8, escape: EscapeMode) -> SampleParse {
    if let Err(err) = validate_quotes(input, delimiter, escape) {
        return SampleParse {
            escape,
            header_fields: 0,
            score: DialectScore {
                records_parsed: 0,
                mode_count: 0,
                mode_fields: 0,
            },
            records: Vec::new(),
            error: Some(CsvParseError::new(err, escape)),
        };
    }

    let mut reader = build_reader(Cursor::new(input), delimiter, escape);
    let mut record = ByteRecord::new();
    let mut header_fields = 0;
    let mut data_records = 0usize;
    let mut records_parsed = 0u64;
    let mut histogram: HashMap<usize, u64> = HashMap::new();
    let mut records: Vec<NormalizedRecord> = Vec::new();
    let mut error: Option<CsvParseError> = None;
    let mut seen_header = false;

    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {
                if !seen_header {
                    seen_header = true;
                    header_fields = record.len();
                    records_parsed += 1;
                    push_histogram(
                        &mut histogram,
                        effective_field_count(&record, header_fields),
                    );
                    records.push(normalize_record_for_compare(&record, header_fields));
                } else if !is_blank_record(&record) {
                    data_records += 1;
                    if data_records > MAX_DATA_RECORDS {
                        break;
                    }
                    records_parsed += 1;
                    push_histogram(
                        &mut histogram,
                        effective_field_count(&record, header_fields),
                    );
                    records.push(normalize_record_for_compare(&record, header_fields));
                }

                if reader.position().byte() >= MAX_SAMPLE_BYTES {
                    break;
                }
            }
            Ok(false) => break,
            Err(err) => {
                error = Some(build_parse_error(err, escape));
                break;
            }
        }
    }

    let (mode_count, mode_fields) = compute_mode(&histogram);

    SampleParse {
        escape,
        header_fields,
        score: DialectScore {
            records_parsed,
            mode_count,
            mode_fields,
        },
        records,
        error,
    }
}

fn build_parse_error(err: csv::Error, escape: EscapeMode) -> CsvParseError {
    let (record, line) = err
        .position()
        .map(|pos| (Some(pos.record()), Some(pos.line())))
        .unwrap_or((None, None));
    CsvParseError {
        escape_mode: escape,
        record,
        line,
        source: err,
    }
}

fn push_histogram(histogram: &mut HashMap<usize, u64>, fields: usize) {
    let entry = histogram.entry(fields).or_insert(0);
    *entry += 1;
}

fn compute_mode(histogram: &HashMap<usize, u64>) -> (u64, usize) {
    let mut mode_count = 0u64;
    let mut mode_fields = 0usize;
    for (&fields, &count) in histogram {
        if count > mode_count || (count == mode_count && fields > mode_fields) {
            mode_count = count;
            mode_fields = fields;
        }
    }
    (mode_count, mode_fields)
}

fn effective_field_count(record: &ByteRecord, header_fields: usize) -> usize {
    if record.len() < header_fields {
        return header_fields;
    }
    if record.len() == header_fields {
        return header_fields;
    }
    let mut all_blank = true;
    for field in record.iter().skip(header_fields) {
        if !is_ascii_blank_slice(field) {
            all_blank = false;
            break;
        }
    }
    if all_blank {
        header_fields
    } else {
        record.len()
    }
}

fn normalize_record_for_compare(record: &ByteRecord, header_fields: usize) -> NormalizedRecord {
    let mut normalized: NormalizedRecord = record.iter().map(|field| field.to_vec()).collect();
    if normalized.len() < header_fields {
        normalized.resize_with(header_fields, Vec::new);
        return normalized;
    }
    if normalized.len() > header_fields {
        while normalized.len() > header_fields {
            if let Some(last) = normalized.last() {
                if is_ascii_blank_slice(last) {
                    normalized.pop();
                } else {
                    break;
                }
            }
        }
    }
    normalized
}

fn samples_identical(candidates: &[&CandidateSample]) -> bool {
    if candidates.is_empty() {
        return true;
    }
    let first = &candidates[0].records;
    candidates
        .iter()
        .skip(1)
        .all(|candidate| &candidate.records == first)
}

fn delimiter_rank(delimiter: u8) -> usize {
    CANDIDATE_DELIMITERS
        .iter()
        .position(|candidate| *candidate == delimiter)
        .unwrap_or(usize::MAX)
}

fn skip_leading_blank_lines(input: &[u8]) -> &[u8] {
    let mut offset = 0usize;
    for line in input.split(|byte| *byte == b'\n') {
        if is_blank_line(line) {
            offset = offset.saturating_add(line.len() + 1);
            continue;
        }
        return &input[offset..];
    }
    b""
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_detects_backslash_escape() {
        let input = b"col1,col2\n\"hello\\\"world\",x\n";
        let dialect = auto_detect(input).expect("should detect");
        assert_eq!(dialect.delimiter, b',');
        assert_eq!(dialect.escape, EscapeMode::Backslash);
    }

    #[test]
    fn auto_detects_ambiguous_when_samples_differ() {
        let input = b"h1,h2;h3\n1,2;3\n";
        let err = auto_detect(input).expect_err("should be ambiguous");
        match err {
            DialectError::Ambiguous { tied } => {
                assert_eq!(tied, vec![b',', b';']);
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn auto_detect_guard_single_column() {
        let input = b"col\n1\n";
        let err = auto_detect(input).expect_err("should refuse single column");
        match err {
            DialectError::SingleColumn { delimiter } => {
                assert_eq!(delimiter, b',');
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn skip_leading_blank_lines_works() {
        let input = b"   \n\t\t\r\ncol1,col2\n1,2\n";
        let trimmed = skip_leading_blank_lines(input);
        assert_eq!(trimmed, b"col1,col2\n1,2\n");
    }
}
