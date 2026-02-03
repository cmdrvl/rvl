//! Record width normalization & E_HEADERS (bd-1r2).
//!
//! Rules (docs/PLAN_RVL.md):
//! - If a row has fewer fields than the header, missing trailing fields are
//!   treated as empty string.
//! - If a row has more fields than the header, extra trailing fields must be
//!   empty after ASCII-trim; otherwise refuse with E_HEADERS.

use csv::ByteRecord;

use crate::normalize::trim::is_ascii_blank_slice;

/// Error returned when a record has non-empty extra fields beyond the header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordWidthError {
    pub record_number: u64,
    pub first_extra_index: usize,
}

/// A view that treats missing trailing fields as empty strings.
#[derive(Debug, Clone, Copy)]
pub struct NormalizedRecord<'a> {
    record: &'a ByteRecord,
    header_len: usize,
}

impl<'a> NormalizedRecord<'a> {
    /// Returns the field at `index`, or empty string if missing.
    pub fn field(&self, index: usize) -> &'a [u8] {
        if index >= self.header_len {
            return b"";
        }
        self.record.get(index).unwrap_or(b"")
    }

    /// Returns the normalized width (header length).
    pub fn len(&self) -> usize {
        self.header_len
    }

    /// Returns true if the normalized width is zero.
    pub fn is_empty(&self) -> bool {
        self.header_len == 0
    }
}

/// Normalize a record to the header width, validating extra trailing fields.
pub fn normalize_record<'a>(
    record: &'a ByteRecord,
    header_len: usize,
    record_number: u64,
) -> Result<NormalizedRecord<'a>, RecordWidthError> {
    if record.len() > header_len {
        for (index, field) in record.iter().enumerate().skip(header_len) {
            if !is_ascii_blank_slice(field) {
                return Err(RecordWidthError {
                    record_number,
                    first_extra_index: index,
                });
            }
        }
    }
    Ok(NormalizedRecord { record, header_len })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(fields: &[&[u8]]) -> ByteRecord {
        let mut rec = ByteRecord::new();
        for field in fields {
            rec.push_field(field);
        }
        rec
    }

    #[test]
    fn pads_short_rows_as_empty() {
        let rec = record(&[b"a", b"b"]);
        let normalized = normalize_record(&rec, 4, 1).expect("should normalize");
        assert_eq!(normalized.len(), 4);
        assert_eq!(normalized.field(0), b"a");
        assert_eq!(normalized.field(1), b"b");
        assert_eq!(normalized.field(2), b"");
        assert_eq!(normalized.field(3), b"");
    }

    #[test]
    fn accepts_equal_width_rows() {
        let rec = record(&[b"a", b"b", b"c"]);
        let normalized = normalize_record(&rec, 3, 9).expect("should normalize");
        assert_eq!(normalized.len(), 3);
        assert_eq!(normalized.field(2), b"c");
    }

    #[test]
    fn ignores_extra_trailing_empty_fields() {
        let rec = record(&[b"a", b"b", b"", b" \t"]);
        let normalized = normalize_record(&rec, 2, 7).expect("should normalize");
        assert_eq!(normalized.len(), 2);
        assert_eq!(normalized.field(1), b"b");
    }

    #[test]
    fn rejects_extra_non_empty_fields() {
        let rec = record(&[b"a", b"b", b"extra"]);
        let err = normalize_record(&rec, 2, 42).expect_err("should error");
        assert_eq!(
            err,
            RecordWidthError {
                record_number: 42,
                first_extra_index: 2
            }
        );
    }
}
