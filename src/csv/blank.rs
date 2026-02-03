use crate::normalize::trim::{ascii_trim, is_ascii_blank_slice};

use csv::ByteRecord;

/// Returns true if the line is blank after ASCII-trim, ignoring a single
/// trailing carriage return (`\r`).
///
/// The caller should provide a line slice without the trailing `\n`.
#[inline]
pub fn is_blank_line(line: &[u8]) -> bool {
    is_ascii_blank_slice(strip_trailing_cr(line))
}

/// Strip a single trailing carriage return (`\r`) if present.
#[inline]
pub fn strip_trailing_cr(line: &[u8]) -> &[u8] {
    if line.ends_with(b"\r") {
        &line[..line.len() - 1]
    } else {
        line
    }
}

/// Returns true if every field in the record is empty after ASCII-trim.
///
/// Note: the header record must never be skipped even if blank; callers
/// should only apply this to data records.
pub fn is_blank_record(record: &ByteRecord) -> bool {
    if record.is_empty() {
        return true;
    }
    record.iter().all(|field| ascii_trim(field).is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use csv::ByteRecord;

    fn record(fields: &[&[u8]]) -> ByteRecord {
        let mut rec = ByteRecord::new();
        for field in fields {
            rec.push_field(field);
        }
        rec
    }

    #[test]
    fn blank_line_detection() {
        assert!(is_blank_line(b""));
        assert!(is_blank_line(b"   "));
        assert!(is_blank_line(b"\t\t"));
        assert!(is_blank_line(b" \t "));
        assert!(!is_blank_line(b" x "));
    }

    #[test]
    fn blank_line_trailing_cr() {
        assert!(is_blank_line(b"\r"));
        assert!(is_blank_line(b" \t\r"));
        assert!(!is_blank_line(b"x\r"));
    }

    #[test]
    fn blank_record_all_empty() {
        let rec = record(&[b"", b"  ", b"\t"]);
        assert!(is_blank_record(&rec));
    }

    #[test]
    fn blank_record_non_blank() {
        let rec = record(&[b"", b"\r", b"  "]);
        assert!(!is_blank_record(&rec));
        let rec = record(&[b"x"]);
        assert!(!is_blank_record(&rec));
    }
}
