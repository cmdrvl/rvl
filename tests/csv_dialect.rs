use rvl::csv::dialect::{DialectError, auto_detect};
use rvl::csv::parser::EscapeMode;

#[test]
fn ambiguous_when_comma_and_tab_scores_tie() {
    let input = b"a,b\tc\n1,2\t3\n";
    match auto_detect(input) {
        Err(DialectError::Ambiguous { tied }) => {
            assert_eq!(tied, vec![b',', b'\t']);
        }
        other => panic!("expected ambiguous, got {:?}", other),
    }
}

#[test]
fn single_column_guard_triggers() {
    let input = b"only\n1\n";
    match auto_detect(input) {
        Err(DialectError::SingleColumn { delimiter }) => {
            assert_eq!(delimiter, b',');
        }
        other => panic!("expected single-column guard, got {:?}", other),
    }
}

#[test]
fn no_header_when_only_blank_lines() {
    let input = b"   \n\t\n";
    match auto_detect(input) {
        Err(DialectError::NoHeader) => {}
        other => panic!("expected no header, got {:?}", other),
    }
}

#[test]
fn detects_tab_delimiter_by_score() {
    let input = b"a\tb\n1\t2\n";
    let dialect = auto_detect(input).expect("should detect");
    assert_eq!(dialect.delimiter, b'\t');
}

#[test]
fn detects_backslash_escape_when_rfc_fails() {
    let input = b"col1,col2\n\"hello\\\"world\",x\n";
    let dialect = auto_detect(input).expect("should detect");
    assert_eq!(dialect.delimiter, b',');
    assert_eq!(dialect.escape, EscapeMode::Backslash);
}
