use rvl::numeric::parse::parse_numeric;

fn assert_close(actual: f64, expected: f64) {
    let diff = (actual - expected).abs();
    assert!(diff <= 1e-12, "expected {expected}, got {actual}");
}

#[test]
fn parses_supported_formats() {
    assert_close(parse_numeric(b"123").unwrap(), 123.0);
    assert_close(parse_numeric(b"-123").unwrap(), -123.0);
    assert_close(parse_numeric(b"+123").unwrap(), 123.0);
    assert_close(parse_numeric(b"123.45").unwrap(), 123.45);
    assert_close(parse_numeric(b"-123.45").unwrap(), -123.45);
    assert_close(parse_numeric(b"1e6").unwrap(), 1e6);
    assert_close(parse_numeric(b"-1.2E-3").unwrap(), -1.2e-3);

    assert_close(parse_numeric(b"1,234").unwrap(), 1234.0);
    assert_close(parse_numeric(b"-1,234").unwrap(), -1234.0);
    assert_close(parse_numeric(b"+1,234").unwrap(), 1234.0);
    assert_close(parse_numeric(b"1,234,567.89").unwrap(), 1234567.89);
    assert_close(parse_numeric(b"-1,234,567.89").unwrap(), -1234567.89);

    assert_close(parse_numeric(b"$123.45").unwrap(), 123.45);
    assert_close(parse_numeric(b"$1,234.56").unwrap(), 1234.56);
    assert_close(parse_numeric(b"-$1,234.56").unwrap(), -1234.56);
    assert_close(parse_numeric(b"$-1,234.56").unwrap(), -1234.56);
    assert_close(parse_numeric(b"+$1,234.56").unwrap(), 1234.56);
    assert_close(parse_numeric(b"$+1,234.56").unwrap(), 1234.56);

    assert_close(parse_numeric(b"(123.45)").unwrap(), -123.45);
    assert_close(parse_numeric(b"(1,234.56)").unwrap(), -1234.56);
    assert_close(parse_numeric(b"($1,234.56)").unwrap(), -1234.56);
    assert_close(parse_numeric(b"($-1,234.56)").unwrap(), -1234.56);
}

#[test]
fn rejects_invalid_formats() {
    assert!(parse_numeric(b"").is_none());
    assert!(parse_numeric(b"$").is_none());
    assert!(parse_numeric(b"12,34").is_none());
    assert!(parse_numeric(b"1,23,456").is_none());
    assert!(parse_numeric(b"1,234,56.78").is_none());
    assert!(parse_numeric(b",123").is_none());
    assert!(parse_numeric(b"123,").is_none());
    assert!(parse_numeric(b"1,234.5.6").is_none());
    assert!(parse_numeric(b"+$-1").is_none());
    assert!(parse_numeric(b"--1").is_none());
    assert!(parse_numeric(b"NaN").is_none());
    assert!(parse_numeric(b"inf").is_none());
    assert!(parse_numeric(b"+inf").is_none());
    assert!(parse_numeric(b"-inf").is_none());
}
