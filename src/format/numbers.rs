// Number formatting utilities (bd-8z7)

/// Format an integer with thousands separators.
pub fn format_int_with_commas(value: i64) -> String {
    let mut n = value as i128;
    let negative = n < 0;
    if negative {
        n = -n;
    }
    let digits = n.to_string();
    let bytes = digits.as_bytes();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3 + 1);
    if negative {
        out.push('-');
    }
    let mut idx = 0;
    let first_group = if bytes.len().is_multiple_of(3) {
        3
    } else {
        bytes.len() % 3
    };
    out.push_str(std::str::from_utf8(&bytes[..first_group]).expect("digits are ascii"));
    idx += first_group;
    while idx < bytes.len() {
        out.push(',');
        out.push_str(std::str::from_utf8(&bytes[idx..idx + 3]).expect("digits are ascii"));
        idx += 3;
    }
    out
}

/// Format a float using the shortest round-trippable representation.
pub fn format_float_shortest(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let plain = value.to_string();
    let scientific = format_scientific_short(value);
    if scientific.len() < plain.len() {
        scientific
    } else {
        plain
    }
}

/// Format a delta with an explicit sign prefix.
pub fn format_delta(value: f64) -> String {
    if value == 0.0 {
        return "+0".to_string();
    }
    let sign = if value.is_sign_negative() { '-' } else { '+' };
    let abs = value.abs();
    let mut out = String::with_capacity(1 + 24);
    out.push(sign);
    out.push_str(&format_float_shortest(abs));
    out
}

/// Format a ratio as a percentage with one decimal place.
pub fn format_percent_one_decimal(value: f64) -> String {
    format!("{:.1}%", value * 100.0)
}

fn format_scientific_short(value: f64) -> String {
    let raw = format!("{:e}", value);
    let (mantissa, exponent) = match raw.split_once('e') {
        Some(parts) => parts,
        None => return raw,
    };

    let mantissa = trim_mantissa(mantissa);
    let exponent = trim_exponent(exponent);
    format!("{mantissa}e{exponent}")
}

fn trim_mantissa(input: &str) -> String {
    let mut out = input.to_string();
    if let Some(dot_index) = out.find('.') {
        while out.ends_with('0') {
            out.pop();
        }
        if out.len() == dot_index + 1 {
            out.pop();
        }
    }
    out
}

fn trim_exponent(input: &str) -> String {
    let (sign, digits) = match input.as_bytes().first() {
        Some(b'+') => ("", &input[1..]),
        Some(b'-') => ("-", &input[1..]),
        _ => ("", input),
    };
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        format!("{sign}0")
    } else {
        format!("{sign}{digits}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_int_with_commas() {
        assert_eq!(format_int_with_commas(0), "0");
        assert_eq!(format_int_with_commas(12), "12");
        assert_eq!(format_int_with_commas(1234), "1,234");
        assert_eq!(format_int_with_commas(1234567), "1,234,567");
        assert_eq!(format_int_with_commas(-1234567), "-1,234,567");
    }

    #[test]
    fn formats_float_shortest() {
        assert_eq!(format_float_shortest(1.0), "1");
        assert_eq!(format_float_shortest(1.25), "1.25");
        assert_eq!(format_float_shortest(-1.25), "-1.25");
    }

    #[test]
    fn formats_delta_with_sign() {
        assert_eq!(format_delta(0.0), "+0");
        assert_eq!(format_delta(-0.0), "+0");
        assert_eq!(format_delta(2.5), "+2.5");
        assert_eq!(format_delta(-2.5), "-2.5");
    }

    #[test]
    fn formats_percent_one_decimal() {
        assert_eq!(format_percent_one_decimal(0.95), "95.0%");
        assert_eq!(format_percent_one_decimal(0.001), "0.1%");
    }
}
