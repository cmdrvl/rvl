use std::fmt;
use std::str::FromStr;

/// Canonical refusal codes (v0).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefusalCode {
    Io,
    Encoding,
    CsvParse,
    Headers,
    NoKey,
    KeyEmpty,
    KeyDup,
    KeyMismatch,
    RowCount,
    NeedKey,
    Dialect,
    MixedTypes,
    NoNumeric,
    Missingness,
    Diffuse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownRefusalCode;

impl RefusalCode {
    pub const ALL: [RefusalCode; 15] = [
        RefusalCode::Io,
        RefusalCode::Encoding,
        RefusalCode::CsvParse,
        RefusalCode::Headers,
        RefusalCode::NoKey,
        RefusalCode::KeyEmpty,
        RefusalCode::KeyDup,
        RefusalCode::KeyMismatch,
        RefusalCode::RowCount,
        RefusalCode::NeedKey,
        RefusalCode::Dialect,
        RefusalCode::MixedTypes,
        RefusalCode::NoNumeric,
        RefusalCode::Missingness,
        RefusalCode::Diffuse,
    ];

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            RefusalCode::Io => "E_IO",
            RefusalCode::Encoding => "E_ENCODING",
            RefusalCode::CsvParse => "E_CSV_PARSE",
            RefusalCode::Headers => "E_HEADERS",
            RefusalCode::NoKey => "E_NO_KEY",
            RefusalCode::KeyEmpty => "E_KEY_EMPTY",
            RefusalCode::KeyDup => "E_KEY_DUP",
            RefusalCode::KeyMismatch => "E_KEY_MISMATCH",
            RefusalCode::RowCount => "E_ROWCOUNT",
            RefusalCode::NeedKey => "E_NEED_KEY",
            RefusalCode::Dialect => "E_DIALECT",
            RefusalCode::MixedTypes => "E_MIXED_TYPES",
            RefusalCode::NoNumeric => "E_NO_NUMERIC",
            RefusalCode::Missingness => "E_MISSINGNESS",
            RefusalCode::Diffuse => "E_DIFFUSE",
        }
    }

    /// A short, stable reason label for human output.
    #[inline]
    pub const fn reason(self) -> &'static str {
        match self {
            RefusalCode::Io => "file read error",
            RefusalCode::Encoding => "unsupported text encoding",
            RefusalCode::CsvParse => "CSV parse failure",
            RefusalCode::Headers => "invalid or duplicate headers",
            RefusalCode::NoKey => "key column missing",
            RefusalCode::KeyEmpty => "empty key value",
            RefusalCode::KeyDup => "duplicate key values",
            RefusalCode::KeyMismatch => "key sets differ",
            RefusalCode::RowCount => "row count mismatch",
            RefusalCode::NeedKey => "cannot deterministically align without a key",
            RefusalCode::Dialect => "delimiter ambiguous or undetectable",
            RefusalCode::MixedTypes => "mixed numeric and non-numeric values",
            RefusalCode::NoNumeric => "no numeric columns in common",
            RefusalCode::Missingness => "missing value vs numeric value",
            RefusalCode::Diffuse => "diffuse change below coverage threshold",
        }
    }
}

impl fmt::Display for RefusalCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for UnknownRefusalCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown refusal code")
    }
}

impl std::error::Error for UnknownRefusalCode {}

impl FromStr for RefusalCode {
    type Err = UnknownRefusalCode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "E_IO" => Ok(RefusalCode::Io),
            "E_ENCODING" => Ok(RefusalCode::Encoding),
            "E_CSV_PARSE" => Ok(RefusalCode::CsvParse),
            "E_HEADERS" => Ok(RefusalCode::Headers),
            "E_NO_KEY" => Ok(RefusalCode::NoKey),
            "E_KEY_EMPTY" => Ok(RefusalCode::KeyEmpty),
            "E_KEY_DUP" => Ok(RefusalCode::KeyDup),
            "E_KEY_MISMATCH" => Ok(RefusalCode::KeyMismatch),
            "E_ROWCOUNT" => Ok(RefusalCode::RowCount),
            "E_NEED_KEY" => Ok(RefusalCode::NeedKey),
            "E_DIALECT" => Ok(RefusalCode::Dialect),
            "E_MIXED_TYPES" => Ok(RefusalCode::MixedTypes),
            "E_NO_NUMERIC" => Ok(RefusalCode::NoNumeric),
            "E_MISSINGNESS" => Ok(RefusalCode::Missingness),
            "E_DIFFUSE" => Ok(RefusalCode::Diffuse),
            _ => Err(UnknownRefusalCode),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RefusalCode, UnknownRefusalCode};
    use std::str::FromStr;

    #[test]
    fn codes_round_trip() {
        for code in RefusalCode::ALL {
            let text = code.as_str();
            let parsed = RefusalCode::from_str(text).expect("parse");
            assert_eq!(parsed, code);
        }
    }

    #[test]
    fn unknown_code_rejected() {
        let err = RefusalCode::from_str("E_NOPE").unwrap_err();
        assert_eq!(err, UnknownRefusalCode);
    }
}
