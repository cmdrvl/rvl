// JSON output schema assembly (bd-1lt)

use crate::diff::heap::MAX_CONTRIBUTORS;
use crate::format::ident_json::encode_identifier_json;
use crate::refusal::codes::RefusalCode;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Outcome {
    RealChange,
    NoRealChange,
    Refusal,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlignmentMode {
    Key,
    RowOrder,
}

#[derive(Debug, Clone, Serialize)]
pub struct Files {
    pub old: String,
    pub new: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Alignment {
    pub mode: AlignmentMode,
    pub key_column: Option<String>,
}

impl Alignment {
    pub fn key(encoded_key_column: String) -> Self {
        Self {
            mode: AlignmentMode::Key,
            key_column: Some(encoded_key_column),
        }
    }

    pub fn row_order() -> Self {
        Self {
            mode: AlignmentMode::RowOrder,
            key_column: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DialectSide {
    pub delimiter: String,
    pub quote: String,
    pub escape: Option<String>,
}

impl DialectSide {
    pub fn new(delimiter: u8, quote: u8, escape: Option<u8>) -> Self {
        Self {
            delimiter: byte_to_string(delimiter),
            quote: byte_to_string(quote),
            escape: escape.map(byte_to_string),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Dialect {
    pub old: Option<DialectSide>,
    pub new: Option<DialectSide>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Counts {
    pub rows_old: Option<u64>,
    pub rows_new: Option<u64>,
    pub rows_aligned: Option<u64>,
    pub columns_old: Option<u64>,
    pub columns_new: Option<u64>,
    pub columns_common: Option<u64>,
    pub columns_old_only: Option<u64>,
    pub columns_new_only: Option<u64>,
    pub numeric_columns: Option<u64>,
    pub numeric_cells_checked: Option<u64>,
    pub numeric_cells_changed: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Metrics {
    pub total_change: Option<f64>,
    pub max_abs_delta: Option<f64>,
    pub top_k_coverage: Option<f64>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Limits {
    pub max_contributors: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_contributors: MAX_CONTRIBUTORS as u64,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Contributor {
    pub row_id: String,
    pub column: String,
    pub old: f64,
    pub new: f64,
    pub delta: f64,
    pub contribution: f64,
    pub share: f64,
    pub cumulative_share: f64,
}

impl Contributor {
    #[allow(clippy::too_many_arguments)]
    pub fn from_bytes(
        row_id: &[u8],
        column: &[u8],
        old: f64,
        new: f64,
        delta: f64,
        contribution: f64,
        share: f64,
        cumulative_share: f64,
    ) -> Self {
        Self {
            row_id: encode_identifier_json(row_id),
            column: encode_identifier_json(column),
            old,
            new,
            delta,
            contribution,
            share,
            cumulative_share,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Refusal {
    pub code: String,
    pub message: String,
    pub detail: Value,
}

impl Refusal {
    pub fn new(code: RefusalCode, message: impl Into<String>, detail: Value) -> Self {
        Self {
            code: code.as_str().to_string(),
            message: message.into(),
            detail,
        }
    }
}

#[derive(Debug, Clone)]
pub struct JsonContext {
    pub files: Files,
    pub alignment: Alignment,
    pub dialect: Dialect,
    pub threshold: f64,
    pub tolerance: f64,
    pub counts: Counts,
    pub metrics: Metrics,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonOutput {
    pub version: &'static str,
    pub outcome: Outcome,
    pub files: Files,
    pub alignment: Alignment,
    pub dialect: Dialect,
    pub threshold: f64,
    pub tolerance: f64,
    pub counts: Counts,
    pub metrics: Metrics,
    pub limits: Limits,
    pub contributors: Vec<Contributor>,
    pub refusal: Option<Refusal>,
}

impl JsonOutput {
    pub fn real_change(ctx: JsonContext, contributors: Vec<Contributor>) -> Self {
        Self {
            version: "rvl.v0",
            outcome: Outcome::RealChange,
            files: ctx.files,
            alignment: ctx.alignment,
            dialect: ctx.dialect,
            threshold: ctx.threshold,
            tolerance: ctx.tolerance,
            counts: ctx.counts,
            metrics: ctx.metrics,
            limits: Limits::default(),
            contributors,
            refusal: None,
        }
    }

    pub fn no_real_change(ctx: JsonContext) -> Self {
        Self {
            version: "rvl.v0",
            outcome: Outcome::NoRealChange,
            files: ctx.files,
            alignment: ctx.alignment,
            dialect: ctx.dialect,
            threshold: ctx.threshold,
            tolerance: ctx.tolerance,
            counts: ctx.counts,
            metrics: ctx.metrics,
            limits: Limits::default(),
            contributors: Vec::new(),
            refusal: None,
        }
    }

    pub fn refusal(ctx: JsonContext, refusal: Refusal) -> Self {
        Self {
            version: "rvl.v0",
            outcome: Outcome::Refusal,
            files: ctx.files,
            alignment: ctx.alignment,
            dialect: ctx.dialect,
            threshold: ctx.threshold,
            tolerance: ctx.tolerance,
            counts: ctx.counts,
            metrics: ctx.metrics,
            limits: Limits::default(),
            contributors: Vec::new(),
            refusal: Some(refusal),
        }
    }

    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

pub fn render_json(output: &JsonOutput) -> Result<String, serde_json::Error> {
    serde_json::to_string(output)
}

pub fn encode_identifier_for_json(bytes: &[u8]) -> String {
    encode_identifier_json(bytes)
}

fn byte_to_string(byte: u8) -> String {
    (byte as char).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_context() -> JsonContext {
        JsonContext {
            files: Files {
                old: "old.csv".to_string(),
                new: "new.csv".to_string(),
            },
            alignment: Alignment {
                mode: AlignmentMode::Key,
                key_column: Some("u8:id".to_string()),
            },
            dialect: Dialect {
                old: Some(DialectSide::new(b',', b'"', None)),
                new: Some(DialectSide::new(b',', b'"', None)),
            },
            threshold: 0.95,
            tolerance: 1e-9,
            counts: Counts {
                rows_old: Some(10),
                rows_new: Some(10),
                rows_aligned: Some(10),
                columns_old: Some(3),
                columns_new: Some(3),
                columns_common: Some(3),
                columns_old_only: Some(0),
                columns_new_only: Some(0),
                numeric_columns: Some(2),
                numeric_cells_checked: Some(20),
                numeric_cells_changed: Some(3),
            },
            metrics: Metrics {
                total_change: Some(10.0),
                max_abs_delta: Some(5.0),
                top_k_coverage: Some(0.95),
            },
        }
    }

    #[test]
    fn renders_real_change_json_shape() {
        let ctx = sample_context();
        let contributor = Contributor::from_bytes(b"row1", b"value", 1.0, 2.0, 1.0, 1.0, 0.1, 0.1);
        let output = JsonOutput::real_change(ctx, vec![contributor]);
        let value = serde_json::to_value(output).expect("json");
        assert_eq!(value["version"], "rvl.v0");
        assert_eq!(value["outcome"], "REAL_CHANGE");
        assert_eq!(value["files"]["old"], "old.csv");
        assert_eq!(value["alignment"]["mode"], "key");
        assert_eq!(value["dialect"]["old"]["delimiter"], ",");
        assert_eq!(value["limits"]["max_contributors"], MAX_CONTRIBUTORS);
        assert!(value["contributors"].is_array());
    }

    #[test]
    fn renders_refusal_json_with_detail() {
        let mut ctx = sample_context();
        ctx.counts = Counts::default();
        ctx.metrics = Metrics::default();
        let refusal = Refusal::new(
            RefusalCode::RowCount,
            "row count mismatch",
            json!({"file":"old"}),
        );
        let output = JsonOutput::refusal(ctx, refusal);
        let value = serde_json::to_value(output).expect("json");
        assert_eq!(value["outcome"], "REFUSAL");
        assert_eq!(value["refusal"]["code"], "E_ROWCOUNT");
        assert_eq!(value["refusal"]["detail"]["file"], "old");
    }

    #[test]
    fn renders_no_real_change_with_empty_contributors() {
        let ctx = sample_context();
        let output = JsonOutput::no_real_change(ctx);
        let value = serde_json::to_value(output).expect("json");
        assert_eq!(value["outcome"], "NO_REAL_CHANGE");
        assert!(value["contributors"].as_array().expect("array").is_empty());
    }

    #[test]
    fn encodes_identifiers() {
        assert_eq!(encode_identifier_for_json(b"alpha"), "u8:alpha".to_string());
        assert_eq!(encode_identifier_for_json(b"\xff"), "hex:ff".to_string());
    }
}
