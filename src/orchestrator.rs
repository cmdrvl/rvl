//! Pipeline orchestration: parse → align → diff → output (bd-22s)

mod capsule;

use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::io::Cursor;
use std::path::Path;

use csv::ByteRecord;
use serde_json::{Value, json};

use crate::alignment::key_discovery::{KeyRow, discover_key_candidates};
use crate::alignment::key_join::{
    KeyAlignedRow, KeyJoinError, OwnedRecord, build_key_map, join_key_maps,
};
use crate::alignment::key_parse::parse_key_identifier;
use crate::alignment::shuffle::detect_shuffle;
use crate::cli::args::Args;
use crate::cli::exit::Outcome;
use crate::csv::blank::is_blank_record;
use crate::csv::dialect::{DialectError, auto_detect};
use crate::csv::input::{
    EncodingIssue as InputEncodingIssue, UTF32_BE_BOM, UTF32_LE_BOM, guard_input_bytes,
};
use crate::csv::parser::{EscapeMode, build_reader, detect_escape_mode};
use crate::csv::records::normalize_record;
use crate::csv::sep::{SepScan, scan_first_non_blank_line};
use crate::diff::coverage::{CoverageDecision, evaluate_coverage};
use crate::diff::heap::DiffAccumulator;
use crate::diff::order::{CellId, RowId, TieBreaker, sort_contributors};
use crate::diff::tolerance::ToleranceTracker;
use crate::format::ident_human::render_identifier_human;
use crate::format::ident_json::encode_identifier_json;
use crate::normalize::headers::normalize_headers;
use crate::numeric::columns::{
    ColumnIntersection, ColumnTypingError, Side as ColumnSide, detect_numeric_columns,
    intersect_headers,
};
use crate::numeric::missing::is_missing_token;
use crate::numeric::parse::parse_numeric;
use crate::output::human::header::{
    Alignment as HumanAlignment, CheckedCounts, ColumnCounts, DialectReceipt, HumanHeader,
    Profile as HumanProfile, RefusalHeader, Settings as HumanSettings, render_real_no_real_header,
    render_refusal_header,
};
use crate::output::human::no_real::{NoRealBody, render_no_real_body};
use crate::output::human::real_change::{
    RealChangeBody, RealChangeContributor, render_real_change_body,
};
use crate::output::human::refusal::{RefusalBody, render_refusal_body};
use crate::output::json::{
    Alignment as JsonAlignment, Counts, Dialect, DialectSide, Files, JsonContext, JsonOutput,
    Metrics, Refusal as JsonRefusal,
};
use crate::profile::{ResolvedProfile, load_profile_from_path, resolve_profile_id};
use crate::refusal::codes::RefusalCode;
use crate::refusal::details::{
    DelimiterHint, DialectSuggestion, EncodingIssue, FileSide, HeadersIssue, NamedDelimiter,
    RefusalDetail, RefusalKind, RerunPaths,
};
use capsule::{CapsuleContributor, CapsuleContributorSummary, CapsuleRunSummary};

pub struct PipelineResult {
    pub outcome: Outcome,
    pub output: String,
    pub profile: ProfileRunInfo,
}

#[derive(Clone, Debug, Default)]
pub struct ProfileRunInfo {
    pub used: bool,
    pub profile_id: Option<String>,
    pub profile_sha256: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct ActiveProfile {
    include_scope: Option<HashSet<Vec<u8>>>,
    key: Option<Vec<u8>>,
    key_labels: Vec<String>,
    info: ProfileRunInfo,
}

struct ParsedCsv {
    delimiter: u8,
    escape: EscapeMode,
    headers: Vec<Vec<u8>>,
    records: Vec<OwnedRecord>,
}

struct RefusalPayload {
    code: RefusalCode,
    detail: RefusalDetail,
}

struct RefusalContext<'a> {
    key: Option<&'a [u8]>,
    dialect_old: Option<DialectReceipt>,
    dialect_new: Option<DialectReceipt>,
    alignment: JsonAlignment,
    profile: ProfileRunInfo,
    counts: Counts,
    metrics: Metrics,
}

#[derive(Clone, Copy)]
struct RunContext<'a> {
    args: &'a Args,
    dialect_old: Option<DialectReceipt>,
    dialect_new: Option<DialectReceipt>,
    rerun_paths: RerunPaths<'a>,
    active_profile: &'a ActiveProfile,
}

#[derive(Clone, Debug)]
struct RowRef {
    old_record: u64,
    new_record: u64,
    key: Option<Vec<u8>>,
}

impl RowRef {
    fn record_for(&self, side: ColumnSide) -> u64 {
        match side {
            ColumnSide::Old => self.old_record,
            ColumnSide::New => self.new_record,
        }
    }
}

impl KeyRow for OwnedRecord {
    fn field(&self, index: usize) -> &[u8] {
        self.get(index).map(|v| v.as_slice()).unwrap_or(b"")
    }
}

pub fn run(args: &Args) -> Result<PipelineResult, Box<dyn Error>> {
    let old_path = args.old_path().to_string_lossy().to_string();
    let new_path = args.new_path().to_string_lossy().to_string();
    let rerun_paths = RerunPaths {
        old: &old_path,
        new: &new_path,
    };

    let active_profile = match resolve_active_profile(args, rerun_paths) {
        Ok(profile) => profile,
        Err(refusal) => {
            return Ok(render_refusal(
                *refusal,
                args,
                None,
                None,
                None,
                &ProfileRunInfo::default(),
            ));
        }
    };

    let cli_key = match args.key.as_deref() {
        Some(key) => Some(parse_key_identifier(key)?),
        None => None,
    };
    if cli_key.is_some() && active_profile.key.is_some() {
        let refusal = RefusalPayload::with_default_next(
            RefusalCode::KeyConflict,
            RefusalKind::KeyConflict {
                key_flag: args.key.clone().unwrap_or_default(),
                profile_key: active_profile.key_labels.clone(),
            },
            rerun_paths,
        );
        return Ok(render_refusal(
            refusal,
            args,
            None,
            None,
            None,
            &active_profile.info,
        ));
    }
    let key_bytes = cli_key.or_else(|| active_profile.key.clone());

    let old = match parse_csv(args.old_path(), FileSide::Old, args.delimiter, rerun_paths) {
        Ok(parsed) => parsed,
        Err(refusal) => {
            return Ok(render_refusal(
                *refusal,
                args,
                key_bytes.as_deref(),
                None,
                None,
                &active_profile.info,
            ));
        }
    };

    let new = match parse_csv(args.new_path(), FileSide::New, args.delimiter, rerun_paths) {
        Ok(parsed) => parsed,
        Err(refusal) => {
            return Ok(render_refusal(
                *refusal,
                args,
                key_bytes.as_deref(),
                Some(dialect_receipt(&old)),
                None,
                &active_profile.info,
            ));
        }
    };

    let dialect_old = Some(dialect_receipt(&old));
    let dialect_new = Some(dialect_receipt(&new));
    let context = RunContext {
        args,
        dialect_old,
        dialect_new,
        rerun_paths,
        active_profile: &active_profile,
    };

    if let Some(key) = key_bytes.as_deref() {
        run_key_mode(key, old, new, context)
    } else {
        run_row_order(old, new, context)
    }
}

fn resolve_active_profile(
    args: &Args,
    rerun_paths: RerunPaths<'_>,
) -> Result<ActiveProfile, Box<RefusalPayload>> {
    if let (Some(profile_path), Some(profile_id)) =
        (args.profile.as_ref(), args.profile_id.as_ref())
    {
        return Err(Box::new(RefusalPayload::with_default_next(
            RefusalCode::AmbiguousProfile,
            RefusalKind::AmbiguousProfile {
                profile_path: profile_path.to_string_lossy().to_string(),
                profile_id: profile_id.clone(),
            },
            rerun_paths,
        )));
    }

    if let Some(profile_path) = args.profile.as_ref() {
        let selector = profile_path.to_string_lossy().to_string();
        let resolved = load_profile_from_path(profile_path).map_err(|_| {
            Box::new(RefusalPayload::with_default_next(
                RefusalCode::ProfileNotFound,
                RefusalKind::ProfileNotFound {
                    profile_id: selector.clone(),
                },
                rerun_paths,
            ))
        })?;
        return Ok(active_profile_from_resolved(resolved));
    }

    if let Some(profile_id) = args.profile_id.as_ref() {
        let resolved = resolve_profile_id(profile_id).map_err(|_| {
            Box::new(RefusalPayload::with_default_next(
                RefusalCode::ProfileNotFound,
                RefusalKind::ProfileNotFound {
                    profile_id: profile_id.clone(),
                },
                rerun_paths,
            ))
        })?;
        return Ok(active_profile_from_resolved(resolved));
    }

    Ok(ActiveProfile::default())
}

fn active_profile_from_resolved(profile: ResolvedProfile) -> ActiveProfile {
    ActiveProfile {
        include_scope: Some(profile.include_set()),
        key: profile.primary_key().map(|key| key.to_vec()),
        key_labels: profile.key_labels,
        info: ProfileRunInfo {
            used: true,
            profile_id: profile.profile_id,
            profile_sha256: profile.profile_sha256,
        },
    }
}

fn run_key_mode(
    key: &[u8],
    old: ParsedCsv,
    new: ParsedCsv,
    context: RunContext<'_>,
) -> Result<PipelineResult, Box<dyn Error>> {
    let args = context.args;
    let dialect_old = context.dialect_old;
    let dialect_new = context.dialect_new;
    let rerun_paths = context.rerun_paths;
    let active_profile = context.active_profile;

    let old_key_index = match find_key_index(&old.headers, key) {
        Some(index) => index,
        None => {
            let refusal = RefusalPayload::with_default_next(
                RefusalCode::NoKey,
                RefusalKind::NoKey {
                    key_column: key.to_vec(),
                },
                rerun_paths,
            );
            return Ok(render_refusal(
                refusal,
                args,
                Some(key),
                dialect_old,
                dialect_new,
                &active_profile.info,
            ));
        }
    };

    let new_key_index = match find_key_index(&new.headers, key) {
        Some(index) => index,
        None => {
            let refusal = RefusalPayload::with_default_next(
                RefusalCode::NoKey,
                RefusalKind::NoKey {
                    key_column: key.to_vec(),
                },
                rerun_paths,
            );
            return Ok(render_refusal(
                refusal,
                args,
                Some(key),
                dialect_old,
                dialect_new,
                &active_profile.info,
            ));
        }
    };

    let rows_old = old.records.len() as u64;
    let rows_new = new.records.len() as u64;

    let old_map = match build_key_map(
        old.records
            .into_iter()
            .enumerate()
            .map(|(idx, record)| ((idx + 1) as u64, record)),
        old_key_index,
    ) {
        Ok(map) => map,
        Err(err) => {
            let refusal = map_key_join_error(err, FileSide::Old, key, rerun_paths);
            return Ok(render_refusal(
                refusal,
                args,
                Some(key),
                dialect_old,
                dialect_new,
                &active_profile.info,
            ));
        }
    };

    let new_map = match build_key_map(
        new.records
            .into_iter()
            .enumerate()
            .map(|(idx, record)| ((idx + 1) as u64, record)),
        new_key_index,
    ) {
        Ok(map) => map,
        Err(err) => {
            let refusal = map_key_join_error(err, FileSide::New, key, rerun_paths);
            return Ok(render_refusal(
                refusal,
                args,
                Some(key),
                dialect_old,
                dialect_new,
                &active_profile.info,
            ));
        }
    };

    let aligned = match join_key_maps(old_map, new_map) {
        Ok(rows) => rows,
        Err(err) => {
            let refusal = map_key_join_error(err, FileSide::New, key, rerun_paths);
            return Ok(render_refusal(
                refusal,
                args,
                Some(key),
                dialect_old,
                dialect_new,
                &active_profile.info,
            ));
        }
    };

    run_diff(
        AlignmentContext::Key {
            key: key.to_vec(),
            rows_old,
            rows_new,
            key_rows: aligned,
        },
        old.headers,
        new.headers,
        context,
    )
}

fn run_row_order(
    old: ParsedCsv,
    new: ParsedCsv,
    context: RunContext<'_>,
) -> Result<PipelineResult, Box<dyn Error>> {
    let args = context.args;
    let dialect_old = context.dialect_old;
    let dialect_new = context.dialect_new;
    let rerun_paths = context.rerun_paths;
    let active_profile = context.active_profile;

    if old.records.len() != new.records.len() {
        let suggested_keys = discover_key_candidates(
            &old.headers,
            &new.headers,
            old.records.iter(),
            new.records.iter(),
        )
        .into_iter()
        .map(|candidate| candidate.name)
        .take(3)
        .collect();

        let refusal = RefusalPayload::with_default_next(
            RefusalCode::RowCount,
            RefusalKind::RowCount {
                rows_old: old.records.len() as u64,
                rows_new: new.records.len() as u64,
                suggested_keys,
            },
            rerun_paths,
        );
        let intersection = scope_intersection(
            intersect_headers(&old.headers, &new.headers, None),
            active_profile.include_scope.as_ref(),
        );
        let counts = Counts {
            rows_old: Some(old.records.len() as u64),
            rows_new: Some(new.records.len() as u64),
            rows_aligned: None,
            columns_old: Some(count_columns(
                &old.headers,
                None,
                active_profile.include_scope.as_ref(),
            )),
            columns_new: Some(count_columns(
                &new.headers,
                None,
                active_profile.include_scope.as_ref(),
            )),
            columns_common: Some(intersection.common.len() as u64),
            columns_old_only: Some(intersection.old_only.len() as u64),
            columns_new_only: Some(intersection.new_only.len() as u64),
            ..Counts::default()
        };
        let context = RefusalContext {
            key: None,
            dialect_old,
            dialect_new,
            alignment: JsonAlignment::row_order(),
            profile: active_profile.info.clone(),
            counts,
            metrics: Metrics::default(),
        };
        return Ok(render_refusal_with_context(refusal, args, context));
    }

    run_diff(
        AlignmentContext::RowOrder {
            old_rows: old.records,
            new_rows: new.records,
        },
        old.headers,
        new.headers,
        context,
    )
}

enum AlignmentContext {
    Key {
        key: Vec<u8>,
        rows_old: u64,
        rows_new: u64,
        key_rows: Vec<KeyAlignedRow>,
    },
    RowOrder {
        old_rows: Vec<OwnedRecord>,
        new_rows: Vec<OwnedRecord>,
    },
}

fn run_diff(
    alignment: AlignmentContext,
    old_headers: Vec<Vec<u8>>,
    new_headers: Vec<Vec<u8>>,
    context: RunContext<'_>,
) -> Result<PipelineResult, Box<dyn Error>> {
    let args = context.args;
    let dialect_old = context.dialect_old;
    let dialect_new = context.dialect_new;
    let rerun_paths = context.rerun_paths;
    let active_profile = context.active_profile;

    let key_bytes = match &alignment {
        AlignmentContext::Key { key, .. } => Some(key.as_slice()),
        AlignmentContext::RowOrder { .. } => None,
    };

    let intersection = scope_intersection(
        intersect_headers(&old_headers, &new_headers, key_bytes),
        active_profile.include_scope.as_ref(),
    );

    let (rows_old, rows_new, rows_aligned) = match &alignment {
        AlignmentContext::Key {
            rows_old,
            rows_new,
            key_rows,
            ..
        } => (*rows_old, *rows_new, key_rows.len() as u64),
        AlignmentContext::RowOrder { old_rows, new_rows } => {
            let rows_old = old_rows.len() as u64;
            let rows_new = new_rows.len() as u64;
            (rows_old, rows_new, rows_old.min(rows_new))
        }
    };

    let numeric_columns = match &alignment {
        AlignmentContext::Key { key_rows, .. } => {
            let rows = key_rows.iter().map(|row| {
                (
                    RowRef {
                        old_record: row.old.record_number,
                        new_record: row.new.record_number,
                        key: Some(row.key.clone()),
                    },
                    row.old.fields.as_slice(),
                    row.new.fields.as_slice(),
                )
            });
            match detect_numeric_columns(&intersection.common, rows) {
                Ok(columns) => columns,
                Err(err) => {
                    let refusal = map_column_error(err, rerun_paths);
                    return Ok(render_refusal(
                        refusal,
                        args,
                        key_bytes,
                        dialect_old,
                        dialect_new,
                        &active_profile.info,
                    ));
                }
            }
        }
        AlignmentContext::RowOrder { old_rows, new_rows } => {
            let rows = old_rows.iter().zip(new_rows.iter()).enumerate().map(
                |(idx, (old_row, new_row))| {
                    let record = (idx + 1) as u64;
                    (
                        RowRef {
                            old_record: record,
                            new_record: record,
                            key: None,
                        },
                        old_row.as_slice(),
                        new_row.as_slice(),
                    )
                },
            );
            match detect_numeric_columns(&intersection.common, rows) {
                Ok(columns) => columns,
                Err(err) => {
                    let refusal = map_column_error(err, rerun_paths);
                    return Ok(render_refusal(
                        refusal,
                        args,
                        key_bytes,
                        dialect_old,
                        dialect_new,
                        &active_profile.info,
                    ));
                }
            }
        }
    };

    if numeric_columns.is_empty() {
        let refusal = RefusalPayload::with_default_next(
            RefusalCode::NoNumeric,
            RefusalKind::NoNumeric,
            rerun_paths,
        );
        let alignment_mode = match &alignment {
            AlignmentContext::Key { key, .. } => JsonAlignment::key(encode_identifier_json(key)),
            AlignmentContext::RowOrder { .. } => JsonAlignment::row_order(),
        };
        let counts = Counts {
            rows_old: Some(rows_old),
            rows_new: Some(rows_new),
            rows_aligned: Some(rows_aligned),
            columns_old: Some(count_columns(
                &old_headers,
                key_bytes,
                active_profile.include_scope.as_ref(),
            )),
            columns_new: Some(count_columns(
                &new_headers,
                key_bytes,
                active_profile.include_scope.as_ref(),
            )),
            columns_common: Some(intersection.common.len() as u64),
            columns_old_only: Some(intersection.old_only.len() as u64),
            columns_new_only: Some(intersection.new_only.len() as u64),
            numeric_columns: Some(0),
            numeric_cells_checked: Some(0),
            numeric_cells_changed: Some(0),
        };
        let context = RefusalContext {
            key: key_bytes,
            dialect_old,
            dialect_new,
            alignment: alignment_mode,
            profile: active_profile.info.clone(),
            counts,
            metrics: Metrics::default(),
        };
        return Ok(render_refusal_with_context(refusal, args, context));
    }

    let mut accumulator = DiffAccumulator::with_default_max();
    let mut tie_breaker = TieBreaker::default();
    let mut tolerance = ToleranceTracker::new(args.tolerance);
    let mut numeric_cells_changed = 0u64;

    match &alignment {
        AlignmentContext::Key { key_rows, .. } => {
            for row in key_rows.iter() {
                let row_id = RowId::key(row.key.clone());
                for column in &numeric_columns {
                    let old_raw = row
                        .old
                        .fields
                        .get(column.old_index)
                        .map(|v| v.as_slice())
                        .unwrap_or(b"");
                    let new_raw = row
                        .new
                        .fields
                        .get(column.new_index)
                        .map(|v| v.as_slice())
                        .unwrap_or(b"");
                    if is_missing_token(old_raw) && is_missing_token(new_raw) {
                        continue;
                    }
                    let (old_val, new_val) = match (parse_numeric(old_raw), parse_numeric(new_raw))
                    {
                        (Some(old_val), Some(new_val)) => (old_val, new_val),
                        _ => continue,
                    };
                    let (delta, contribution) = tolerance.apply(old_val, new_val);
                    if contribution > 0.0 {
                        numeric_cells_changed += 1;
                    }
                    let cell_id = CellId::new(row_id.clone(), column.name.clone());
                    accumulator.observe(
                        cell_id,
                        old_val,
                        new_val,
                        delta,
                        contribution,
                        tie_breaker.next_value(),
                    );
                }
            }
        }
        AlignmentContext::RowOrder { old_rows, new_rows } => {
            for (idx, (old_row, new_row)) in old_rows.iter().zip(new_rows.iter()).enumerate() {
                let row_id = RowId::row_index(idx + 1);
                for column in &numeric_columns {
                    let old_raw = old_row
                        .get(column.old_index)
                        .map(|v| v.as_slice())
                        .unwrap_or(b"");
                    let new_raw = new_row
                        .get(column.new_index)
                        .map(|v| v.as_slice())
                        .unwrap_or(b"");
                    if is_missing_token(old_raw) && is_missing_token(new_raw) {
                        continue;
                    }
                    let (old_val, new_val) = match (parse_numeric(old_raw), parse_numeric(new_raw))
                    {
                        (Some(old_val), Some(new_val)) => (old_val, new_val),
                        _ => continue,
                    };
                    let (delta, contribution) = tolerance.apply(old_val, new_val);
                    if contribution > 0.0 {
                        numeric_cells_changed += 1;
                    }
                    let cell_id = CellId::new(row_id.clone(), column.name.clone());
                    accumulator.observe(
                        cell_id,
                        old_val,
                        new_val,
                        delta,
                        contribution,
                        tie_breaker.next_value(),
                    );
                }
            }
        }
    }

    let mut top = accumulator.top.into_vec();
    sort_contributors(&mut top);
    let contributions: Vec<f64> = top.iter().map(|c| c.contribution).collect();

    let top_k_coverage = if accumulator.total_change > 0.0 {
        Some(contributions.iter().copied().sum::<f64>() / accumulator.total_change)
    } else {
        None
    };

    let coverage = evaluate_coverage(&contributions, accumulator.total_change, args.threshold);

    let alignment_mode = match &alignment {
        AlignmentContext::Key { key, .. } => JsonAlignment::key(encode_identifier_json(key)),
        AlignmentContext::RowOrder { .. } => JsonAlignment::row_order(),
    };

    let counts = Counts {
        rows_old: Some(rows_old),
        rows_new: Some(rows_new),
        rows_aligned: Some(rows_aligned),
        columns_old: Some(count_columns(
            &old_headers,
            key_bytes,
            active_profile.include_scope.as_ref(),
        )),
        columns_new: Some(count_columns(
            &new_headers,
            key_bytes,
            active_profile.include_scope.as_ref(),
        )),
        columns_common: Some(intersection.common.len() as u64),
        columns_old_only: Some(intersection.old_only.len() as u64),
        columns_new_only: Some(intersection.new_only.len() as u64),
        numeric_columns: Some(numeric_columns.len() as u64),
        numeric_cells_checked: Some(rows_aligned * numeric_columns.len() as u64),
        numeric_cells_changed: Some(numeric_cells_changed),
    };

    let mut metrics = Metrics {
        total_change: Some(accumulator.total_change),
        max_abs_delta: Some(accumulator.max_abs_delta),
        top_k_coverage,
    };

    if let AlignmentContext::RowOrder { old_rows, new_rows } = &alignment
        && accumulator.total_change > 0.0
    {
        let detection = detect_shuffle(&old_headers, &new_headers, old_rows, new_rows);
        if detection.reordered {
            let refusal = RefusalPayload::with_default_next(
                RefusalCode::NeedKey,
                RefusalKind::NeedKey {
                    suggested_keys: detection.suggested_keys,
                },
                rerun_paths,
            );
            let mut counts = counts.clone();
            counts.numeric_cells_checked = None;
            counts.numeric_cells_changed = None;
            metrics = Metrics::default();
            let context = RefusalContext {
                key: key_bytes,
                dialect_old,
                dialect_new,
                alignment: alignment_mode,
                profile: active_profile.info.clone(),
                counts,
                metrics,
            };
            return Ok(render_refusal_with_context(refusal, args, context));
        }
    }

    let alignment_label = key_bytes.map(render_identifier_human);

    match coverage {
        CoverageDecision::NoChange => {
            let ctx = json_context(
                args,
                alignment_mode,
                dialect_old,
                dialect_new,
                &active_profile.info,
                counts,
                metrics,
            );
            Ok(render_no_real_change(args, ctx, alignment_label.as_deref()))
        }
        CoverageDecision::Diffuse { top_k_coverage } => {
            let refusal = RefusalPayload::with_default_next(
                RefusalCode::Diffuse,
                RefusalKind::Diffuse {
                    top_k_coverage,
                    threshold: args.threshold,
                },
                rerun_paths,
            );
            let context = RefusalContext {
                key: key_bytes,
                dialect_old,
                dialect_new,
                alignment: alignment_mode,
                profile: active_profile.info.clone(),
                counts,
                metrics,
            };
            Ok(render_refusal_with_context(refusal, args, context))
        }
        CoverageDecision::Explainable { cutoff, coverage } => {
            let details =
                collect_details(&alignment, &numeric_columns, &top[..cutoff], args.tolerance);
            let ctx = json_context(
                args,
                alignment_mode,
                dialect_old,
                dialect_new,
                &active_profile.info,
                counts,
                metrics,
            );
            Ok(render_real_change(
                args,
                ctx,
                &details,
                coverage,
                alignment_label.as_deref(),
            ))
        }
    }
}

fn parse_csv(
    path: &Path,
    file_side: FileSide,
    forced_delimiter: Option<u8>,
    rerun_paths: RerunPaths<'_>,
) -> Result<ParsedCsv, Box<RefusalPayload>> {
    let bytes = fs::read(path).map_err(|err| {
        Box::new(RefusalPayload::with_default_next(
            RefusalCode::Io,
            RefusalKind::Io {
                file: file_side,
                error: err.to_string(),
            },
            rerun_paths,
        ))
    })?;

    let guarded = guard_input_bytes(&bytes).map_err(|issue| {
        Box::new(RefusalPayload::with_default_next(
            RefusalCode::Encoding,
            RefusalKind::Encoding {
                file: file_side,
                issue: map_encoding_issue(&bytes, issue),
            },
            rerun_paths,
        ))
    })?;

    let mut skip_sep = false;
    let mut sep_delimiter = None;
    match scan_first_non_blank_line(guarded.split(|byte| *byte == b'\n')) {
        SepScan::Directive { delimiter, .. } => {
            sep_delimiter = Some(delimiter);
            skip_sep = true;
        }
        SepScan::FirstNonBlank { .. } | SepScan::NoLines => {}
    }

    let (delimiter, escape) = if let Some(forced) = forced_delimiter {
        let mut cursor = Cursor::new(guarded);
        let escape = detect_escape_mode(&mut cursor, forced).map_err(|err| {
            Box::new(RefusalPayload::with_default_next(
                RefusalCode::CsvParse,
                RefusalKind::CsvParse {
                    file: file_side,
                    line: err.line,
                    column: None,
                },
                rerun_paths,
            ))
        })?;
        (forced, escape)
    } else if let Some(sep) = sep_delimiter {
        let mut cursor = Cursor::new(guarded);
        let escape = detect_escape_mode(&mut cursor, sep).map_err(|err| {
            Box::new(RefusalPayload::with_default_next(
                RefusalCode::CsvParse,
                RefusalKind::CsvParse {
                    file: file_side,
                    line: err.line,
                    column: None,
                },
                rerun_paths,
            ))
        })?;
        (sep, escape)
    } else {
        match auto_detect(guarded) {
            Ok(dialect) => (dialect.delimiter, dialect.escape),
            Err(err) => return Err(Box::new(map_dialect_error(err, file_side, rerun_paths))),
        }
    };

    let mut reader = build_reader(Cursor::new(guarded), delimiter, escape);
    let mut record = ByteRecord::new();
    let mut header: Option<Vec<Vec<u8>>> = None;
    let mut records = Vec::new();
    let mut data_index: u64 = 0;
    let mut skipped_sep = !skip_sep;

    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {
                if header.is_none() {
                    if is_blank_record(&record) && record.len() == 1 {
                        continue;
                    }
                    if !skipped_sep {
                        skipped_sep = true;
                        continue;
                    }
                    let normalized = normalize_headers(record.iter()).map_err(|err| {
                        Box::new(RefusalPayload::with_default_next(
                            RefusalCode::Headers,
                            RefusalKind::Headers {
                                file: file_side,
                                issue: HeadersIssue::Duplicate { name: err.name },
                            },
                            rerun_paths,
                        ))
                    })?;
                    header = Some(normalized);
                    continue;
                }

                if is_blank_record(&record) {
                    continue;
                }

                data_index += 1;
                let header_len = header.as_ref().map(|h| h.len()).unwrap_or(0);
                let normalized =
                    normalize_record(&record, header_len, data_index).map_err(|err| {
                        Box::new(RefusalPayload::with_default_next(
                            RefusalCode::Headers,
                            RefusalKind::Headers {
                                file: file_side,
                                issue: HeadersIssue::ExtraFields {
                                    record: err.record_number,
                                },
                            },
                            rerun_paths,
                        ))
                    })?;
                records.push(owned_record(normalized));
            }
            Ok(false) => break,
            Err(err) => {
                return Err(Box::new(RefusalPayload::with_default_next(
                    RefusalCode::CsvParse,
                    RefusalKind::CsvParse {
                        file: file_side,
                        line: err.position().map(|pos| pos.line()),
                        column: None,
                    },
                    rerun_paths,
                )));
            }
        }
    }

    let headers = match header {
        Some(headers) => headers,
        None => {
            return Err(Box::new(RefusalPayload::with_default_next(
                RefusalCode::Headers,
                RefusalKind::Headers {
                    file: file_side,
                    issue: HeadersIssue::MissingHeader,
                },
                rerun_paths,
            )));
        }
    };

    Ok(ParsedCsv {
        delimiter,
        escape,
        headers,
        records,
    })
}

fn owned_record(normalized: crate::csv::records::NormalizedRecord<'_>) -> OwnedRecord {
    let mut fields = Vec::with_capacity(normalized.len());
    for idx in 0..normalized.len() {
        fields.push(normalized.field(idx).to_vec());
    }
    fields
}

fn map_dialect_error(
    err: DialectError,
    file_side: FileSide,
    paths: RerunPaths<'_>,
) -> RefusalPayload {
    match err {
        DialectError::NoHeader => RefusalPayload::with_default_next(
            RefusalCode::Headers,
            RefusalKind::Headers {
                file: file_side,
                issue: HeadersIssue::MissingHeader,
            },
            paths,
        ),
        DialectError::CsvParse { error } => RefusalPayload::with_default_next(
            RefusalCode::CsvParse,
            RefusalKind::CsvParse {
                file: file_side,
                line: error.as_ref().and_then(|err| err.line),
                column: None,
            },
            paths,
        ),
        DialectError::Ambiguous { tied } => RefusalPayload::with_default_next(
            RefusalCode::Dialect,
            RefusalKind::Dialect {
                file: file_side,
                tied_delimiters: tied.clone(),
                suggestion: DialectSuggestion::SepDirective(tied.first().copied().unwrap_or(b',')),
            },
            paths,
        ),
        DialectError::SingleColumn { delimiter } => RefusalPayload::with_default_next(
            RefusalCode::Dialect,
            RefusalKind::Dialect {
                file: file_side,
                tied_delimiters: vec![delimiter],
                suggestion: DialectSuggestion::ForceDelimiter(delimiter_hint(delimiter)),
            },
            paths,
        ),
    }
}

fn delimiter_hint(delimiter: u8) -> DelimiterHint {
    match delimiter {
        b',' => DelimiterHint::Named(NamedDelimiter::Comma),
        b'\t' => DelimiterHint::Named(NamedDelimiter::Tab),
        b';' => DelimiterHint::Named(NamedDelimiter::Semicolon),
        b'|' => DelimiterHint::Named(NamedDelimiter::Pipe),
        b'^' => DelimiterHint::Named(NamedDelimiter::Caret),
        other => DelimiterHint::Byte(other),
    }
}

fn find_key_index(headers: &[Vec<u8>], key: &[u8]) -> Option<usize> {
    headers.iter().position(|name| name.as_slice() == key)
}

fn map_key_join_error(
    err: KeyJoinError,
    file: FileSide,
    key: &[u8],
    paths: RerunPaths<'_>,
) -> RefusalPayload {
    match err {
        KeyJoinError::EmptyKey { record_number } => RefusalPayload::with_default_next(
            RefusalCode::KeyEmpty,
            RefusalKind::KeyEmpty {
                file,
                record: record_number,
                key_column: key.to_vec(),
            },
            paths,
        ),
        KeyJoinError::DuplicateKey {
            key: key_value,
            second_record,
            ..
        } => RefusalPayload::with_default_next(
            RefusalCode::KeyDup,
            RefusalKind::KeyDup {
                file,
                record: second_record,
                key_value,
            },
            paths,
        ),
        KeyJoinError::KeySetMismatch {
            missing_count,
            extra_count,
            missing_samples,
            extra_samples,
        } => RefusalPayload::with_default_next(
            RefusalCode::KeyMismatch,
            RefusalKind::KeyMismatch {
                missing_in_new: missing_count,
                extra_in_new: extra_count,
                missing_samples,
                extra_samples,
            },
            paths,
        ),
    }
}

fn map_column_error(err: ColumnTypingError<RowRef>, paths: RerunPaths<'_>) -> RefusalPayload {
    match err {
        ColumnTypingError::MixedTypes(detail) => {
            let file = match detail.side {
                ColumnSide::Old => FileSide::Old,
                ColumnSide::New => FileSide::New,
            };
            let key_value = detail.row_id.key.clone();
            RefusalPayload::with_default_next(
                RefusalCode::MixedTypes,
                RefusalKind::MixedTypes {
                    file,
                    record: if key_value.is_some() {
                        None
                    } else {
                        Some(detail.row_id.record_for(detail.side))
                    },
                    column: detail.column,
                    value: detail.value,
                    key_value,
                },
                paths,
            )
        }
        ColumnTypingError::Missingness(detail) => {
            let present_side = match detail.missing_side {
                ColumnSide::Old => ColumnSide::New,
                ColumnSide::New => ColumnSide::Old,
            };
            let file = match present_side {
                ColumnSide::Old => FileSide::Old,
                ColumnSide::New => FileSide::New,
            };
            let key_value = detail.row_id.key.clone();
            RefusalPayload::with_default_next(
                RefusalCode::Missingness,
                RefusalKind::Missingness {
                    file,
                    record: if key_value.is_some() {
                        None
                    } else {
                        Some(detail.row_id.record_for(present_side))
                    },
                    column: detail.column,
                    value: detail.present_value,
                    key_value,
                },
                paths,
            )
        }
    }
}

fn render_refusal(
    refusal: RefusalPayload,
    args: &Args,
    key: Option<&[u8]>,
    dialect_old: Option<DialectReceipt>,
    dialect_new: Option<DialectReceipt>,
    profile: &ProfileRunInfo,
) -> PipelineResult {
    let alignment_mode = match key {
        Some(key) => JsonAlignment::key(encode_identifier_json(key)),
        None => JsonAlignment::row_order(),
    };

    let context = RefusalContext {
        key,
        dialect_old,
        dialect_new,
        alignment: alignment_mode,
        profile: profile.clone(),
        counts: Counts::default(),
        metrics: Metrics::default(),
    };

    render_refusal_with_context(refusal, args, context)
}

fn render_refusal_with_context(
    refusal: RefusalPayload,
    args: &Args,
    context: RefusalContext<'_>,
) -> PipelineResult {
    let old_display = display_name(args.old_path());
    let new_display = display_name(args.new_path());

    let result = if args.json {
        let ctx = json_context(
            args,
            context.alignment,
            context.dialect_old,
            context.dialect_new,
            &context.profile,
            context.counts,
            context.metrics,
        );
        let detail = refusal_detail_json(&refusal.detail);
        let refusal_json = JsonRefusal::new(refusal.code, refusal.code.reason(), detail);
        let output = JsonOutput::refusal(ctx, refusal_json)
            .to_string()
            .unwrap_or_else(|_| "{}".to_string());
        PipelineResult {
            outcome: Outcome::Refusal,
            output,
            profile: context.profile.clone(),
        }
    } else {
        let mut lines = Vec::new();
        lines.push(format!("RVL ERROR ({})", refusal.code));
        lines.push(String::new());
        let alignment_label = context.key.map(render_identifier_human);
        let header = RefusalHeader {
            old_name: &old_display,
            new_name: &new_display,
            alignment: match alignment_label.as_deref() {
                Some(label) => HumanAlignment::Key { column: label },
                None => HumanAlignment::RowOrder,
            },
            profile: to_human_profile(&context.profile),
            dialect_old: context.dialect_old,
            dialect_new: context.dialect_new,
            settings: HumanSettings {
                threshold: args.threshold,
                tolerance: args.tolerance,
            },
        };
        lines.extend(render_refusal_header(&header));
        lines.push(String::new());
        let body = RefusalBody {
            code: refusal.code,
            detail: &refusal.detail,
            old_name: &old_display,
            new_name: &new_display,
        };
        lines.extend(render_refusal_body(&body));
        PipelineResult {
            outcome: Outcome::Refusal,
            output: lines.join("\n"),
            profile: context.profile.clone(),
        }
    };

    capsule::write_capsule(
        args,
        &result,
        &CapsuleRunSummary::refusal(refusal.code.to_string()),
    );
    result
}

fn render_no_real_change(
    args: &Args,
    ctx: JsonContext,
    alignment_label: Option<&str>,
) -> PipelineResult {
    let run_profile = profile_from_json_context(&ctx);
    let result = if args.json {
        let output = JsonOutput::no_real_change(ctx)
            .to_string()
            .unwrap_or_else(|_| "{}".to_string());
        PipelineResult {
            outcome: Outcome::NoRealChange,
            output,
            profile: run_profile.clone(),
        }
    } else {
        let old_display = display_name(args.old_path());
        let new_display = display_name(args.new_path());
        let mut lines = vec![
            "RVL".to_string(),
            String::new(),
            "NO REAL CHANGE".to_string(),
            String::new(),
        ];
        lines.extend(render_human_header_lines(
            args,
            &ctx,
            alignment_label,
            &old_display,
            &new_display,
        ));
        lines.push(String::new());
        let body = NoRealBody {
            max_abs_delta: ctx.metrics.max_abs_delta.unwrap_or(0.0),
            tolerance: args.tolerance,
        };
        lines.extend(render_no_real_body(&body));
        PipelineResult {
            outcome: Outcome::NoRealChange,
            output: lines.join("\n"),
            profile: run_profile.clone(),
        }
    };

    capsule::write_capsule(args, &result, &CapsuleRunSummary::no_real_change());
    result
}

fn render_real_change(
    args: &Args,
    ctx: JsonContext,
    details: &[ContributionDetail],
    coverage: f64,
    alignment_label: Option<&str>,
) -> PipelineResult {
    let run_profile = profile_from_json_context(&ctx);
    let total_change = ctx.metrics.total_change.unwrap_or(0.0);
    let contributor_summary = build_capsule_contributor_summary(details, total_change, coverage);

    let result = if args.json {
        let contributors = build_json_contributors(details, total_change, args.explicit);
        let output = JsonOutput::real_change(ctx, contributors)
            .to_string()
            .unwrap_or_else(|_| "{}".to_string());
        PipelineResult {
            outcome: Outcome::RealChange,
            output,
            profile: run_profile.clone(),
        }
    } else {
        let old_display = display_name(args.old_path());
        let new_display = display_name(args.new_path());
        let mut lines = vec![
            "RVL".to_string(),
            String::new(),
            "REAL CHANGE".to_string(),
            String::new(),
        ];
        lines.extend(render_human_header_lines(
            args,
            &ctx,
            alignment_label,
            &old_display,
            &new_display,
        ));
        lines.push(String::new());
        let contributors = build_human_contributors(details, total_change);
        let body = RealChangeBody {
            contributors: &contributors,
            coverage,
            threshold: args.threshold,
            explicit: args.explicit,
        };
        lines.extend(render_real_change_body(&body));
        PipelineResult {
            outcome: Outcome::RealChange,
            output: lines.join("\n"),
            profile: run_profile.clone(),
        }
    };

    capsule::write_capsule(
        args,
        &result,
        &CapsuleRunSummary::real_change(contributor_summary),
    );
    result
}

fn render_human_header_lines(
    args: &Args,
    ctx: &JsonContext,
    alignment_label: Option<&str>,
    old_name: &str,
    new_name: &str,
) -> Vec<String> {
    let alignment = match alignment_label {
        Some(label) => HumanAlignment::Key { column: label },
        None => HumanAlignment::RowOrder,
    };

    let columns = ColumnCounts {
        common: ctx.counts.columns_common.unwrap_or(0),
        old_only: ctx.counts.columns_old_only.unwrap_or(0),
        new_only: ctx.counts.columns_new_only.unwrap_or(0),
    };

    let checked = CheckedCounts {
        rows: ctx.counts.rows_aligned.unwrap_or(0),
        numeric_columns: ctx.counts.numeric_columns.unwrap_or(0),
        cells: ctx.counts.numeric_cells_checked.unwrap_or(0),
    };

    let dialect_old = ctx
        .dialect
        .old
        .as_ref()
        .map(|dialect| DialectReceipt {
            delimiter: dialect.delimiter.as_bytes()[0],
            quote: dialect.quote.as_bytes()[0],
            escape: dialect.escape.as_ref().map(|s| s.as_bytes()[0]),
        })
        .unwrap_or(DialectReceipt {
            delimiter: b',',
            quote: b'"',
            escape: None,
        });
    let dialect_new = ctx
        .dialect
        .new
        .as_ref()
        .map(|dialect| DialectReceipt {
            delimiter: dialect.delimiter.as_bytes()[0],
            quote: dialect.quote.as_bytes()[0],
            escape: dialect.escape.as_ref().map(|s| s.as_bytes()[0]),
        })
        .unwrap_or(DialectReceipt {
            delimiter: b',',
            quote: b'"',
            escape: None,
        });
    let profile = profile_from_json_context(ctx);

    let header = HumanHeader {
        old_name,
        new_name,
        alignment,
        profile: to_human_profile(&profile),
        columns,
        checked,
        dialect_old,
        dialect_new,
        settings: HumanSettings {
            threshold: args.threshold,
            tolerance: args.tolerance,
        },
    };

    render_real_no_real_header(&header)
}

fn json_context(
    args: &Args,
    alignment: JsonAlignment,
    dialect_old: Option<DialectReceipt>,
    dialect_new: Option<DialectReceipt>,
    profile: &ProfileRunInfo,
    counts: Counts,
    metrics: Metrics,
) -> JsonContext {
    JsonContext {
        files: Files {
            old: args.old_path().to_string_lossy().to_string(),
            new: args.new_path().to_string_lossy().to_string(),
        },
        alignment,
        dialect: Dialect {
            old: dialect_old
                .map(|dialect| DialectSide::new(dialect.delimiter, dialect.quote, dialect.escape)),
            new: dialect_new
                .map(|dialect| DialectSide::new(dialect.delimiter, dialect.quote, dialect.escape)),
        },
        profile_used: profile.used,
        profile_id: profile.profile_id.clone(),
        profile_sha256: profile.profile_sha256.clone(),
        threshold: args.threshold,
        tolerance: args.tolerance,
        counts,
        metrics,
    }
}

fn profile_from_json_context(ctx: &JsonContext) -> ProfileRunInfo {
    ProfileRunInfo {
        used: ctx.profile_used,
        profile_id: ctx.profile_id.clone(),
        profile_sha256: ctx.profile_sha256.clone(),
    }
}

fn to_human_profile<'a>(profile: &'a ProfileRunInfo) -> Option<HumanProfile<'a>> {
    if !profile.used {
        return None;
    }
    match profile.profile_id.as_deref() {
        Some(profile_id) => Some(HumanProfile::Id {
            profile_id,
            profile_sha256: profile.profile_sha256.as_deref(),
        }),
        None => Some(HumanProfile::Draft),
    }
}

fn build_human_contributors(
    details: &[ContributionDetail],
    total_change: f64,
) -> Vec<RealChangeContributor> {
    let mut cumulative = 0.0;
    details
        .iter()
        .map(|detail| {
            let share = if total_change > 0.0 {
                detail.contribution / total_change
            } else {
                0.0
            };
            cumulative += share;
            RealChangeContributor {
                label: render_cell_label(&detail.id),
                old: detail.old,
                new: detail.new,
                delta: detail.delta,
                share,
            }
        })
        .collect()
}

fn build_json_contributors(
    details: &[ContributionDetail],
    total_change: f64,
    explicit: bool,
) -> Vec<crate::output::json::Contributor> {
    let mut contributors = Vec::with_capacity(details.len());
    let mut cumulative = 0.0;
    for detail in details {
        let share = if total_change > 0.0 {
            detail.contribution / total_change
        } else {
            0.0
        };
        cumulative += share;
        contributors.push(crate::output::json::Contributor::from_bytes(
            &row_id_bytes(&detail.id.row_id),
            &detail.id.column,
            detail.old,
            detail.new,
            detail.delta,
            detail.contribution,
            share,
            cumulative,
            explicit,
        ));
    }
    contributors
}

fn build_capsule_contributor_summary(
    details: &[ContributionDetail],
    total_change: f64,
    coverage: f64,
) -> CapsuleContributorSummary {
    let top = details
        .iter()
        .map(|detail| {
            let share = if total_change > 0.0 {
                detail.contribution / total_change
            } else {
                0.0
            };
            CapsuleContributor {
                row_id: encode_identifier_json(&row_id_bytes(&detail.id.row_id)),
                column: encode_identifier_json(&detail.id.column),
                delta: detail.delta,
                contribution: detail.contribution,
                share,
            }
        })
        .collect();

    CapsuleContributorSummary {
        count: details.len(),
        coverage,
        top,
    }
}

fn row_id_bytes(row_id: &RowId) -> Vec<u8> {
    match row_id {
        RowId::RowIndex(index) => index.to_string().into_bytes(),
        RowId::Key(bytes) => bytes.clone(),
    }
}

fn render_cell_label(cell_id: &CellId) -> String {
    let row_label = match &cell_id.row_id {
        RowId::RowIndex(index) => index.to_string(),
        RowId::Key(bytes) => render_identifier_human(bytes),
    };
    let column = render_identifier_human(&cell_id.column);
    format!("{row_label}.{column}")
}

fn scope_intersection(
    intersection: ColumnIntersection,
    include_scope: Option<&HashSet<Vec<u8>>>,
) -> ColumnIntersection {
    let Some(scope) = include_scope else {
        return intersection;
    };

    let common = intersection
        .common
        .into_iter()
        .filter(|column| scope.contains(&column.name))
        .collect();
    let old_only = intersection
        .old_only
        .into_iter()
        .filter(|column| scope.contains(column))
        .collect();
    let new_only = intersection
        .new_only
        .into_iter()
        .filter(|column| scope.contains(column))
        .collect();

    ColumnIntersection {
        common,
        old_only,
        new_only,
    }
}

fn count_columns(
    headers: &[Vec<u8>],
    key: Option<&[u8]>,
    include_scope: Option<&HashSet<Vec<u8>>>,
) -> u64 {
    headers
        .iter()
        .filter(|name| {
            if let Some(scope) = include_scope
                && !scope.contains(name.as_slice())
            {
                return false;
            }
            if let Some(key) = key {
                return name.as_slice() != key;
            }
            true
        })
        .count() as u64
}

fn dialect_receipt(parsed: &ParsedCsv) -> DialectReceipt {
    DialectReceipt {
        delimiter: parsed.delimiter,
        quote: b'"',
        escape: parsed.escape.escape_byte(),
    }
}

fn display_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string_lossy().to_string())
}

fn map_encoding_issue(bytes: &[u8], issue: InputEncodingIssue) -> EncodingIssue {
    match issue {
        InputEncodingIssue::Utf16Or32Bom => {
            if bytes.starts_with(&UTF32_BE_BOM) || bytes.starts_with(&UTF32_LE_BOM) {
                EncodingIssue::Utf32
            } else {
                EncodingIssue::Utf16
            }
        }
        InputEncodingIssue::NulByte => EncodingIssue::NulByte,
    }
}

#[derive(Clone)]
struct ContributionDetail {
    id: CellId,
    old: f64,
    new: f64,
    delta: f64,
    contribution: f64,
}

fn collect_details(
    alignment: &AlignmentContext,
    columns: &[crate::numeric::columns::CommonColumn],
    top: &[crate::diff::heap::Contributor<CellId>],
    tolerance: f64,
) -> Vec<ContributionDetail> {
    let mut details: Vec<Option<ContributionDetail>> = vec![None; top.len()];
    let mut tracker = ToleranceTracker::new(tolerance);

    match alignment {
        AlignmentContext::Key { key_rows, .. } => {
            for row in key_rows.iter() {
                let row_id = RowId::key(row.key.clone());
                for column in columns {
                    let old_raw = row
                        .old
                        .fields
                        .get(column.old_index)
                        .map(|v| v.as_slice())
                        .unwrap_or(b"");
                    let new_raw = row
                        .new
                        .fields
                        .get(column.new_index)
                        .map(|v| v.as_slice())
                        .unwrap_or(b"");
                    if is_missing_token(old_raw) && is_missing_token(new_raw) {
                        continue;
                    }
                    let (old_val, new_val) = match (parse_numeric(old_raw), parse_numeric(new_raw))
                    {
                        (Some(old_val), Some(new_val)) => (old_val, new_val),
                        _ => continue,
                    };
                    let (delta, contribution) = tracker.apply(old_val, new_val);
                    if contribution == 0.0 {
                        continue;
                    }
                    let cell_id = CellId::new(row_id.clone(), column.name.clone());
                    for (idx, top_item) in top.iter().enumerate() {
                        if top_item.id == cell_id {
                            details[idx] = Some(ContributionDetail {
                                id: cell_id.clone(),
                                old: old_val,
                                new: new_val,
                                delta,
                                contribution,
                            });
                        }
                    }
                }
            }
        }
        AlignmentContext::RowOrder {
            old_rows, new_rows, ..
        } => {
            for (idx, (old_row, new_row)) in old_rows.iter().zip(new_rows.iter()).enumerate() {
                let row_id = RowId::row_index(idx + 1);
                for column in columns {
                    let old_raw = old_row
                        .get(column.old_index)
                        .map(|v| v.as_slice())
                        .unwrap_or(b"");
                    let new_raw = new_row
                        .get(column.new_index)
                        .map(|v| v.as_slice())
                        .unwrap_or(b"");
                    if is_missing_token(old_raw) && is_missing_token(new_raw) {
                        continue;
                    }
                    let (old_val, new_val) = match (parse_numeric(old_raw), parse_numeric(new_raw))
                    {
                        (Some(old_val), Some(new_val)) => (old_val, new_val),
                        _ => continue,
                    };
                    let (delta, contribution) = tracker.apply(old_val, new_val);
                    if contribution == 0.0 {
                        continue;
                    }
                    let cell_id = CellId::new(row_id.clone(), column.name.clone());
                    for (idx, top_item) in top.iter().enumerate() {
                        if top_item.id == cell_id {
                            details[idx] = Some(ContributionDetail {
                                id: cell_id.clone(),
                                old: old_val,
                                new: new_val,
                                delta,
                                contribution,
                            });
                        }
                    }
                }
            }
        }
    }

    details.into_iter().flatten().collect()
}

impl RefusalPayload {
    fn with_default_next(code: RefusalCode, kind: RefusalKind, paths: RerunPaths<'_>) -> Self {
        Self {
            code,
            detail: RefusalDetail::with_default_next(kind, paths),
        }
    }
}

fn refusal_detail_json(detail: &RefusalDetail) -> Value {
    match &detail.kind {
        RefusalKind::Io { file, error } => json!({
            "file": file.as_str(),
            "error": error,
        }),
        RefusalKind::Encoding { file, issue } => json!({
            "file": file.as_str(),
            "issue": match issue {
                EncodingIssue::Utf16 => "utf16",
                EncodingIssue::Utf32 => "utf32",
                EncodingIssue::NulByte => "nul_byte",
            },
        }),
        RefusalKind::CsvParse { file, line, column } => json!({
            "file": file.as_str(),
            "line": line,
            "column": column,
        }),
        RefusalKind::Headers { file, issue } => match issue {
            HeadersIssue::MissingHeader => json!({
                "file": file.as_str(),
                "issue": "missing_header",
            }),
            HeadersIssue::Duplicate { name } => json!({
                "file": file.as_str(),
                "issue": "duplicate",
                "name": encode_identifier_json(name),
            }),
            HeadersIssue::ExtraFields { record } => json!({
                "file": file.as_str(),
                "issue": "extra_fields",
                "record": record,
            }),
        },
        RefusalKind::NoKey { key_column } => json!({
            "key_column": encode_identifier_json(key_column),
        }),
        RefusalKind::KeyEmpty {
            file,
            record,
            key_column,
        } => json!({
            "file": file.as_str(),
            "record": record,
            "column": encode_identifier_json(key_column),
        }),
        RefusalKind::KeyDup {
            file,
            record,
            key_value,
        } => json!({
            "file": file.as_str(),
            "record": record,
            "key": encode_identifier_json(key_value),
        }),
        RefusalKind::KeyMismatch {
            missing_in_new,
            extra_in_new,
            missing_samples,
            extra_samples,
        } => json!({
            "missing_in_new": missing_in_new,
            "extra_in_new": extra_in_new,
            "missing_samples": missing_samples.iter().map(|k| encode_identifier_json(k)).collect::<Vec<_>>(),
            "extra_samples": extra_samples.iter().map(|k| encode_identifier_json(k)).collect::<Vec<_>>(),
        }),
        RefusalKind::RowCount {
            rows_old,
            rows_new,
            suggested_keys,
        } => json!({
            "rows_old": rows_old,
            "rows_new": rows_new,
            "suggested_keys": suggested_keys.iter().map(|k| encode_identifier_json(k)).collect::<Vec<_>>(),
        }),
        RefusalKind::NeedKey { suggested_keys } => json!({
            "suggested_keys": suggested_keys.iter().map(|k| encode_identifier_json(k)).collect::<Vec<_>>(),
        }),
        RefusalKind::Dialect {
            file,
            tied_delimiters,
            suggestion,
        } => json!({
            "file": file.as_str(),
            "tied_delimiters": tied_delimiters
                .iter()
                .map(|b| byte_to_string(*b))
                .collect::<Vec<_>>(),
            "suggestion": match suggestion {
                DialectSuggestion::ForceDelimiter(hint) => format!("--delimiter {}", render_hint(*hint)),
                DialectSuggestion::SepDirective(byte) => format!("sep={}", byte_to_string(*byte)),
            },
        }),
        RefusalKind::AmbiguousProfile {
            profile_path,
            profile_id,
        } => json!({
            "profile_path": profile_path,
            "profile_id": profile_id,
        }),
        RefusalKind::ProfileNotFound { profile_id } => json!({
            "profile_id": profile_id,
        }),
        RefusalKind::KeyConflict {
            key_flag,
            profile_key,
        } => json!({
            "key_flag": key_flag,
            "profile_key": profile_key,
        }),
        RefusalKind::MixedTypes {
            file,
            record,
            column,
            value,
            key_value,
        } => {
            let mut obj = json!({
                "file": file.as_str(),
                "column": encode_identifier_json(column),
                "value": encode_identifier_json(value),
            });
            if let Some(record) = record {
                obj["record"] = json!(record);
            }
            if let Some(key) = key_value {
                obj["key"] = json!(encode_identifier_json(key));
            }
            obj
        }
        RefusalKind::NoNumeric => json!({}),
        RefusalKind::Missingness {
            file,
            record,
            column,
            value,
            key_value,
        } => {
            let mut obj = json!({
                "file": file.as_str(),
                "column": encode_identifier_json(column),
                "value": encode_identifier_json(value),
            });
            if let Some(record) = record {
                obj["record"] = json!(record);
            }
            if let Some(key) = key_value {
                obj["key"] = json!(encode_identifier_json(key));
            }
            obj
        }
        RefusalKind::Diffuse {
            top_k_coverage,
            threshold,
        } => json!({
            "top_k_coverage": top_k_coverage,
            "threshold": threshold,
        }),
    }
}

fn render_hint(hint: DelimiterHint) -> String {
    match hint {
        DelimiterHint::Named(name) => match name {
            NamedDelimiter::Comma => "comma".to_string(),
            NamedDelimiter::Tab => "tab".to_string(),
            NamedDelimiter::Semicolon => "semicolon".to_string(),
            NamedDelimiter::Pipe => "pipe".to_string(),
            NamedDelimiter::Caret => "caret".to_string(),
        },
        DelimiterHint::Byte(byte) => format!("0x{byte:02X}"),
    }
}

fn byte_to_string(byte: u8) -> String {
    (byte as char).to_string()
}
