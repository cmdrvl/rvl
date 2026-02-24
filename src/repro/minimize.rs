use std::collections::HashSet;

const DEFAULT_MAX_ROWS: usize = 64;
const DEFAULT_MAX_COLUMNS: usize = 16;
const HARD_MAX_ROWS: usize = 1024;
const HARD_MAX_COLUMNS: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReproOutcome {
    RealChange,
    NoRealChange,
    Refusal,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RowAnchor {
    RowIndex(u64),
    Key(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowSelectionInput<'a> {
    pub candidate_rows: &'a [RowAnchor],
    pub contributor_rows: &'a [RowAnchor],
    pub refusal_rows: &'a [RowAnchor],
    pub max_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSelectionInput<'a> {
    pub key_column: Option<&'a [u8]>,
    pub numeric_columns: &'a [Vec<u8>],
    pub contributor_columns: &'a [Vec<u8>],
    pub refusal_columns: &'a [Vec<u8>],
    pub max_columns: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection<T> {
    pub items: Vec<T>,
    pub truncated: bool,
}

pub fn select_rows(outcome: ReproOutcome, input: RowSelectionInput<'_>) -> Selection<RowAnchor> {
    let limit = effective_limit(input.max_rows, DEFAULT_MAX_ROWS, HARD_MAX_ROWS);
    let mut selected = Vec::new();
    let mut seen = HashSet::new();

    match outcome {
        ReproOutcome::RealChange => {
            extend_unique_rows(&mut selected, &mut seen, input.contributor_rows);
            extend_unique_rows(&mut selected, &mut seen, input.refusal_rows);
            extend_unique_rows(&mut selected, &mut seen, input.candidate_rows);
        }
        ReproOutcome::NoRealChange => {
            extend_unique_rows(&mut selected, &mut seen, input.candidate_rows);
        }
        ReproOutcome::Refusal => {
            extend_unique_rows(&mut selected, &mut seen, input.refusal_rows);
            extend_unique_rows(&mut selected, &mut seen, input.candidate_rows);
        }
    }

    if selected.is_empty()
        && let Some(first) = first_row_anchor(
            input.candidate_rows,
            input.contributor_rows,
            input.refusal_rows,
        )
    {
        selected.push(first.clone());
    }

    let truncated = selected.len() > limit;
    selected.truncate(limit);

    Selection {
        items: selected,
        truncated,
    }
}

pub fn select_columns(
    outcome: ReproOutcome,
    input: ColumnSelectionInput<'_>,
) -> Selection<Vec<u8>> {
    let limit = effective_limit(input.max_columns, DEFAULT_MAX_COLUMNS, HARD_MAX_COLUMNS);
    let mut selected = Vec::new();
    let mut seen = HashSet::new();

    if let Some(key_column) = input.key_column {
        push_unique_column(&mut selected, &mut seen, key_column);
    }

    match outcome {
        ReproOutcome::RealChange => {
            extend_unique_columns(&mut selected, &mut seen, input.contributor_columns);
            extend_unique_columns(&mut selected, &mut seen, input.refusal_columns);
            extend_unique_columns(&mut selected, &mut seen, input.numeric_columns);
        }
        ReproOutcome::NoRealChange => {
            extend_unique_columns(&mut selected, &mut seen, input.numeric_columns);
        }
        ReproOutcome::Refusal => {
            extend_unique_columns(&mut selected, &mut seen, input.refusal_columns);
            extend_unique_columns(&mut selected, &mut seen, input.numeric_columns);
        }
    }

    if selected.is_empty()
        && let Some(first) = first_column(
            input.numeric_columns,
            input.contributor_columns,
            input.refusal_columns,
            input.key_column,
        )
    {
        selected.push(first.to_vec());
    }

    let truncated = selected.len() > limit;
    selected.truncate(limit);

    Selection {
        items: selected,
        truncated,
    }
}

fn effective_limit(requested: usize, default_value: usize, hard_max: usize) -> usize {
    let base = if requested == 0 {
        default_value
    } else {
        requested
    };
    base.min(hard_max).max(1)
}

fn extend_unique_rows(
    target: &mut Vec<RowAnchor>,
    seen: &mut HashSet<RowAnchor>,
    rows: &[RowAnchor],
) {
    for row in rows {
        if seen.insert(row.clone()) {
            target.push(row.clone());
        }
    }
}

fn extend_unique_columns(
    target: &mut Vec<Vec<u8>>,
    seen: &mut HashSet<Vec<u8>>,
    columns: &[Vec<u8>],
) {
    for column in columns {
        push_unique_column(target, seen, column);
    }
}

fn push_unique_column(target: &mut Vec<Vec<u8>>, seen: &mut HashSet<Vec<u8>>, column: &[u8]) {
    let value = column.to_vec();
    if seen.insert(value.clone()) {
        target.push(value);
    }
}

fn first_row_anchor<'a>(
    candidates: &'a [RowAnchor],
    contributors: &'a [RowAnchor],
    refusals: &'a [RowAnchor],
) -> Option<&'a RowAnchor> {
    candidates
        .first()
        .or_else(|| contributors.first())
        .or_else(|| refusals.first())
}

fn first_column<'a>(
    numeric_columns: &'a [Vec<u8>],
    contributor_columns: &'a [Vec<u8>],
    refusal_columns: &'a [Vec<u8>],
    key_column: Option<&'a [u8]>,
) -> Option<&'a [u8]> {
    key_column
        .or_else(|| numeric_columns.first().map(|value| value.as_slice()))
        .or_else(|| contributor_columns.first().map(|value| value.as_slice()))
        .or_else(|| refusal_columns.first().map(|value| value.as_slice()))
}
