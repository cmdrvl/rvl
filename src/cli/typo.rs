//! Typo recovery for unknown flags and subcommands: one consistent behavior across the
//! CMD+RVL tools (first implemented in `assess`, bd-zka; keep this logic identical).
//!
//! A usage error gets exactly one extra stderr line, `hint: did you mean `X`?`, when exactly
//! one candidate is one edit away from the unknown token. Candidates come only from the command
//! the user was inside (plus the global flags clap propagates), so a hint is always a valid
//! thing to type. Ties and far misses get no hint: a hint must never be a guess. The exit
//! code (2) and the empty stdout are unchanged.

use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Command, CommandFactory};

/// Optimal-string-alignment distance: insert, delete, substitute or swap two adjacent
/// characters each cost 1 (so `--jsno` is one edit from `--json`).
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut d = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    d[0] = (0..=b.len()).collect();
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            d[i][j] = (d[i - 1][j] + 1)
                .min(d[i][j - 1] + 1)
                .min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                d[i][j] = d[i][j].min(d[i - 2][j - 2] + 1);
            }
        }
    }
    d[a.len()][b.len()]
}

/// `Some(candidate)` only when exactly one candidate is one edit from `token`.
fn unique_one_edit_away(
    token: &str,
    candidates: impl IntoIterator<Item = String>,
) -> Option<String> {
    let mut close = candidates
        .into_iter()
        .filter(|candidate| edit_distance(token, candidate) == 1);
    match (close.next(), close.next()) {
        (Some(only), None) => Some(only),
        _ => None,
    }
}

/// Subcommand names (visible only, plus `help`) of `scope`.
fn subcommand_candidates(scope: &Command) -> std::collections::BTreeSet<String> {
    let mut candidates = std::collections::BTreeSet::new();
    candidates.insert("help".to_owned());
    for sub in scope.get_subcommands().filter(|sub| !sub.is_hide_set()) {
        candidates.insert(sub.get_name().to_owned());
    }
    candidates
}

/// The one candidate exactly one edit away from an unknown long flag or subcommand name,
/// drawn from the command the user was actually inside.
pub fn did_you_mean<C: CommandFactory>(error: &clap::Error) -> Option<String> {
    let context = match error.kind() {
        ErrorKind::UnknownArgument => ContextKind::InvalidArg,
        ErrorKind::InvalidSubcommand => ContextKind::InvalidSubcommand,
        _ => return None,
    };
    let token = match error.get(context)? {
        ContextValue::String(value) => value.split_whitespace().next()?.to_owned(),
        _ => return None,
    };

    // Resolve the command path from clap's usage line ("Usage: rvl witness [OPTIONS]").
    let usage = error
        .get(ContextKind::Usage)
        .map(ToString::to_string)
        .unwrap_or_default();
    let mut command = C::command();
    command.build();
    let mut scope = command.clone();
    for word in usage.split_whitespace().skip(2) {
        match scope.find_subcommand(word) {
            Some(sub) => scope = sub.clone(),
            None => break,
        }
    }

    let mut candidates = std::collections::BTreeSet::new();
    if token.starts_with("--") {
        candidates.insert("--help".to_owned());
        for arg in scope.get_arguments() {
            if arg.is_hide_set() {
                continue;
            }
            if let Some(long) = arg.get_long() {
                candidates.insert(format!("--{long}"));
            }
            for alias in arg.get_visible_aliases().into_iter().flatten() {
                candidates.insert(format!("--{alias}"));
            }
        }
    } else if !token.starts_with('-') {
        candidates = subcommand_candidates(&scope);
    } else {
        return None;
    }

    unique_one_edit_away(&token, candidates)
}

/// A bare word given where rvl expected `<OLD> <NEW>` (so only one was given) that is one edit
/// from a top-level subcommand: `rvl witnes` is far more likely a typo than a file name.
pub fn did_you_mean_subcommand<C: CommandFactory>(word: &str) -> Option<String> {
    if word.starts_with('-') || word.contains(['/', '.']) {
        return None;
    }
    let mut command = C::command();
    command.build();
    unique_one_edit_away(word, subcommand_candidates(&command))
}

/// The stderr line for a hint.
pub fn hint_line(candidate: &str) -> String {
    format!("hint: did you mean `{candidate}`?")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_counts_a_swap_as_one_edit() {
        assert_eq!(edit_distance("--jsno", "--json"), 1);
        assert_eq!(edit_distance("--jso", "--json"), 1);
        assert_eq!(edit_distance("--js", "--json"), 2);
        assert_eq!(edit_distance("same", "same"), 0);
    }

    #[test]
    fn a_tie_is_no_hint() {
        let none = unique_one_edit_away("--ab", ["--aa".to_owned(), "--ac".to_owned()]);
        assert_eq!(none, None);
        let one = unique_one_edit_away("--ab", ["--aa".to_owned(), "--zzz".to_owned()]);
        assert_eq!(one.as_deref(), Some("--aa"));
    }
}
