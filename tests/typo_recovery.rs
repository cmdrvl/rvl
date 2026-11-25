//! Generalized typo recovery through the real binary (bd-1gj): one edit away from a flag or
//! subcommand of the command the user is inside gets exactly one `did you mean` hint; far typos
//! and ties get none; every usage error exits 2 with an empty stdout. Same behavior as assess
//! (bd-zka); keep the two in step.

use std::process::{Command, Output};

fn rvl(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .args(args)
        .output()
        .expect("run rvl")
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

fn hints(o: &Output) -> Vec<String> {
    stderr(o)
        .lines()
        .filter(|l| l.starts_with("hint: did you mean"))
        .map(str::to_owned)
        .collect()
}

fn assert_usage_error(o: &Output) {
    assert_eq!(o.status.code(), Some(2), "stderr: {}", stderr(o));
    assert!(o.stdout.is_empty(), "usage errors never write stdout");
}

#[test]
fn flag_typos_one_edit_away_get_one_hint() {
    for (typo, want) in [
        ("--jsno", "--json"),
        ("--josn", "--json"),
        ("--jso", "--json"),
        ("--threshhold", "--threshold"),
        ("--no-witnes", "--no-witness"),
        ("--explict", "--explicit"),
        ("--sheett", "--sheet"),
    ] {
        let o = rvl(&[typo]);
        assert_usage_error(&o);
        assert_eq!(
            hints(&o),
            vec![format!("hint: did you mean `{want}`?")],
            "typo {typo}"
        );
    }
}

#[test]
fn flag_typos_work_after_the_two_inputs_too() {
    let o = rvl(&["old.csv", "new.csv", "--kye", "loan_id"]);
    assert_usage_error(&o);
    assert_eq!(hints(&o), vec!["hint: did you mean `--key`?".to_owned()]);
}

#[test]
fn subcommand_typos_inside_a_group_get_one_hint() {
    for (args, want) in [
        (&["witness", "qurey"][..], "query"),
        (&["witness", "cuont"][..], "count"),
        (&["doctor", "triag"][..], "triage"),
        (&["doctor", "helth"][..], "health"),
    ] {
        let o = rvl(args);
        assert_usage_error(&o);
        assert_eq!(
            hints(&o),
            vec![format!("hint: did you mean `{want}`?")],
            "{args:?}"
        );
    }
}

#[test]
fn a_lone_word_one_edit_from_a_subcommand_gets_one_hint() {
    for (word, want) in [
        ("witnes", "witness"),
        ("capabilites", "capabilities"),
        ("docter", "doctor"),
    ] {
        let o = rvl(&[word]);
        assert_usage_error(&o);
        assert_eq!(
            hints(&o),
            vec![format!("hint: did you mean `{want}`?")],
            "{word}"
        );
    }
}

#[test]
fn candidates_come_from_the_command_the_user_is_in() {
    // `--threshhold` is one edit from the top-level --threshold, but `witness last` has no
    // such flag: suggesting it would send the user to an invalid command.
    let o = rvl(&["witness", "last", "--threshhold"]);
    assert_usage_error(&o);
    assert!(hints(&o).is_empty(), "stderr: {}", stderr(&o));

    // A global flag is valid everywhere, so it is still suggested inside a group.
    let o = rvl(&["witness", "last", "--josn"]);
    assert_usage_error(&o);
    assert_eq!(hints(&o), vec!["hint: did you mean `--json`?".to_owned()]);
}

#[test]
fn far_typos_non_typos_and_file_names_get_no_hint() {
    for args in [
        &["--completely-unrelated"][..],
        &["--js"][..],
        &["witness", "everything"][..],
        &["old.csv"][..],   // a path, not a subcommand
        &["somefile"][..],  // not one edit from any subcommand
        &["doctr.csv"][..], // has an extension: a file
    ] {
        let o = rvl(args);
        assert_usage_error(&o);
        assert!(hints(&o).is_empty(), "{args:?}: {}", stderr(&o));
    }
}

#[test]
fn two_inputs_are_never_reinterpreted_as_a_subcommand() {
    // `witnes` + a second word is a comparison of two paths (refused as missing files), not a
    // typo: no hint, and the exit is the ordinary refusal, not a usage error.
    let o = rvl(&["witnes", "other", "--no-witness"]);
    assert!(hints(&o).is_empty(), "stderr: {}", stderr(&o));
}
