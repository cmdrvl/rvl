//! bd-yoi: an empty or absent ledger is a successful empty read (`last` prints `null`, query `[]`,
//! count `{"count":0}`; all exit 0), an invalid-UTF-8 line is skipped, and an unreadable ledger (a
//! directory, mode 000) is a refusal: exit 2, empty stdout, one stderr line naming the path. Every
//! case runs the binary as a child with a hard 5 s kill so a hang fails instead of blocking.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rvl-ledger-read-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

/// `None` means the child did not exit within 5 seconds and was killed.
fn rvl_with_timeout(ledger: &Path, args: &[&str]) -> Option<Output> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rvl"))
        .env("EPISTEMIC_WITNESS", ledger)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rvl");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait().expect("try_wait").is_some() {
            return Some(child.wait_with_output().expect("collect output"));
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill hung child");
            child.wait().expect("reap child");
            return None;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn run(ledger: &Path, args: &[&str]) -> Output {
    let output = rvl_with_timeout(ledger, args);
    assert!(output.is_some(), "rvl {} hung", args.join(" "));
    output.expect("checked above")
}

fn assert_empty_reads_succeed(ledger: &Path) {
    let last = run(ledger, &["witness", "last", "--json"]);
    assert_eq!(last.status.code(), Some(0), "last: {:?}", last);
    assert_eq!(stdout(&last), "null");
    let human = run(ledger, &["witness", "last"]);
    assert_eq!(human.status.code(), Some(0), "human last");
    assert!(
        human.stdout.is_empty(),
        "human last prints its note on stderr"
    );
    assert!(
        String::from_utf8_lossy(&human.stderr).contains("empty"),
        "human last must say the ledger is empty"
    );
    let query = run(ledger, &["witness", "query", "--json"]);
    assert_eq!(
        (query.status.code(), stdout(&query).as_str()),
        (Some(0), "[]")
    );
    let count = run(ledger, &["witness", "count", "--json"]);
    assert_eq!(
        (count.status.code(), stdout(&count).as_str()),
        (Some(0), r#"{"count":0}"#)
    );
}

#[test]
fn absent_ledger_is_a_successful_empty_read() {
    let dir = scratch("absent");
    assert_empty_reads_succeed(&dir.join("absent.jsonl"));
}

#[test]
fn empty_ledger_is_a_successful_empty_read() {
    let dir = scratch("empty");
    let ledger = dir.join("empty.jsonl");
    std::fs::write(&ledger, "").expect("write");
    assert_empty_reads_succeed(&ledger);
}

#[test]
fn a_non_utf8_line_is_skipped_and_the_rest_is_still_read() {
    let dir = scratch("non-utf8");
    let ledger = dir.join("witness.jsonl");
    // Produce two real records with real runs, then splice an invalid-UTF-8 line between them.
    let old = dir.join("old.csv");
    let new = dir.join("new.csv");
    std::fs::write(&old, "id,v\nA,1\nB,2\n").expect("write");
    std::fs::write(&new, "id,v\nA,1\nB,9\n").expect("write");
    for _ in 0..2 {
        let status = Command::new(env!("CARGO_BIN_EXE_rvl"))
            .env("EPISTEMIC_WITNESS", &ledger)
            .args([
                old.to_str().unwrap(),
                new.to_str().unwrap(),
                "--key",
                "id",
                "--json",
            ])
            .output()
            .expect("run rvl");
        assert_eq!(status.status.code(), Some(1));
    }
    let text = std::fs::read(&ledger).expect("ledger exists");
    let first_newline = text.iter().position(|b| *b == b'\n').expect("newline") + 1;
    let mut bytes = text[..first_newline].to_vec();
    bytes.extend_from_slice(&[0xff, 0xfe, b'\n']);
    bytes.extend_from_slice(&text[first_newline..]);
    std::fs::write(&ledger, bytes).expect("write");

    let count = run(&ledger, &["witness", "count", "--json"]);
    assert_eq!(count.status.code(), Some(0));
    assert_eq!(stdout(&count), r#"{"count":2}"#);
    let last = run(&ledger, &["witness", "last", "--json"]);
    assert_eq!(last.status.code(), Some(0));
    assert!(stdout(&last).starts_with('{'), "last must be a record");
}

fn assert_unreadable_refuses(ledger: &Path) {
    for args in [
        &["witness", "last", "--json"][..],
        &["witness", "query", "--json"][..],
        &["witness", "count", "--json"][..],
        &["witness", "last"][..],
    ] {
        let output = run(ledger, args);
        assert_eq!(output.status.code(), Some(2), "rvl {}", args.join(" "));
        assert!(output.stdout.is_empty(), "stdout must stay empty");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&ledger.display().to_string()),
            "stderr must name the path: {stderr}"
        );
        assert_eq!(
            stderr.trim().lines().count(),
            1,
            "one stderr line: {stderr}"
        );
    }
}

#[test]
fn a_directory_ledger_is_a_refusal_not_an_empty_result() {
    let dir = scratch("directory").join("a_directory");
    std::fs::create_dir_all(&dir).expect("dir");
    assert_unreadable_refuses(&dir);
}

#[cfg(unix)]
#[test]
fn a_mode_000_ledger_is_a_refusal_not_an_empty_result() {
    use std::os::unix::fs::PermissionsExt;
    let dir = scratch("chmod");
    let ledger = dir.join("witness.jsonl");
    std::fs::write(&ledger, "{}\n").expect("write");
    std::fs::set_permissions(&ledger, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    // A process that can read mode-000 files (root) cannot exercise this case.
    if std::fs::File::open(&ledger).is_ok() {
        std::fs::set_permissions(&ledger, std::fs::Permissions::from_mode(0o600)).ok();
        return;
    }
    assert_unreadable_refuses(&ledger);
    std::fs::set_permissions(&ledger, std::fs::Permissions::from_mode(0o600)).expect("restore");
}
