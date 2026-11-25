//! bd-59w: spine-rules v2 family-contract behaviors that the generated golden suite cannot
//! reach on its own: set -e ergonomics, flag aliases, doctor-unhealthy exit, NO_COLOR/TERM=dumb,
//! SOURCE_DATE_EPOCH, and the deprecated spellings kept as aliases.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::Value;

fn temp_dir(label: &str) -> PathBuf {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("rvl_family_{label}_{}_{n}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn rvl_in(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rvl"))
        .args(args)
        .current_dir(dir)
        .output()
        .expect("run rvl")
}

fn rvl(args: &[&str]) -> Output {
    rvl_in(Path::new(env!("CARGO_MANIFEST_DIR")), args)
}

fn old_csv() -> String {
    format!(
        "{}/tests/fixtures/basic_old.csv",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn new_csv() -> String {
    format!(
        "{}/tests/fixtures/basic_new.csv",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn json(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(&out.stdout)))
}

// ── set -e ergonomics ───────────────────────────────────────────────────────

#[test]
fn real_change_exits_one_by_default_and_zero_with_exit_zero_on_change() {
    let (old, new) = (old_csv(), new_csv());
    let default = rvl(&[&old, &new, "--no-witness", "--json"]);
    assert_eq!(
        default.status.code(),
        Some(1),
        "REAL_CHANGE defaults to exit 1"
    );
    assert_eq!(json(&default)["outcome"], "REAL_CHANGE");

    let opted = rvl(&[
        &old,
        &new,
        "--no-witness",
        "--json",
        "--exit-zero-on-change",
    ]);
    assert_eq!(opted.status.code(), Some(0));
    // The verdict itself is unchanged; only the process exit code moves.
    assert_eq!(json(&opted)["outcome"], "REAL_CHANGE");
    assert_eq!(opted.stdout, default.stdout);
}

#[test]
fn exit_zero_on_change_never_masks_a_refusal() {
    let out = rvl(&[
        "/nonexistent/old.csv",
        "/nonexistent/new.csv",
        "--no-witness",
        "--json",
        "--exit-zero-on-change",
    ]);
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(json(&out)["outcome"], "REFUSAL");
}

#[test]
fn exit_zero_on_change_is_declared_in_the_contract() {
    let contract = json(&rvl(&["capabilities", "--json"]));
    assert_eq!(contract["exit_codes"]["1"]["domain"], "expected_change");
    let flags: Vec<&str> = contract["options"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|o| o["flag"].as_str())
        .collect();
    assert!(flags.contains(&"--exit-zero-on-change"), "{flags:?}");
}

// ── --capsule-dir canonical, --capsule-out alias ────────────────────────────

fn capsule_dirs(root: &Path) -> Vec<String> {
    fs::read_dir(root)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("capsule-"))
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn capsule_dir_and_deprecated_capsule_out_both_write_a_capsule() {
    let (old, new) = (old_csv(), new_csv());
    for flag in ["--capsule-dir", "--capsule-out"] {
        let root = temp_dir("capsule");
        let out = rvl(&[&old, &new, "--no-witness", flag, root.to_str().unwrap()]);
        assert_eq!(
            out.status.code(),
            Some(1),
            "{flag}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            capsule_dirs(&root).len(),
            1,
            "{flag} must create exactly one capsule dir"
        );
    }
    // Control: with neither flag no capsule is written, so the assertion above is not vacuous.
    let root = temp_dir("capsule_none");
    rvl(&[&old, &new, "--no-witness"]);
    assert!(capsule_dirs(&root).is_empty());
}

#[test]
fn help_names_capsule_dir_as_canonical() {
    let help = String::from_utf8_lossy(&rvl(&["--help"]).stdout).into_owned();
    assert!(help.contains("--capsule-dir"), "{help}");
}

// ── doctor: cmdrvl.doctor.v1, unhealthy exits 1 ─────────────────────────────

/// A directory that looks like an rvl source checkout but does not ignore `.doctor/`,
/// which makes the `source-gitignore-doctor` check fail.
fn unhealthy_checkout() -> PathBuf {
    let dir = temp_dir("unhealthy");
    fs::write(dir.join("Cargo.toml"), "[package]\nname = \"rvl\"\n").unwrap();
    fs::write(dir.join("operator.json"), "{}").unwrap();
    dir
}

#[test]
fn doctor_unhealthy_exits_one_with_error_finding_and_fix() {
    let dir = unhealthy_checkout();
    let out = rvl_in(&dir, &["doctor", "--json"]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "unhealthy doctor must exit 1, never 2"
    );
    let report = json(&out);
    assert_eq!(report["schema_version"], "cmdrvl.doctor.v1");
    assert_eq!(report["mode"], "health");
    assert_eq!(report["healthy"], false);
    let errors: Vec<&Value> = report["findings"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|f| f["severity"] == "error")
        .collect();
    assert_eq!(errors.len(), 1, "{report}");
    assert_eq!(errors[0]["id"], "source-gitignore-doctor");
    assert!(errors[0]["fix"].as_str().unwrap().contains(".doctor/"));
}

#[test]
fn doctor_healthy_when_gitignore_covers_doctor_dir() {
    let dir = unhealthy_checkout();
    fs::write(dir.join(".gitignore"), ".doctor/\n").unwrap();
    let out = rvl_in(&dir, &["doctor", "--json"]);
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(json(&out)["healthy"], true);
}

#[test]
fn doctor_triage_unhealthy_has_next_actions_and_exit_one() {
    let dir = unhealthy_checkout();
    let out = rvl_in(&dir, &["doctor", "triage", "--json"]);
    assert_eq!(out.status.code(), Some(1));
    let report = json(&out);
    assert_eq!(report["mode"], "triage");
    assert!(!report["next_actions"].as_array().unwrap().is_empty());
}

#[test]
fn exit_zero_on_change_does_not_mask_an_unhealthy_doctor() {
    let dir = unhealthy_checkout();
    let out = rvl_in(&dir, &["--exit-zero-on-change", "doctor", "--json"]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "flag must affect the verdict only, never doctor"
    );
}

#[test]
fn doctor_capabilities_embeds_the_capabilities_contract() {
    let report = json(&rvl(&["doctor", "capabilities", "--json"]));
    assert_eq!(report["mode"], "capabilities");
    assert_eq!(report["contract"], json(&rvl(&["capabilities", "--json"])));
}

#[test]
fn deprecated_spellings_still_emit_doctor_v1() {
    // Old triage spellings and `doctor health --json` keep working as aliases.
    for args in [&["--robot-triage"][..], &["doctor", "--robot-triage"][..]] {
        let report = json(&rvl(args));
        assert_eq!(report["schema_version"], "cmdrvl.doctor.v1", "{args:?}");
        assert_eq!(report["mode"], "triage", "{args:?}");
    }
    let health = json(&rvl(&["doctor", "health", "--json"]));
    assert_eq!(health["schema_version"], "cmdrvl.doctor.v1");
    assert_eq!(health["mode"], "health");
}

// ── NO_COLOR / TERM=dumb / non-TTY ──────────────────────────────────────────

fn has_ansi(out: &Output) -> bool {
    let esc = |b: &[u8]| b.windows(2).any(|w| w == b"\x1b[");
    esc(&out.stdout) || esc(&out.stderr)
}

fn run_with_env(args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rvl"));
    cmd.args(args).env_remove("NO_COLOR").env_remove("TERM");
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.output().expect("run rvl")
}

#[test]
fn forced_color_is_stripped_by_no_color_and_term_dumb() {
    // Positive control: when color is forced and nothing disables it, clap output is styled.
    // If this ever stops holding, the two assertions below would pass vacuously.
    let forced = run_with_env(&["--bogus-flag"], &[("CLICOLOR_FORCE", "1")]);
    assert!(
        has_ansi(&forced),
        "control: CLICOLOR_FORCE should style clap output"
    );

    for env in [
        &[("CLICOLOR_FORCE", "1"), ("NO_COLOR", "1")][..],
        &[("CLICOLOR_FORCE", "1"), ("TERM", "dumb")][..],
    ] {
        for args in [&["--bogus-flag"][..], &["--help"][..]] {
            let out = run_with_env(args, env);
            assert!(!has_ansi(&out), "ANSI leaked for {args:?} with {env:?}");
        }
    }
}

// ── SOURCE_DATE_EPOCH ───────────────────────────────────────────────────────

fn last_witness_ts(ledger: &Path) -> String {
    let text = fs::read_to_string(ledger).expect("ledger written");
    let line = text.lines().last().expect("one record");
    serde_json::from_str::<Value>(line).unwrap()["ts"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn witness_timestamp_honors_source_date_epoch() {
    let (old, new) = (old_csv(), new_csv());
    let run = |label: &str, epoch: Option<&str>| {
        let ledger = temp_dir(label).join("witness.jsonl");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_rvl"));
        cmd.args([old.as_str(), new.as_str()])
            .env("EPISTEMIC_WITNESS", &ledger)
            .env_remove("SOURCE_DATE_EPOCH");
        if let Some(epoch) = epoch {
            cmd.env("SOURCE_DATE_EPOCH", epoch);
        }
        cmd.output().expect("run rvl");
        last_witness_ts(&ledger)
    };

    // 1700000000 == 2023-11-14T22:13:20Z
    assert_eq!(run("sde_a", Some("1700000000")), "2023-11-14T22:13:20Z");
    assert_eq!(run("sde_b", Some("1700000000")), "2023-11-14T22:13:20Z");
    // Control: without the variable the wall clock is used, so the pinned value is not a constant.
    assert_ne!(run("sde_none", None), "2023-11-14T22:13:20Z");
}
