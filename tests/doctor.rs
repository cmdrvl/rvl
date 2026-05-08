use std::process::{Command, Output};

fn rvl_binary() -> &'static str {
    env!("CARGO_BIN_EXE_rvl")
}

fn run(args: &[&str]) -> Output {
    Command::new(rvl_binary())
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("failed to run rvl")
}

#[test]
fn help_routes_exit_success() {
    for args in [
        &["--help"][..],
        &["witness", "--help"][..],
        &["doctor", "--help"][..],
        &["doctor", "capabilities", "--help"][..],
    ] {
        let output = run(args);
        assert_eq!(
            output.status.code(),
            Some(0),
            "help route should exit 0: {args:?}"
        );
        assert!(output.stderr.is_empty(), "stderr should remain empty");
        assert!(!output.stdout.is_empty(), "help should print to stdout");
    }
}

#[test]
fn doctor_health_is_read_only_and_successful() {
    let output = run(&["doctor", "health"]);

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("rvl doctor healthy"));
    assert!(output.stderr.is_empty(), "stderr should remain empty");
}

#[test]
fn doctor_capabilities_json_declares_read_only_contract() {
    let output = run(&["doctor", "capabilities", "--json"]);

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty(), "stderr should remain empty");
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("capabilities should be JSON");

    assert_eq!(value["schema_version"], "rvl.doctor.capabilities.v1");
    assert_eq!(value["tool"], "rvl");
    assert_eq!(value["read_only"], true);
    assert_eq!(
        value["fixers"]
            .as_array()
            .expect("fixers should be an array")
            .len(),
        0
    );

    let commands = value["commands"]
        .as_array()
        .expect("commands should be an array");
    for expected in [
        "rvl doctor health",
        "rvl doctor capabilities --json",
        "rvl doctor robot-docs",
        "rvl doctor --robot-triage",
    ] {
        assert!(
            commands
                .iter()
                .any(|command| command["command"].as_str() == Some(expected)),
            "missing command capability {expected}"
        );
    }
}

#[test]
fn describe_includes_doctor_surface() {
    let output = run(&["--describe"]);

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty(), "stderr should remain empty");
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("describe should be JSON");
    let subcommands = value["subcommands"]
        .as_array()
        .expect("subcommands should be an array");
    let doctor = subcommands
        .iter()
        .find(|command| command["name"].as_str() == Some("doctor"))
        .expect("operator.json should describe doctor");

    assert_eq!(doctor["current_runtime_behavior"]["read_only"], true);
    assert_eq!(
        doctor["current_runtime_behavior"]["fix_mode"],
        "not_available"
    );
}

#[test]
fn doctor_robot_docs_names_agent_surface() {
    let output = run(&["doctor", "robot-docs"]);

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty(), "stderr should remain empty");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("rvl doctor health"));
    assert!(stdout.contains("rvl doctor capabilities --json"));
    assert!(stdout.contains("no doctor --fix surface exists yet"));
}

#[test]
fn doctor_robot_triage_is_single_call_json() {
    let output = run(&["doctor", "--robot-triage"]);

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty(), "stderr should remain empty");
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("robot triage should be JSON");

    assert_eq!(value["schema_version"], "rvl.doctor.v1");
    assert_eq!(value["summary"]["status"], "healthy");
    assert_eq!(value["read_only"], true);
    assert_eq!(value["actions_planned"].as_array().unwrap().len(), 0);
    assert_eq!(
        value["capabilities_url"],
        "command:rvl doctor capabilities --json"
    );
}

#[test]
fn doctor_fix_surface_is_not_present() {
    let output = run(&["doctor", "--fix"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(
        output.stdout.is_empty(),
        "usage errors should not emit stdout"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unexpected argument '--fix'"));
}

#[test]
fn doctor_runtime_artifacts_are_gitignored() {
    let gitignore = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/.gitignore"))
        .expect(".gitignore should be readable");

    assert!(gitignore.lines().any(|line| line.trim() == ".doctor/"));
}
