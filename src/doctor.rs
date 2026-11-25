use std::path::Path;

use serde_json::{Value, json};

use crate::cli::args::{DoctorAction, DoctorArgs, RobotDocsAction};

/// Schema id shared by every spine tool's doctor reports (spine-rules `cmdrvl.doctor.v1`).
const DOCTOR_SCHEMA_VERSION: &str = "cmdrvl.doctor.v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DoctorMode {
    Health,
    Capabilities,
    Triage,
}

impl DoctorMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::Capabilities => "capabilities",
            Self::Triage => "triage",
        }
    }
}

pub fn run(args: &DoctorArgs, json_output: bool) -> Result<u8, Box<dyn std::error::Error>> {
    if args.fix {
        return fix_unavailable();
    }

    let json = args.json || json_output;

    if args.robot_triage {
        return emit_report(DoctorMode::Triage);
    }

    match &args.action {
        Some(DoctorAction::Health(health_args)) => health(health_args.json || json),
        Some(DoctorAction::Capabilities(capabilities_args)) => {
            if capabilities_args.json || json {
                emit_report(DoctorMode::Capabilities)
            } else {
                emit_capabilities(false)
            }
        }
        Some(DoctorAction::Triage(triage_args)) => {
            if triage_args.json || json {
                emit_report(DoctorMode::Triage)
            } else {
                human_triage()
            }
        }
        Some(DoctorAction::RobotDocs) => emit_robot_docs(None),
        None if json => emit_report(DoctorMode::Health),
        None => human_triage(),
    }
}

fn health(json: bool) -> Result<u8, Box<dyn std::error::Error>> {
    if json {
        return emit_report(DoctorMode::Health);
    }
    let report = build_report(DoctorMode::Health);
    println!(
        "rvl doctor {}: {} checks passed, {} findings",
        report.status(),
        report.checks_passed,
        report.error_count()
    );
    Ok(report.exit_code())
}

fn human_triage() -> Result<u8, Box<dyn std::error::Error>> {
    let report = build_report(DoctorMode::Triage);
    println!("RVL DOCTOR");
    println!();
    println!("Status: {}", report.status());
    println!("Checks passed: {}", report.checks_passed);
    println!("Findings: {}", report.error_count());
    if report.error_count() > 0 {
        println!();
        for check in report.checks.iter().filter(|check| !check.pass) {
            println!("- {}: {}", check.id, check.message);
            println!("  next: {}", check.fix);
        }
    }
    println!();
    println!("Next: rvl capabilities --json");
    Ok(report.exit_code())
}

/// `rvl capabilities --json` is the operator.v1 contract, byte-identical to `--describe`.
pub fn emit_capabilities(json: bool) -> Result<u8, Box<dyn std::error::Error>> {
    if json {
        println!("{}", crate::OPERATOR_JSON);
    } else {
        let contract = operator_contract();
        println!("rvl capabilities");
        println!(
            "schema_version: {}",
            contract["schema_version"].as_str().unwrap_or("?")
        );
        println!(
            "contract_version: {}",
            contract["contract_version"].as_str().unwrap_or("?")
        );
        println!("json: rvl capabilities --json");
    }
    Ok(0)
}

pub fn emit_robot_docs(action: Option<&RobotDocsAction>) -> Result<u8, Box<dyn std::error::Error>> {
    match action {
        Some(RobotDocsAction::Guide) | None => {}
    }

    println!("# rvl robot-docs guide");
    println!();
    println!(
        "rvl's agent discovery surface is read-only. The discovery commands never repair files, delete files, run network probes, write witness ledgers, create capsules, or change CSV comparison behavior."
    );
    println!();
    println!("Commands:");
    println!("- rvl capabilities --json");
    println!("- rvl robot-docs guide");
    println!("- rvl --json <old.csv> <new.csv>");
    println!("- rvl doctor [--json]");
    println!("- rvl doctor capabilities --json");
    println!("- rvl doctor triage --json");
    println!("- rvl doctor robot-docs");
    println!("- rvl doctor --fix");
    println!();
    println!("Deprecated aliases (still work; same cmdrvl.doctor.v1 output):");
    println!("- rvl --robot-triage  ->  rvl doctor triage --json");
    println!("- rvl doctor --robot-triage  ->  rvl doctor triage --json");
    println!("- rvl doctor health [--json]  ->  rvl doctor [--json]");
    println!("- --capsule-out  ->  --capsule-dir");
    println!();
    println!("Exit codes:");
    println!("- 0: NO_REAL_CHANGE (doctor: healthy)");
    println!("- 1: REAL_CHANGE, a normal expected outcome (doctor: unhealthy, an error finding)");
    println!("- 2: refusal or command-line usage error");
    println!();
    println!("set -e safety:");
    println!(
        "- Pass `--exit-zero-on-change` to exit 0 on REAL_CHANGE (refusals still exit 2, doctor is unaffected), or guard the call: `rvl a.csv b.csv || [ $? -eq 1 ]`."
    );
    println!();
    println!("Redaction (--explicit):");
    println!(
        "- Default output is redacted for zero-retention safety: labels, counts and shares only, no raw values."
    );
    println!(
        "- Pass `--explicit` to show raw values (old/new/delta, key samples, audit field text), e.g. `rvl <old.csv> <new.csv> --key <column> --json --explicit`."
    );
    println!(
        "- Use `--explicit` for local non-sensitive runs; omit it for logged, shared, or veil/airlock zero-retention workflows."
    );
    println!(
        "- `--non-numeric` default markers are ordinal refs (`redacted:ref:v1..` for values, `redacted:ref:k1..` for rows), deterministic and hash-free."
    );
    println!(
        "- Opt-in stable markers: set RVL_REDACTION_KEY (at least 16 bytes) for `redacted:hmac-sha256:<32 hex>`; an empty or short key refuses with E_REDACTION_KEY_INVALID. The key is never printed, logged or witnessed."
    );
    println!();
    println!("Composition:");
    println!(
        "- Pair `rvl <old.csv> <new.csv> --key <column> --json` with a preceding `shape` compatibility report."
    );
    println!(
        "- Treat shape REFUSAL or INCOMPATIBLE as a stop condition before numeric explanation."
    );
    println!(
        "- Feed rvl reports with shape/verify/benchmark artifacts into `assess`, or seal them with `pack`."
    );
    println!();
    println!(
        "Repair policy: rvl doctor --fix is unavailable and exits 2 without stdout. Use rvl doctor triage --json or rvl capabilities --json for read-only diagnostics."
    );
    Ok(0)
}

/// Deprecated `rvl --robot-triage` alias: the triage-mode cmdrvl.doctor.v1 report.
pub fn emit_robot_triage() -> Result<u8, Box<dyn std::error::Error>> {
    emit_report(DoctorMode::Triage)
}

fn emit_report(mode: DoctorMode) -> Result<u8, Box<dyn std::error::Error>> {
    let report = build_report(mode);
    println!("{}", serde_json::to_string_pretty(&report.to_json())?);
    Ok(report.exit_code())
}

fn fix_unavailable() -> Result<u8, Box<dyn std::error::Error>> {
    use std::io::Write;

    let mut stderr = std::io::stderr();
    writeln!(
        stderr,
        "rvl doctor --fix is unavailable: diagnostics are read-only in this release."
    )?;
    writeln!(stderr, "Try triage: rvl doctor triage --json")?;
    writeln!(stderr, "Try capabilities --json: rvl capabilities --json")?;
    writeln!(stderr, "Try robot-docs guide: rvl robot-docs guide")?;
    stderr.flush()?;
    Ok(2)
}

fn operator_contract() -> Value {
    serde_json::from_str(crate::OPERATOR_JSON).unwrap_or(Value::Null)
}

struct Check {
    id: &'static str,
    pass: bool,
    message: String,
    fix: &'static str,
}

struct Report {
    mode: DoctorMode,
    checks: Vec<Check>,
    checks_passed: usize,
}

impl Report {
    fn error_count(&self) -> usize {
        self.checks.iter().filter(|check| !check.pass).count()
    }

    fn healthy(&self) -> bool {
        self.error_count() == 0
    }

    fn status(&self) -> &'static str {
        if self.healthy() {
            "healthy"
        } else {
            "unhealthy"
        }
    }

    /// Doctor contract: 0 healthy, 1 unhealthy (an error finding), never 2.
    fn exit_code(&self) -> u8 {
        if self.healthy() { 0 } else { 1 }
    }

    fn to_json(&self) -> Value {
        let findings: Vec<Value> = self
            .checks
            .iter()
            .map(|check| {
                let mut finding = json!({
                    "id": check.id,
                    "severity": if check.pass { "info" } else { "error" },
                    "message": check.message,
                });
                if !check.pass {
                    finding["fix"] = json!(check.fix);
                }
                finding
            })
            .collect();

        let mut report = json!({
            "schema_version": DOCTOR_SCHEMA_VERSION,
            "mode": self.mode.as_str(),
            "tool": "rvl",
            "tool_version": env!("CARGO_PKG_VERSION"),
            "healthy": self.healthy(),
            "findings": findings,
        });

        match self.mode {
            DoctorMode::Health => {}
            DoctorMode::Capabilities => {
                report["contract"] = operator_contract();
            }
            DoctorMode::Triage => {
                let mut next_actions: Vec<String> = self
                    .checks
                    .iter()
                    .filter(|check| !check.pass)
                    .map(|check| check.fix.to_string())
                    .collect();
                if next_actions.is_empty() {
                    next_actions.push("rvl capabilities --json".to_string());
                    next_actions.push("rvl <old.csv> <new.csv> --key <column> --json".to_string());
                } else {
                    next_actions.push("rvl doctor capabilities --json".to_string());
                }
                report["next_actions"] = json!(next_actions);
            }
        }
        report
    }
}

fn build_report(mode: DoctorMode) -> Report {
    let mut checks = vec![
        Check {
            id: "binary-metadata",
            pass: true,
            message: format!("rvl {} is runnable", env!("CARGO_PKG_VERSION")),
            fix: "reinstall rvl",
        },
        Check {
            id: "operator-manifest",
            pass: operator_manifest_ok(),
            message: "compiled operator manifest is readable".to_string(),
            fix: "rebuild rvl with a valid operator.json",
        },
    ];

    if let Some(check) = source_checkout_gitignore_check() {
        checks.push(check);
    }

    let checks_passed = checks.iter().filter(|check| check.pass).count();
    Report {
        mode,
        checks,
        checks_passed,
    }
}

fn operator_manifest_ok() -> bool {
    operator_contract().get("name").and_then(Value::as_str) == Some("rvl")
}

fn source_checkout_gitignore_check() -> Option<Check> {
    let cwd = std::env::current_dir().ok()?;
    if !looks_like_rvl_source_checkout(&cwd) {
        return None;
    }

    let gitignore = cwd.join(".gitignore");
    let pass = match std::fs::read_to_string(&gitignore) {
        Ok(contents) => contents.lines().any(|line| line.trim() == ".doctor/"),
        Err(_) => false,
    };

    Some(Check {
        id: "source-gitignore-doctor",
        pass,
        message: ".doctor/ is ignored in this rvl checkout".to_string(),
        fix: "add .doctor/ to .gitignore",
    })
}

fn looks_like_rvl_source_checkout(path: &Path) -> bool {
    let cargo_toml = path.join("Cargo.toml");
    let operator_json = path.join("operator.json");
    match std::fs::read_to_string(cargo_toml) {
        Ok(contents) => {
            contents
                .lines()
                .any(|line| line.trim() == r#"name = "rvl""#)
                && operator_json.exists()
        }
        Err(_) => false,
    }
}
