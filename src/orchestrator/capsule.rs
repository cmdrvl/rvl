use std::fs;
use std::path::Path;

use serde::Serialize;

use crate::cli::args::Args;
use crate::cli::exit::Outcome;
use crate::profile::render_profile_yaml_with_registry_override;
use crate::witness::hash::hash_bytes;

use super::PipelineResult;

const CAPSULE_MANIFEST_VERSION: &str = "rvl.capsule.v0";
const PROFILE_ARTIFACT_PATH: &str = "profile.yaml";
const REGISTRY_ARTIFACT_DIR: &str = "registries/column_registry";

#[derive(Clone, Debug, Default)]
pub(super) struct CapsuleRunSummary {
    pub refusal_code: Option<String>,
    pub contributors: Option<CapsuleContributorSummary>,
}

impl CapsuleRunSummary {
    pub(super) fn refusal(code: String) -> Self {
        Self {
            refusal_code: Some(code),
            contributors: None,
        }
    }

    pub(super) fn no_real_change() -> Self {
        Self {
            refusal_code: None,
            contributors: Some(CapsuleContributorSummary {
                count: 0,
                coverage: 0.0,
                top: Vec::new(),
            }),
        }
    }

    pub(super) fn real_change(contributors: CapsuleContributorSummary) -> Self {
        Self {
            refusal_code: None,
            contributors: Some(contributors),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct CapsuleContributorSummary {
    pub count: usize,
    pub coverage: f64,
    pub top: Vec<CapsuleContributor>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct CapsuleContributor {
    pub row_id: String,
    pub column: String,
    pub delta: f64,
    pub contribution: f64,
    pub share: f64,
}

#[derive(Debug, Serialize)]
struct CapsuleManifest {
    version: &'static str,
    capsule_id: String,
    tool: ToolInfo,
    args: CapsuleArgs,
    outcome: String,
    refusal_code: Option<String>,
    contributor_summary: ContributorSummary,
    replay_command: String,
    artifacts: CapsuleArtifacts,
}

#[derive(Debug, Serialize)]
struct ToolInfo {
    name: &'static str,
    version: &'static str,
}

#[derive(Debug, Serialize)]
struct CapsuleArgs {
    old: String,
    new: String,
    key: Option<String>,
    profile: Option<String>,
    profile_id: Option<String>,
    threshold: f64,
    tolerance: f64,
    delimiter: Option<String>,
    exhaustive: bool,
    max_audit_changes: u64,
    json: bool,
    no_witness: bool,
}

#[derive(Debug, Serialize)]
struct ContributorSummary {
    count: usize,
    coverage: f64,
    top: Vec<CapsuleContributor>,
}

#[derive(Debug, Serialize)]
struct CapsuleArtifacts {
    old_csv: CapsuleArtifact,
    new_csv: CapsuleArtifact,
    output: CapsuleArtifact,
    replay: CapsuleArtifact,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<CapsuleArtifact>,
    #[serde(skip_serializing_if = "Option::is_none")]
    column_registry: Option<Vec<CapsuleArtifact>>,
}

#[derive(Debug, Serialize)]
struct CapsuleArtifact {
    path: String,
    hash: String,
    bytes: u64,
}

pub(super) fn write_capsule(args: &Args, result: &PipelineResult, summary: &CapsuleRunSummary) {
    let Some(root) = args.capsule_out.as_ref() else {
        return;
    };

    let old_path = args.old_path().to_string_lossy().to_string();
    let new_path = args.new_path().to_string_lossy().to_string();
    let old_bytes = match fs::read(args.old_path()) {
        Ok(bytes) => bytes,
        Err(_) => return,
    };
    let new_bytes = match fs::read(args.new_path()) {
        Ok(bytes) => bytes,
        Err(_) => return,
    };

    let old_hash = format!("blake3:{}", hash_bytes(&old_bytes));
    let new_hash = format!("blake3:{}", hash_bytes(&new_bytes));
    let output_hash = format!("blake3:{}", hash_bytes(result.output.as_bytes()));
    let capsule_registry = result
        .profile
        .capsule_profile
        .as_ref()
        .and_then(|profile| profile.column_registry.as_ref());
    let registry_override = capsule_registry.map(|_| REGISTRY_ARTIFACT_DIR);
    let profile_bytes = result
        .profile
        .capsule_profile
        .as_ref()
        .map(|profile| render_profile_yaml_with_registry_override(profile, registry_override))
        .map(String::into_bytes);

    let args_manifest = CapsuleArgs {
        old: old_path,
        new: new_path,
        key: args.key.clone(),
        profile: args
            .profile
            .as_ref()
            .map(|path| path.to_string_lossy().to_string()),
        profile_id: args.profile_id.clone(),
        threshold: args.threshold,
        tolerance: args.tolerance,
        delimiter: args.delimiter.map(|d| format!("0x{d:02x}")),
        exhaustive: args.exhaustive,
        max_audit_changes: args.max_audit_changes,
        json: args.json,
        no_witness: args.no_witness,
    };

    let replay_command = build_replay_command(args, profile_bytes.is_some());
    let replay_script = format!("#!/usr/bin/env bash\nset -euo pipefail\n{replay_command}\n");
    let replay_hash = format!("blake3:{}", hash_bytes(replay_script.as_bytes()));

    let capsule_seed = serde_json::json!({
        "old_hash": old_hash,
        "new_hash": new_hash,
        "args": &args_manifest,
        "outcome": outcome_string(result.outcome),
        "refusal_code": summary.refusal_code,
        "output_hash": output_hash,
    });
    let capsule_id = hash_bytes(capsule_seed.to_string().as_bytes());
    let capsule_dir = root.join(format!("capsule-{capsule_id}"));
    if fs::create_dir_all(&capsule_dir).is_err() {
        return;
    }

    let old_artifact = CapsuleArtifact {
        path: "old.csv".to_string(),
        hash: format!("blake3:{}", hash_bytes(&old_bytes)),
        bytes: old_bytes.len() as u64,
    };
    let new_artifact = CapsuleArtifact {
        path: "new.csv".to_string(),
        hash: format!("blake3:{}", hash_bytes(&new_bytes)),
        bytes: new_bytes.len() as u64,
    };
    let output_artifact = CapsuleArtifact {
        path: "output.txt".to_string(),
        hash: format!("blake3:{}", hash_bytes(result.output.as_bytes())),
        bytes: result.output.len() as u64,
    };
    let replay_artifact = CapsuleArtifact {
        path: "replay.sh".to_string(),
        hash: replay_hash,
        bytes: replay_script.len() as u64,
    };
    let profile_artifact = profile_bytes.as_ref().map(|bytes| CapsuleArtifact {
        path: PROFILE_ARTIFACT_PATH.to_string(),
        hash: format!("blake3:{}", hash_bytes(bytes)),
        bytes: bytes.len() as u64,
    });
    let registry_artifacts = capsule_registry.map(|registry| {
        registry
            .files
            .iter()
            .map(|file| CapsuleArtifact {
                path: format!("{REGISTRY_ARTIFACT_DIR}/{}", file.relative_path),
                hash: format!("blake3:{}", hash_bytes(&file.bytes)),
                bytes: file.bytes.len() as u64,
            })
            .collect::<Vec<_>>()
    });

    if fs::write(capsule_dir.join(&old_artifact.path), &old_bytes).is_err() {
        return;
    }
    if fs::write(capsule_dir.join(&new_artifact.path), &new_bytes).is_err() {
        return;
    }
    if fs::write(
        capsule_dir.join(&output_artifact.path),
        result.output.as_bytes(),
    )
    .is_err()
    {
        return;
    }
    if fs::write(
        capsule_dir.join(&replay_artifact.path),
        replay_script.as_bytes(),
    )
    .is_err()
    {
        return;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let replay_path = capsule_dir.join(&replay_artifact.path);
        let Ok(mut permissions) = fs::metadata(&replay_path).map(|meta| meta.permissions()) else {
            return;
        };
        permissions.set_mode(0o755);
        if fs::set_permissions(&replay_path, permissions).is_err() {
            return;
        }
    }
    if let Some(profile_bytes) = profile_bytes.as_ref()
        && fs::write(capsule_dir.join(PROFILE_ARTIFACT_PATH), profile_bytes).is_err()
    {
        return;
    }
    if let Some(registry) = capsule_registry {
        let registry_dir = capsule_dir.join(REGISTRY_ARTIFACT_DIR);
        if fs::create_dir_all(&registry_dir).is_err() {
            return;
        }
        for file in &registry.files {
            if fs::write(registry_dir.join(&file.relative_path), &file.bytes).is_err() {
                return;
            }
        }
    }

    let contributor_summary = summary
        .contributors
        .clone()
        .unwrap_or(CapsuleContributorSummary {
            count: 0,
            coverage: 0.0,
            top: Vec::new(),
        });

    let manifest = CapsuleManifest {
        version: CAPSULE_MANIFEST_VERSION,
        capsule_id,
        tool: ToolInfo {
            name: "rvl",
            version: env!("CARGO_PKG_VERSION"),
        },
        args: args_manifest,
        outcome: outcome_string(result.outcome).to_string(),
        refusal_code: summary.refusal_code.clone(),
        contributor_summary: ContributorSummary {
            count: contributor_summary.count,
            coverage: contributor_summary.coverage,
            top: contributor_summary.top,
        },
        replay_command,
        artifacts: CapsuleArtifacts {
            old_csv: old_artifact,
            new_csv: new_artifact,
            output: output_artifact,
            replay: replay_artifact,
            profile: profile_artifact,
            column_registry: registry_artifacts,
        },
    };

    let Ok(manifest_json) = serde_json::to_string_pretty(&manifest) else {
        return;
    };
    let _ = fs::write(capsule_dir.join(Path::new("manifest.json")), manifest_json);
}

fn build_replay_command(args: &Args, use_local_profile: bool) -> String {
    let mut parts = vec![
        "rvl".to_string(),
        "old.csv".to_string(),
        "new.csv".to_string(),
    ];

    if let Some(key) = args.key.as_deref() {
        parts.push("--key".to_string());
        parts.push(shell_escape(key));
    }
    if use_local_profile {
        parts.push("--profile".to_string());
        parts.push(PROFILE_ARTIFACT_PATH.to_string());
    } else if let Some(profile) = args.profile.as_ref() {
        parts.push("--profile".to_string());
        parts.push(shell_escape(&profile.to_string_lossy()));
    } else if let Some(profile_id) = args.profile_id.as_deref() {
        parts.push("--profile-id".to_string());
        parts.push(shell_escape(profile_id));
    }
    parts.push("--threshold".to_string());
    parts.push(args.threshold.to_string());
    parts.push("--tolerance".to_string());
    parts.push(args.tolerance.to_string());
    if let Some(delimiter) = args.delimiter {
        parts.push("--delimiter".to_string());
        parts.push(format!("0x{delimiter:02x}"));
    }
    if args.exhaustive {
        parts.push("--exhaustive".to_string());
        parts.push("--max-audit-changes".to_string());
        parts.push(args.max_audit_changes.to_string());
    }
    if args.json {
        parts.push("--json".to_string());
    }
    if args.no_witness {
        parts.push("--no-witness".to_string());
    }

    parts.join(" ")
}

fn shell_escape(raw: &str) -> String {
    if raw
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':'))
    {
        return raw.to_string();
    }
    format!("'{}'", raw.replace('\'', "'\"'\"'"))
}

fn outcome_string(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::NoRealChange => "NO_REAL_CHANGE",
        Outcome::RealChange => "REAL_CHANGE",
        Outcome::Refusal => "REFUSAL",
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn replay_command_includes_explicit_flags() {
        let args = Args::new(
            PathBuf::from("old.csv"),
            PathBuf::from("new.csv"),
            Some("portfolio id".to_string()),
            0.95,
            1e-9,
            Some(b','),
            true,
        );
        let replay = build_replay_command(&args, false);
        assert!(replay.contains("rvl old.csv new.csv"));
        assert!(replay.contains("--key 'portfolio id'"));
        assert!(replay.contains("--threshold 0.95"));
        assert!(replay.contains("--tolerance "));
        assert!(replay.contains("--delimiter 0x2c"));
        assert!(replay.contains("--json"));
    }

    #[test]
    fn replay_command_prefers_local_profile_artifact_when_available() {
        let mut args = Args::new(
            PathBuf::from("old.csv"),
            PathBuf::from("new.csv"),
            None,
            0.95,
            1e-9,
            None,
            true,
        );
        args.profile = Some(PathBuf::from("profiles/demo.yaml"));
        args.profile_id = Some("csv.demo.v0".to_string());

        let replay = build_replay_command(&args, true);
        assert!(replay.contains("--profile profile.yaml"));
        assert!(!replay.contains("profiles/demo.yaml"));
        assert!(!replay.contains("--profile-id"));
    }
}
