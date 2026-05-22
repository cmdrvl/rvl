use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::alignment::key_parse::parse_key_identifier;
use crate::normalize::trim::ascii_trim;
use crate::witness::hash::hash_bytes;

#[derive(Debug, Clone)]
pub struct ResolvedProfile {
    pub include_columns: Vec<Vec<u8>>,
    pub key_columns: Vec<Vec<u8>>,
    pub key_labels: Vec<String>,
    pub profile_id: Option<String>,
    pub profile_sha256: Option<String>,
    pub source_path: PathBuf,
    pub column_registry: Option<ColumnRegistry>,
}

#[derive(Debug, Clone)]
pub struct ColumnRegistry {
    pub reference: String,
    pub resolved_path: PathBuf,
    pub hash: String,
    pub aliases: HashMap<Vec<u8>, Vec<u8>>,
    pub files: Vec<ColumnRegistryFile>,
}

#[derive(Debug, Clone)]
pub struct ColumnRegistryFile {
    pub relative_path: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRegistryRunInfo {
    pub reference: String,
    pub path: String,
    pub hash: String,
}

impl ResolvedProfile {
    pub fn include_set(&self) -> HashSet<Vec<u8>> {
        self.include_columns.iter().cloned().collect()
    }

    pub fn primary_key(&self) -> Option<&[u8]> {
        self.key_columns.first().map(|value| value.as_slice())
    }
}

#[derive(Debug, Clone)]
pub enum ResolveError {
    NotFound {
        selector: String,
    },
    Invalid {
        selector: String,
        error: String,
    },
    Registry {
        selector: String,
        profile_id: Option<String>,
        column_registry: String,
        reason: String,
        file: Option<String>,
    },
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::NotFound { selector } => write!(f, "profile not found: {selector}"),
            ResolveError::Invalid { selector, error } => {
                write!(f, "invalid profile {selector}: {error}")
            }
            ResolveError::Registry {
                selector,
                column_registry,
                reason,
                ..
            } => write!(
                f,
                "invalid profile registry {column_registry} for {selector}: {reason}"
            ),
        }
    }
}

impl std::error::Error for ResolveError {}

#[derive(Debug, Default)]
struct RawProfile {
    profile_id: Option<String>,
    profile_sha256: Option<String>,
    include_columns: Vec<String>,
    key: Vec<String>,
    column_registry: Option<String>,
}

pub fn load_profile_from_path(path: &Path) -> Result<ResolvedProfile, ResolveError> {
    let selector = path.to_string_lossy().to_string();
    let raw = fs::read_to_string(path).map_err(|err| ResolveError::Invalid {
        selector: selector.clone(),
        error: err.to_string(),
    })?;

    let parsed = parse_profile_yaml(&raw).map_err(|err| ResolveError::Invalid {
        selector: selector.clone(),
        error: err,
    })?;
    let parsed_profile_id = parsed.profile_id.clone();

    let mut include_columns = Vec::new();
    let mut include_seen = HashSet::new();
    for column in parsed.include_columns {
        if let Some(bytes) = parse_column_identifier(&column)
            && include_seen.insert(bytes.clone())
        {
            include_columns.push(bytes);
        }
    }

    let mut key_columns = Vec::new();
    let mut key_labels = Vec::new();
    for key in parsed.key {
        if let Some((bytes, label)) = parse_key_entry(&key) {
            key_columns.push(bytes);
            key_labels.push(label);
        }
    }

    let column_registry = if let Some(reference) = parsed.column_registry {
        Some(
            load_column_registry(path, &reference).map_err(|err| ResolveError::Registry {
                selector: selector.clone(),
                profile_id: parsed_profile_id.clone(),
                column_registry: reference,
                reason: err.reason,
                file: err.file,
            })?,
        )
    } else {
        None
    };

    Ok(ResolvedProfile {
        include_columns,
        key_columns,
        key_labels,
        profile_id: parsed.profile_id,
        profile_sha256: parsed.profile_sha256,
        source_path: path.to_path_buf(),
        column_registry,
    })
}

pub fn resolve_profile_id(selector: &str) -> Result<ResolvedProfile, ResolveError> {
    let selector_path = Path::new(selector);
    if selector_path.exists() {
        return load_profile_from_path(selector_path);
    }

    let search_root =
        crate::paths::profile_dir_for_read().map_err(|error| ResolveError::Invalid {
            selector: selector.to_string(),
            error,
        })?;

    resolve_profile_id_in_directory(selector, &search_root)
}

pub fn render_profile_yaml(profile: &ResolvedProfile) -> String {
    render_profile_yaml_with_registry_override(profile, None)
}

pub fn render_profile_yaml_with_registry_override(
    profile: &ResolvedProfile,
    registry_override: Option<&str>,
) -> String {
    let mut out = String::new();
    if let Some(profile_id) = profile.profile_id.as_deref() {
        out.push_str("profile_id: ");
        out.push_str(profile_id);
        out.push('\n');
    }
    if let Some(profile_sha256) = profile.profile_sha256.as_deref() {
        out.push_str("profile_sha256: ");
        out.push_str(profile_sha256);
        out.push('\n');
    }
    let registry_reference = registry_override.or_else(|| {
        profile
            .column_registry
            .as_ref()
            .map(|registry| registry.reference.as_str())
    });
    if let Some(column_registry) = registry_reference {
        out.push_str("column_registry: ");
        out.push_str(column_registry);
        out.push('\n');
    }
    out.push_str("include_columns:\n");
    for column in &profile.include_columns {
        out.push_str("  - ");
        out.push_str(&encode_profile_identifier(column));
        out.push('\n');
    }
    out.push_str("key:\n");
    for key in &profile.key_columns {
        out.push_str("  - ");
        out.push_str(&encode_profile_identifier(key));
        out.push('\n');
    }
    out
}

fn parse_profile_yaml(raw: &str) -> Result<RawProfile, String> {
    let mut parsed = RawProfile::default();
    let lines: Vec<&str> = raw.lines().collect();
    let mut index = 0usize;
    while index < lines.len() {
        let line = strip_comment(lines[index]).trim();
        if line.is_empty() {
            index += 1;
            continue;
        }

        if let Some(rest) = line.strip_prefix("profile_id:") {
            parsed.profile_id = parse_scalar(rest.trim());
            index += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("profile_sha256:") {
            parsed.profile_sha256 = parse_scalar(rest.trim());
            index += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("column_registry:") {
            parsed.column_registry = parse_scalar(rest.trim());
            index += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("include_columns:") {
            let (items, consumed) = parse_list(rest.trim(), &lines[index + 1..]);
            parsed.include_columns = items;
            index += consumed + 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("key:") {
            let (items, consumed) = parse_list(rest.trim(), &lines[index + 1..]);
            parsed.key = items;
            index += consumed + 1;
            continue;
        }

        index += 1;
    }
    Ok(parsed)
}

fn parse_list(inline_value: &str, following_lines: &[&str]) -> (Vec<String>, usize) {
    if !inline_value.is_empty() {
        return (parse_inline_list(inline_value), 0);
    }

    let mut values = Vec::new();
    let mut consumed = 0usize;
    for raw_line in following_lines {
        let line = strip_comment(raw_line).trim();
        if line.is_empty() {
            consumed += 1;
            continue;
        }
        let Some(item) = line.strip_prefix('-') else {
            break;
        };
        if let Some(value) = parse_scalar(item.trim()) {
            values.push(value);
        }
        consumed += 1;
    }
    (values, consumed)
}

fn parse_inline_list(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    let inner = if trimmed.starts_with('[') && trimmed.ends_with(']') {
        &trimmed[1..trimmed.len().saturating_sub(1)]
    } else {
        trimmed
    };

    inner
        .split(',')
        .filter_map(|item| parse_scalar(item.trim()))
        .collect()
}

fn parse_scalar(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        return Some(value[1..value.len() - 1].to_string());
    }
    Some(value.to_string())
}

fn strip_comment(raw: &str) -> &str {
    raw.split('#').next().unwrap_or(raw)
}

fn parse_column_identifier(raw: &str) -> Option<Vec<u8>> {
    let trimmed = ascii_trim(raw.as_bytes());
    if trimmed.is_empty() {
        return None;
    }

    let text = String::from_utf8_lossy(trimmed);
    let parsed = parse_key_identifier(&text).unwrap_or_else(|_| trimmed.to_vec());
    let normalized = ascii_trim(&parsed);
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_vec())
    }
}

fn parse_key_entry(raw: &str) -> Option<(Vec<u8>, String)> {
    let trimmed = ascii_trim(raw.as_bytes());
    if trimmed.is_empty() {
        return None;
    }

    let label = String::from_utf8_lossy(trimmed).to_string();
    let parsed = parse_key_identifier(&label).unwrap_or_else(|_| trimmed.to_vec());
    let normalized = ascii_trim(&parsed);
    if normalized.is_empty() {
        None
    } else {
        Some((normalized.to_vec(), label))
    }
}

fn encode_profile_identifier(bytes: &[u8]) -> String {
    if bytes.contains(&b'#') {
        let mut out = String::with_capacity(4 + bytes.len() * 2);
        out.push_str("hex:");
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
        }
        return out;
    }

    crate::format::ident_json::encode_identifier_json(bytes)
}

fn is_frozen_with_id(profile: &ResolvedProfile, selector: &str) -> bool {
    matches!(profile.profile_id.as_deref(), Some(id) if id == selector)
        && profile.profile_sha256.is_some()
}

fn resolve_profile_id_in_directory(
    selector: &str,
    directory: &Path,
) -> Result<ResolvedProfile, ResolveError> {
    let entries = fs::read_dir(directory).map_err(|_| ResolveError::NotFound {
        selector: selector.to_string(),
    })?;

    let mut paths: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("yaml"))
        .collect();
    paths.sort();

    for path in paths {
        match load_profile_from_path(&path) {
            Ok(profile) if is_frozen_with_id(&profile, selector) => return Ok(profile),
            Err(err) => {
                if matches!(
                    &err,
                    ResolveError::Registry {
                        profile_id: Some(profile_id),
                        ..
                    } if profile_id == selector
                ) {
                    return Err(err);
                }
            }
            Ok(_) => {}
        }
    }

    Err(ResolveError::NotFound {
        selector: selector.to_string(),
    })
}

#[derive(Debug)]
struct ColumnRegistryLoadError {
    reason: String,
    file: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawRegistryEntry {
    input: String,
    canonical_id: String,
    canonical_type: String,
    #[serde(rename = "rule_id")]
    _rule_id: String,
}

fn load_column_registry(
    profile_path: &Path,
    reference: &str,
) -> Result<ColumnRegistry, ColumnRegistryLoadError> {
    let resolved_path = resolve_registry_path(profile_path, reference);
    if !resolved_path.is_dir() {
        return Err(ColumnRegistryLoadError {
            reason: "registry directory does not exist or is not a directory".to_string(),
            file: None,
        });
    }

    let registry_json_path = resolved_path.join("registry.json");
    let registry_json = read_registry_file(&registry_json_path, "registry.json")?;
    let registry_value: serde_json::Value =
        serde_json::from_slice(&registry_json).map_err(|err| ColumnRegistryLoadError {
            reason: format!("registry.json is not valid JSON: {err}"),
            file: Some("registry.json".to_string()),
        })?;
    if !registry_value.is_object() {
        return Err(ColumnRegistryLoadError {
            reason: "registry.json must be a JSON object".to_string(),
            file: Some("registry.json".to_string()),
        });
    }

    let mut files = vec![ColumnRegistryFile {
        relative_path: "registry.json".to_string(),
        bytes: registry_json,
    }];
    let mut aliases: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    for (relative_path, path) in mapping_files(&resolved_path)? {
        let bytes = read_registry_file(&path, &relative_path)?;
        let entries: Vec<RawRegistryEntry> =
            serde_json::from_slice(&bytes).map_err(|err| ColumnRegistryLoadError {
                reason: format!("mapping file is not a registry entry array: {err}"),
                file: Some(relative_path.clone()),
            })?;
        for entry in entries {
            if entry.canonical_type != "column_name" {
                continue;
            }
            let input =
                parse_registry_identifier(&entry.input).ok_or_else(|| ColumnRegistryLoadError {
                    reason: "mapping entry has empty input".to_string(),
                    file: Some(relative_path.clone()),
                })?;
            let canonical_id = parse_registry_identifier(&entry.canonical_id).ok_or_else(|| {
                ColumnRegistryLoadError {
                    reason: "mapping entry has empty canonical_id".to_string(),
                    file: Some(relative_path.clone()),
                }
            })?;
            aliases.entry(input).or_insert(canonical_id);
        }
        files.push(ColumnRegistryFile {
            relative_path,
            bytes,
        });
    }

    let hash = format!("blake3:{}", registry_content_hash(&files));
    Ok(ColumnRegistry {
        reference: reference.to_string(),
        resolved_path,
        hash,
        aliases,
        files,
    })
}

fn resolve_registry_path(profile_path: &Path, reference: &str) -> PathBuf {
    let registry_path = Path::new(reference);
    if registry_path.is_absolute() {
        return registry_path.to_path_buf();
    }
    profile_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(registry_path)
}

fn mapping_files(registry_path: &Path) -> Result<Vec<(String, PathBuf)>, ColumnRegistryLoadError> {
    let entries = fs::read_dir(registry_path).map_err(|err| ColumnRegistryLoadError {
        reason: format!("cannot read registry directory: {err}"),
        file: None,
    })?;

    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| ColumnRegistryLoadError {
            reason: format!("cannot read registry directory entry: {err}"),
            file: None,
        })?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name == "registry.json" || name == "_build.json" {
            continue;
        }
        files.push((name.to_string(), path));
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(files)
}

fn read_registry_file(
    path: &Path,
    relative_path: &str,
) -> Result<Vec<u8>, ColumnRegistryLoadError> {
    fs::read(path).map_err(|err| ColumnRegistryLoadError {
        reason: err.to_string(),
        file: Some(relative_path.to_string()),
    })
}

fn parse_registry_identifier(raw: &str) -> Option<Vec<u8>> {
    let trimmed = ascii_trim(raw.as_bytes());
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_vec())
    }
}

fn registry_content_hash(files: &[ColumnRegistryFile]) -> String {
    let mut framed = Vec::new();
    for file in files {
        framed.extend_from_slice(file.relative_path.as_bytes());
        framed.push(0);
        framed.extend_from_slice(file.bytes.len().to_string().as_bytes());
        framed.push(0);
        framed.extend_from_slice(&file.bytes);
        framed.push(0xff);
    }
    hash_bytes(&framed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rvl_test_profile_{id}_{seq}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn loads_draft_profile_from_path() {
        let dir = temp_dir();
        let path = dir.join("draft.yaml");
        std::fs::write(
            &path,
            r#"
include_columns:
  - loan_id
  - balance
key: [loan_id]
"#,
        )
        .unwrap();

        let profile = load_profile_from_path(&path).expect("profile should load");
        assert_eq!(profile.include_columns.len(), 2);
        assert_eq!(profile.primary_key(), Some(b"loan_id".as_slice()));
        assert!(profile.profile_id.is_none());
        assert!(profile.profile_sha256.is_none());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn resolves_frozen_profile_by_id_from_directory() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("first.yaml"),
            r#"
profile_id: csv.demo.v0
profile_sha256: sha256:abc
include_columns: [loan_id, balance]
key: [loan_id]
"#,
        )
        .unwrap();
        std::fs::write(
            dir.join("second.yaml"),
            r#"
profile_id: csv.other.v0
include_columns: [loan_id]
key: [loan_id]
"#,
        )
        .unwrap();

        let resolved = resolve_profile_id_in_directory("csv.demo.v0", &dir).expect("resolved");
        assert_eq!(resolved.profile_id.as_deref(), Some("csv.demo.v0"));
        assert_eq!(resolved.profile_sha256.as_deref(), Some("sha256:abc"));

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn rendered_profile_round_trips_through_loader() {
        let dir = temp_dir();
        let path = dir.join("rendered.yaml");
        let profile = ResolvedProfile {
            include_columns: vec![b"loan_id".to_vec(), b"\xff#".to_vec()],
            key_columns: vec![b"loan_id".to_vec()],
            key_labels: vec!["loan_id".to_string()],
            profile_id: Some("csv.demo.v0".to_string()),
            profile_sha256: Some("sha256:abc123".to_string()),
            source_path: path.clone(),
            column_registry: None,
        };
        std::fs::write(&path, render_profile_yaml(&profile)).unwrap();

        let loaded = load_profile_from_path(&path).expect("rendered profile should load");
        assert_eq!(loaded.include_columns, profile.include_columns);
        assert_eq!(loaded.key_columns, profile.key_columns);
        assert_eq!(loaded.profile_id, profile.profile_id);
        assert_eq!(loaded.profile_sha256, profile.profile_sha256);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn loads_column_registry_relative_to_profile_path() {
        let dir = temp_dir();
        let registry_dir = dir.join("registries").join("columns_v0");
        std::fs::create_dir_all(&registry_dir).unwrap();
        std::fs::write(
            registry_dir.join("registry.json"),
            r#"{"id":"columns","version":"1.0.0"}"#,
        )
        .unwrap();
        std::fs::write(
            registry_dir.join("aliases.json"),
            r#"[
  {"input":"Loan Number","canonical_id":"loan_id_number","canonical_type":"column_name","rule_id":"alias"},
  {"input":"Ignored","canonical_id":"ignored","canonical_type":"value","rule_id":"alias"}
]"#,
        )
        .unwrap();
        let path = dir.join("profile.yaml");
        std::fs::write(
            &path,
            r#"
column_registry: registries/columns_v0
include_columns: [loan_id_number]
key: [loan_id_number]
"#,
        )
        .unwrap();

        let profile = load_profile_from_path(&path).expect("profile should load");
        let registry = profile.column_registry.expect("registry should be loaded");
        assert_eq!(registry.reference, "registries/columns_v0");
        assert!(registry.resolved_path.ends_with("registries/columns_v0"));
        assert!(registry.hash.starts_with("blake3:"));
        assert_eq!(
            registry.aliases.get(b"Loan Number".as_slice()),
            Some(&b"loan_id_number".to_vec())
        );
        assert!(!registry.aliases.contains_key(b"Ignored".as_slice()));

        std::fs::remove_dir_all(dir).ok();
    }
}
