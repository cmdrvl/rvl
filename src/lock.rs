//! `--lock` input verification against `lock.v0` lockfiles (parity with `shape`'s `src/lock.rs`).
//!
//! Matching mirrors `shape`/`verify`/`benchmark`: an exact path match wins, otherwise a unique
//! basename match is used (real `lock` output stores member paths relative to the vacuum root, so
//! basename matching is the normal case). A matched member whose `bytes_hash`
//! (`sha256:<hex>` or `blake3:<hex>` of the raw file bytes) differs is drift; no match (or an
//! ambiguous basename) is "not locked". Unreadable or malformed lockfiles are I/O failures.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize)]
struct Lockfile {
    version: String,
    #[serde(default)]
    lock_hash: Option<String>,
    members: Vec<LockMember>,
}

#[derive(Debug, Deserialize)]
struct LockMember {
    path: String,
    bytes_hash: String,
}

/// A lockfile that vouched for at least one verified input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LockReceipt {
    pub lockfile: String,
    /// The lockfile's own `lock_hash` (its identity), when present.
    pub lock_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockFailure {
    /// The lockfile (or an unsupported member hash algorithm) could not be used.
    Lockfile { lockfile: String, error: String },
    /// The input file could not be read for hashing.
    InputUnreadable { error: String },
    /// No member of any lockfile matches the input.
    NotLocked,
    /// A member matches the input path but its pinned hash differs from the file bytes.
    Drift {
        lockfile: String,
        expected_hash: String,
        actual_hash: String,
    },
}

/// A lockfile that could not be read, parsed, or used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockfileError {
    pub lockfile: String,
    pub error: String,
}

impl From<LockfileError> for LockFailure {
    fn from(err: LockfileError) -> Self {
        LockFailure::Lockfile {
            lockfile: err.lockfile,
            error: err.error,
        }
    }
}

struct LoadedLock {
    path: PathBuf,
    file: Lockfile,
}

#[derive(Clone)]
struct Candidate<'a> {
    lock: &'a LoadedLock,
    member_path: &'a str,
    bytes_hash: &'a str,
}

/// Lockfiles parsed once, then used to verify each input.
pub struct LockSet {
    locks: Vec<LoadedLock>,
}

impl LockSet {
    pub fn load(lockfiles: &[PathBuf]) -> Result<Self, LockfileError> {
        let mut locks = Vec::with_capacity(lockfiles.len());
        for path in lockfiles {
            let display = path.to_string_lossy().into_owned();
            let contents = std::fs::read_to_string(path).map_err(|err| LockfileError {
                lockfile: display.clone(),
                error: err.to_string(),
            })?;
            let file: Lockfile = serde_json::from_str(&contents).map_err(|err| LockfileError {
                lockfile: display.clone(),
                error: format!("invalid lockfile: {err}"),
            })?;
            if file.version != "lock.v0" {
                return Err(LockfileError {
                    lockfile: display,
                    error: format!("unsupported lockfile version '{}'", file.version),
                });
            }
            locks.push(LoadedLock {
                path: path.clone(),
                file,
            });
        }
        Ok(Self { locks })
    }

    /// Verify one input; on success returns the receipts of the lockfiles that vouched for it.
    pub fn verify(&self, input: &Path) -> Result<Vec<LockReceipt>, LockFailure> {
        let bytes = std::fs::read(input).map_err(|err| LockFailure::InputUnreadable {
            error: err.to_string(),
        })?;
        let exact_path = input.to_string_lossy().replace('\\', "/");
        let basename = input.file_name().and_then(|name| name.to_str());

        let mut exact: Vec<Candidate<'_>> = Vec::new();
        let mut by_basename: Vec<Candidate<'_>> = Vec::new();
        for lock in &self.locks {
            for member in &lock.file.members {
                let candidate = Candidate {
                    lock,
                    member_path: &member.path,
                    bytes_hash: &member.bytes_hash,
                };
                if member.path == exact_path {
                    exact.push(candidate);
                } else if basename.is_some()
                    && Path::new(&member.path)
                        .file_name()
                        .and_then(|name| name.to_str())
                        == basename
                    && !by_basename
                        .iter()
                        .any(|seen| seen.lock.path == lock.path && seen.member_path == member.path)
                {
                    // The same lockfile passed twice must not make its own member ambiguous.
                    by_basename.push(candidate);
                }
            }
        }

        if !exact.is_empty() {
            return check_matches(&bytes, &exact);
        }
        if by_basename.len() > 1 {
            // An ambiguous basename cannot be verified safely.
            return Err(LockFailure::NotLocked);
        }
        if !by_basename.is_empty() {
            return check_matches(&bytes, &by_basename);
        }
        Err(LockFailure::NotLocked)
    }
}

/// Hash `bytes` with the algorithm named by the prefix of `expected`. `None` if unsupported.
fn hash_like(bytes: &[u8], expected: &str) -> Option<String> {
    match expected.split_once(':')?.0 {
        "sha256" => Some(format!("sha256:{:x}", Sha256::digest(bytes))),
        "blake3" => Some(format!("blake3:{}", blake3::hash(bytes).to_hex())),
        _ => None,
    }
}

fn check_matches(bytes: &[u8], matches: &[Candidate<'_>]) -> Result<Vec<LockReceipt>, LockFailure> {
    for candidate in matches {
        match hash_like(bytes, candidate.bytes_hash) {
            Some(actual) if actual == candidate.bytes_hash => {
                return Ok(vec![LockReceipt {
                    lockfile: candidate.lock.path.to_string_lossy().into_owned(),
                    lock_hash: candidate.lock.file.lock_hash.clone(),
                }]);
            }
            Some(_) => {}
            None => {
                return Err(LockFailure::Lockfile {
                    lockfile: candidate.lock.path.to_string_lossy().into_owned(),
                    error: format!(
                        "unsupported bytes_hash algorithm in '{}' (member '{}')",
                        candidate.bytes_hash, candidate.member_path
                    ),
                });
            }
        }
    }
    let first = &matches[0];
    Err(LockFailure::Drift {
        lockfile: first.lock.path.to_string_lossy().into_owned(),
        expected_hash: first.bytes_hash.to_string(),
        actual_hash: hash_like(bytes, first.bytes_hash).unwrap_or_default(),
    })
}

/// Merge receipts, keeping first-seen order and one entry per lockfile.
pub fn merge_receipts(into: &mut Vec<LockReceipt>, more: Vec<LockReceipt>) {
    for receipt in more {
        if !into.iter().any(|seen| seen.lockfile == receipt.lockfile) {
            into.push(receipt);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static N: AtomicU64 = AtomicU64::new(0);

    fn scratch(name: &str, contents: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rvl-lock-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    fn lock_for(member: &str, hash: &str) -> PathBuf {
        scratch(
            "x.lock.json",
            &format!(
                r#"{{"version":"lock.v0","lock_hash":"sha256:cafe","members":[{{"path":"{member}","bytes_hash":"{hash}"}}]}}"#
            ),
        )
    }

    fn verify(input: &Path, lock: PathBuf) -> Result<Vec<LockReceipt>, LockFailure> {
        LockSet::load(&[lock])
            .map_err(LockFailure::from)?
            .verify(input)
    }

    #[test]
    fn matching_sha256_verifies_and_reports_lock_hash() {
        let input = scratch("a.csv", "id,v\n1,2\n");
        let hash = hash_like(b"id,v\n1,2\n", "sha256:").unwrap();
        let receipts = verify(&input, lock_for(&input.to_string_lossy(), &hash)).unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].lock_hash.as_deref(), Some("sha256:cafe"));
    }

    #[test]
    fn blake3_verifies_and_detects_drift() {
        let input = scratch("a.csv", "id,v\n1,2\n");
        let good = format!("blake3:{}", blake3::hash(b"id,v\n1,2\n").to_hex());
        verify(&input, lock_for(&input.to_string_lossy(), &good)).unwrap();

        let stale = format!("blake3:{}", blake3::hash(b"other").to_hex());
        let err = verify(&input, lock_for(&input.to_string_lossy(), &stale)).unwrap_err();
        assert!(matches!(err, LockFailure::Drift { .. }), "{err:?}");
    }

    #[test]
    fn basename_match_verifies() {
        let input = scratch("a.csv", "id,v\n1,2\n");
        let hash = hash_like(b"id,v\n1,2\n", "sha256:").unwrap();
        verify(&input, lock_for("elsewhere/a.csv", &hash)).unwrap();
    }

    #[test]
    fn drift_reports_expected_and_actual() {
        let input = scratch("a.csv", "id,v\n1,2\n");
        let err = verify(
            &input,
            lock_for(&input.to_string_lossy(), "sha256:deadbeef"),
        )
        .unwrap_err();
        match err {
            LockFailure::Drift {
                expected_hash,
                actual_hash,
                ..
            } => {
                assert_eq!(expected_hash, "sha256:deadbeef");
                assert_eq!(actual_hash, hash_like(b"id,v\n1,2\n", "sha256:").unwrap());
            }
            other => panic!("expected drift, got {other:?}"),
        }
    }

    #[test]
    fn unlocked_and_ambiguous_inputs_are_not_locked() {
        let input = scratch("a.csv", "id,v\n1,2\n");
        let err = verify(&input, lock_for("other.csv", "sha256:00")).unwrap_err();
        assert_eq!(err, LockFailure::NotLocked);

        // Two distinct members sharing the basename cannot be verified safely.
        let hash = hash_like(b"id,v\n1,2\n", "sha256:").unwrap();
        let lock = scratch(
            "dup.lock.json",
            &format!(
                r#"{{"version":"lock.v0","members":[{{"path":"x/a.csv","bytes_hash":"{hash}"}},{{"path":"y/a.csv","bytes_hash":"{hash}"}}]}}"#
            ),
        );
        assert_eq!(verify(&input, lock).unwrap_err(), LockFailure::NotLocked);
    }

    #[test]
    fn unsupported_algorithm_and_malformed_lockfiles_fail_as_lockfile_errors() {
        let input = scratch("a.csv", "id,v\n1,2\n");
        let err = verify(&input, lock_for(&input.to_string_lossy(), "md5:abc")).unwrap_err();
        assert!(matches!(err, LockFailure::Lockfile { .. }), "{err:?}");
        let err = verify(&input, scratch("bad.lock.json", "not json")).unwrap_err();
        assert!(matches!(err, LockFailure::Lockfile { .. }), "{err:?}");
        let wrong_version = scratch("v.lock.json", r#"{"version":"lock.v9","members":[]}"#);
        let err = verify(&input, wrong_version).unwrap_err();
        assert!(matches!(err, LockFailure::Lockfile { .. }), "{err:?}");
    }
}
