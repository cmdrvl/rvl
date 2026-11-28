//! `--lock` input verification against `lock.v0` lockfiles.
//!
//! Matching uses cmdrvl-core's pure R1 names-and-digests rule: drop leading `./`, prefer an exact
//! normalized path, otherwise require one normalized basename path, validate every candidate
//! digest, and require every candidate digest to match. Loading and hashing remain in rvl.
//! Unreadable or malformed lockfiles are I/O failures.

use std::path::{Path, PathBuf};

use cmdrvl_core::hash::{Algorithm, hash_bytes};
use cmdrvl_core::lock_matching::{Member, SelectError, Verdict, select};
use serde::{Deserialize, Serialize};

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

        let input_name = typed_name_for_matching(&input.to_string_lossy());
        let candidates: Vec<(&LoadedLock, usize, &str, &str)> = self
            .locks
            .iter()
            .enumerate()
            .flat_map(|(lock, loaded)| {
                loaded.file.members.iter().map(move |member| {
                    (
                        loaded,
                        lock,
                        member.path.as_str(),
                        member.bytes_hash.as_str(),
                    )
                })
            })
            .collect();
        let members = candidates.iter().map(|(_, lock, path, digest)| Member {
            lock: *lock,
            path,
            digest,
        });
        let selection = match select(&input_name, members) {
            Ok(selection) => selection,
            Err(SelectError::NotListed | SelectError::SeveralPaths(_)) => {
                return Err(LockFailure::NotLocked);
            }
            Err(SelectError::UnusableDigest(member)) => {
                let lockfile = self
                    .locks
                    .get(member.lock)
                    .map(|lock| lock.path.to_string_lossy().into_owned())
                    .unwrap_or_default();
                return Err(LockFailure::Lockfile {
                    lockfile,
                    error: format!(
                        "unsupported bytes_hash algorithm in '{}' (member '{}')",
                        member.digest, member.path
                    ),
                });
            }
        };

        let observed: Vec<(Algorithm, String)> = selection
            .algorithms()
            .into_iter()
            .map(|algorithm| (algorithm, hash_bytes(algorithm, &bytes)))
            .collect();
        let observed_refs: Vec<(Algorithm, &str)> = observed
            .iter()
            .map(|(algorithm, digest)| (*algorithm, digest.as_str()))
            .collect();

        match selection.judge(&observed_refs) {
            Ok(Verdict::Verified { locks, .. }) => {
                let lock = locks
                    .first()
                    .copied()
                    .ok_or_else(|| LockFailure::Lockfile {
                        lockfile: input.to_string_lossy().into_owned(),
                        error: "R1 selected no matching lock".to_string(),
                    })?;
                let loaded = self.locks.get(lock).ok_or_else(|| LockFailure::Lockfile {
                    lockfile: input.to_string_lossy().into_owned(),
                    error: "R1 selected an invalid lock position".to_string(),
                })?;
                Ok(vec![LockReceipt {
                    lockfile: loaded.path.to_string_lossy().into_owned(),
                    lock_hash: loaded.file.lock_hash.clone(),
                }])
            }
            Ok(Verdict::Drift { differing, .. }) => {
                let first = differing.first().ok_or_else(|| LockFailure::Lockfile {
                    lockfile: input.to_string_lossy().into_owned(),
                    error: "R1 reported drift without a differing member".to_string(),
                })?;
                let algorithm =
                    Algorithm::of(first.digest).ok_or_else(|| LockFailure::Lockfile {
                        lockfile: input.to_string_lossy().into_owned(),
                        error: "R1 reported an unknown digest algorithm".to_string(),
                    })?;
                let actual = observed
                    .iter()
                    .find_map(|(kind, digest)| match (*kind, algorithm) {
                        (Algorithm::Blake3, Algorithm::Blake3)
                        | (Algorithm::Sha256, Algorithm::Sha256) => Some(digest.as_str()),
                        _ => None,
                    })
                    .unwrap_or_default();
                Err(LockFailure::Drift {
                    lockfile: self
                        .locks
                        .get(first.lock)
                        .map(|lock| lock.path.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                    expected_hash: first.digest.to_string(),
                    actual_hash: actual.to_string(),
                })
            }
            Err(missing) => Err(LockFailure::Lockfile {
                lockfile: input.to_string_lossy().into_owned(),
                error: format!(
                    "R1 did not receive the computed {} digest",
                    missing.algorithm.prefix()
                ),
            }),
        }
    }
}

#[cfg(windows)]
fn typed_name_for_matching(file: &str) -> String {
    file.replace('\\', "/")
}

#[cfg(not(windows))]
fn typed_name_for_matching(file: &str) -> String {
    file.to_owned()
}

/// Hash `bytes` with the algorithm named by the prefix of `expected`. `None` if unsupported.
/// This remains a focused unit-test helper; production matching uses R1's algorithm list and
/// hashes each required algorithm before judging.
#[cfg(test)]
fn hash_like(bytes: &[u8], expected: &str) -> Option<String> {
    Algorithm::of(expected).map(|algorithm| hash_bytes(algorithm, bytes))
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
        let stale = format!("sha256:{}", "de".repeat(32));
        let err = verify(&input, lock_for(&input.to_string_lossy(), &stale)).unwrap_err();
        match err {
            LockFailure::Drift {
                expected_hash,
                actual_hash,
                ..
            } => {
                assert_eq!(expected_hash, stale);
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
        let err = verify(&input, lock_for(&input.to_string_lossy(), "sha256:00")).unwrap_err();
        assert!(matches!(err, LockFailure::Lockfile { .. }), "{err:?}");
        let err = verify(&input, scratch("bad.lock.json", "not json")).unwrap_err();
        assert!(matches!(err, LockFailure::Lockfile { .. }), "{err:?}");
        let wrong_version = scratch("v.lock.json", r#"{"version":"lock.v9","members":[]}"#);
        let err = verify(&input, wrong_version).unwrap_err();
        assert!(matches!(err, LockFailure::Lockfile { .. }), "{err:?}");
    }
}
