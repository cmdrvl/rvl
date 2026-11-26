//! Only `RVL_REDACTION_KEY` selects keyed markers. The family-wide `CMDRVL_REDACTION_KEY`, which
//! `cmdrvl_core::redact::select_key` knows as a fallback, must have no effect on rvl: not a
//! usable value, not a short one, not an empty one.

use std::path::PathBuf;
use std::process::{Command, Output};

const OWN_KEY: &str = "RVL_REDACTION_KEY";
const SHARED_KEY: &str = "CMDRVL_REDACTION_KEY";

fn scratch(old: &str, new: &str) -> (PathBuf, PathBuf) {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "rvl-redaction-key-env-{}-{id}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let (old_path, new_path) = (dir.join("old.csv"), dir.join("new.csv"));
    std::fs::write(&old_path, old).expect("write old");
    std::fs::write(&new_path, new).expect("write new");
    (old_path, new_path)
}

fn run(files: &(PathBuf, PathBuf), shared: Option<&str>, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rvl"));
    command.env_remove(OWN_KEY).env_remove(SHARED_KEY);
    if let Some(shared) = shared {
        command.env(SHARED_KEY, shared);
    }
    command
        .arg(&files.0)
        .arg(&files.1)
        .args(args)
        .arg("--no-witness")
        .output()
        .expect("run rvl")
}

#[test]
fn shared_family_key_variable_changes_nothing() {
    let numeric = scratch("id,v\nk1,1\nk2,2\n", "id,v\nk1,500\nk2,2\n");
    let categorical = scratch("id,s\nk1,open\nk2,shut\n", "id,s\nk1,shut\nk2,open\n");
    let refusal = scratch("id,v\nk1,1\nk1,2\n", "id,v\nk1,1\nk2,2\n");
    let cases: [(&(PathBuf, PathBuf), &[&str]); 3] = [
        (&numeric, &["--key", "id", "--json"]),
        (&categorical, &["--key", "id", "--non-numeric", "--json"]),
        (&refusal, &["--key", "id", "--json"]),
    ];

    for (files, args) in cases {
        let unset = run(files, None, args);
        let stdout = String::from_utf8_lossy(&unset.stdout).into_owned();
        assert!(stdout.contains("redacted:ref:"), "{stdout}");
        for shared in ["0123456789abcdef0123456789abcdef", "short", ""] {
            let with_shared = run(files, Some(shared), args);
            assert_eq!(with_shared.status.code(), unset.status.code(), "{shared:?}");
            assert_eq!(
                String::from_utf8_lossy(&with_shared.stdout),
                stdout,
                "{shared:?}"
            );
            assert_eq!(with_shared.stderr, unset.stderr, "{shared:?}");
        }
    }
}
