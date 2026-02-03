# Release & Distribution Plan (60s install)

Goal: install rvl on macOS in under 60 seconds via prebuilt binaries and a Homebrew tap.

## Deliverables
- Signed macOS binaries (universal or per-arch) attached to GitHub Releases.
- Homebrew tap formula referencing release artifacts with SHA256.
- Minimal install instructions (copy/paste).

## Release Pipeline Outline
1. Tag version in `Cargo.toml` (semver).
2. Build release binaries on CI.
3. Attach artifacts to a GitHub Release.
4. Update Homebrew tap formula with new version + SHA256.
5. Validate install from scratch on a clean macOS machine.

## Release Checklist
1. Verify local quality gates:
   - `cargo fmt --check`
   - `cargo clippy --all-targets -- -D warnings`
   - `cargo test`
2. Update `Cargo.toml` version.
3. Commit with a release summary.
4. `git push origin main` and keep `master` synced: `git push origin main:master`.
5. CI builds release binaries for:
   - `x86_64-apple-darwin`
   - `aarch64-apple-darwin`
6. Create GitHub Release with artifacts and checksums.
7. Update Homebrew tap formula:
   - Source URL points to the release artifact.
   - SHA256 matches artifact.
   - Formula installs `rvl` binary into `bin`.
8. Smoke test:
   - `brew tap <org>/rvl`
   - `brew install rvl`
   - `rvl --help` and a small CSV diff.

## Homebrew Tap Notes
- Keep the formula minimal and deterministic.
- Avoid build-from-source during install (use prebuilt artifacts).
- Ensure `--version` matches the tag in `Cargo.toml`.

## Rollback Plan
- If a release is broken:
  - Yank the Homebrew formula version or revert the tap to the prior SHA.
  - Mark the GitHub Release as broken with a clear note.

