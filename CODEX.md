# CODEX.md - rvl

Codex agents inherit [`AGENTS.md`](./AGENTS.md). Read it first, then read [`README.md`](./README.md) and [`docs/PLAN_RVL.md`](./docs/PLAN_RVL.md) for user-facing and spec-level behavior.

Codex-specific reminders:

- Use `apply_patch` for manual edits.
- Prefer `rg` for discovery and `ast-grep` when Rust syntax structure matters.
- Do not touch files outside `/Users/zac/Source/cmdrvl/rvl` unless the user explicitly changes the scope.
- Do not bump `Cargo.toml` or cut a release unless the operator or orchestrator explicitly asks.
- For CRV1 work, report the registry hash contract and final handoff blocks exactly when requested.

Verification:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Before landing, sync Beads, commit real docs/code with real tests or documented verification, push `main`, and report any skipped inherited release-sync step.
