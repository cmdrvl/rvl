# CLAUDE.md - rvl

Claude agents inherit [`AGENTS.md`](./AGENTS.md). Read it first, then read [`docs/PLAN_RVL.md`](./docs/PLAN_RVL.md) before changing behavior.

Claude-specific local state:

- `.claude/settings.local.json` is an operator-maintained permission allow-list.
- No repo-local Claude hooks are documented in this repository.
- If a harness permission blocks an action, use the read-only alternatives in `AGENTS.md` and leave a Beads comment if work cannot continue.

Verification:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Keep rvl's output contract exact: one outcome only, JSON on stdout for `--json`, and read-only discovery surfaces. Do not bump `Cargo.toml` or cut a release unless the operator or orchestrator explicitly asks.
