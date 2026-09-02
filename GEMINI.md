# GEMINI.md - rvl

Gemini agents follow [`AGENTS.md`](./AGENTS.md). The behavior source of truth is [`docs/PLAN_RVL.md`](./docs/PLAN_RVL.md).

Use JSON/robot surfaces for unattended work:

```bash
br ready --limit 0 --json
br show <id> --json
rvl --robot-triage
rvl capabilities --json
rvl robot-docs guide
```

Verification:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

When reviewing profile or registry work, preserve exact-byte semantics. Column registry paths resolve relative to the profile YAML file, registry hashes are BLAKE3 over the framed raw file bytes, and rvl must refuse instead of fuzzy-matching headers.
