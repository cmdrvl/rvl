# rvl Agent Ergonomics Handoff

Completed pass 1 on 2026-06-10.

## Applied

- Added top-level `rvl --robot-triage`.
- Added top-level `rvl capabilities --json`.
- Added top-level `rvl robot-docs guide`.
- Added safe `rvl doctor --fix` refusal with exact alternatives.
- Updated `operator.json`, README, AGENTS.md, and `docs/PLAN_RVL.md`.
- Bumped version to `0.7.0`.
- Hardened Homebrew formula generation.

## Validation

- `cargo check`
- `cargo test --test doctor`
- audit regression scripts R-001 through R-004
- intent corpus: 130 entries, 0 silent failures, 0 useless errors

## Notes

The skill preflight reported missing `flock` on macOS; this pass continued single-agent. Core CSV comparison output was intentionally left unchanged.
