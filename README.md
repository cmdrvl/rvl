# rvl

**Reveal the smallest set of numeric changes that explain what actually changed.**

*Built for teams who reconcile CSV exports and need a deterministic verdict fast.*

---

## Why This Exists

Comparing CSV exports by hand is slow and noisy. `rvl` gives a single, copy-paste-able verdict:

- **REAL CHANGE**: the smallest ranked set of numeric deltas that explain the change.
- **NO REAL CHANGE**: confirmed, within tolerance.
- **REFUSAL**: when alignment or parsing is ambiguous, with a concrete next step.

No dashboards. No probabilistic scoring. Just a deterministic explanation or a refusal.

## What rvl does

- Aligns rows by `--key` (or by row order if no key is provided).
- Compares only common numeric columns.
- Produces an explainable top-K list of contributors with coverage.
- Refuses when rules are violated (e.g., mixed types, missingness, ambiguous delimiter).

## Install (dev)

No release artifacts yet. Build from source:

```bash
cargo build --release
./target/release/rvl --help
```

## Quickstart

```bash
rvl old.csv new.csv
```

Keyed alignment:

```bash
rvl old.csv new.csv --key id
```

JSON output:

```bash
rvl old.csv new.csv --json
```

## CLI

```bash
rvl <old.csv> <new.csv> [--key <column>] [--threshold <float>] [--tolerance <float>] [--delimiter <delim>] [--json]
```

Delimiter values:
- `comma`, `tab`, `semicolon`, `pipe`, `caret`
- `0xNN` (ASCII byte in `0x01-0x7F`, excluding line endings and `"`)
- single ASCII character (same constraints)

## Spec

The source of truth is `docs/PLAN_RVL.md`.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
