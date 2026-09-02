# AGENTS.md - rvl

> Repo-specific guidelines. Inherits shared rules from [`../AGENTS.md`](../AGENTS.md).

---

## Quick Reference

```bash
# Find and claim work
br ready --limit 0 --json
br show <id> --json
br update <id> --status in_progress --json

# Verify changes
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test

# Agent discovery surfaces
rvl --robot-triage
rvl capabilities --json
rvl robot-docs guide
rvl doctor health --json
rvl doctor --fix  # exits 2 with read-only alternatives
```

Do not bump `Cargo.toml` for normal work. A push to `main` that changes `Cargo.toml` triggers the release and Homebrew tap workflow; releases are cut only when the operator or orchestrator asks.

---

## rvl - What This Project Does

`rvl` compares two CSVs and reveals the smallest set of numeric changes that explain what actually changed.

Position in the stack:

```text
        profile YAML + optional column registry
                         |
                         v
shape structural check -> rvl numeric explanation -> witness / capsule handoff
                         |
                         v
                    lock / pack consumers
```

### What rvl Owns

- CSV parsing and delimiter detection for two-file comparisons.
- Row alignment by `--key`, profile key, composite profile key, or row order.
- Numeric comparison over common/profile-scoped columns.
- Contributor ranking by absolute delta and coverage threshold.
- Deterministic human and JSON output for `REAL CHANGE`, `NO REAL CHANGE`, and `REFUSAL`.
- Read-only agent discovery surfaces.
- Replay capsule generation and witness metadata for rvl runs.
- Runtime profile loading, profile-scoped comparison, and registry-backed header canonicalization.

### What rvl Does Not Own

- Structural compatibility policy. Run `shape` first when schema/key compatibility is in doubt.
- Profile authoring, validation, freezing, or canonical profile serialization. Those belong in `profile`.
- Durable lockfile provenance. CRV1 provenance for frozen profiles rides in `lock.v0` `profiles[]`, not in extra rvl receipt fields.
- Remote registry discovery or fetching. Registry paths are local filesystem references only.
- General text diffing. `--audit-fields` is profile-scoped, exhaustive, exact-byte field audit only.
- Relative or percentage tolerance. v0 has absolute per-cell `--tolerance` only.
- Multi-file, directory, or time-series comparison.

### Source of Truth

- **Spec:** [`docs/PLAN_RVL.md`](./docs/PLAN_RVL.md) - CLI behavior, parsing, refusal codes, output formatting, profiles, capsules, and tests. Follow it verbatim.
- **User docs:** [`README.md`](./README.md) - install, usage, automation examples, troubleshooting.
- **Release notes/process:** [`docs/release.md`](./docs/release.md) and [`.github/workflows/release.yml`](./.github/workflows/release.yml).
- **Machine contract:** [`operator.json`](./operator.json) and `rvl --describe`.

Do not invent behavior not present in the plan.

---

## Core Behavior

- Align rows by profile key, `--key`, or row order.
- Compare only common numeric columns, scoped by profile when active.
- Emit exactly one outcome: `REAL CHANGE`, `NO REAL CHANGE`, or `REFUSAL`.
- Refuse with a concrete next step instead of guessing.
- Treat output formatting as part of the public contract.

---

## Output Contract

rvl has two output modes:

- **Human (default):** Emit exactly one outcome: `REAL CHANGE`, `NO REAL CHANGE`, or `REFUSAL`.
  - `REAL CHANGE` / `NO REAL CHANGE` go to stdout; `REFUSAL` goes to stderr.
- **`--json`:** Emit exactly one JSON object on stdout for all outcomes; stderr is reserved for process-level failures only.

Follow the exact headers, wording, and schema in `docs/PLAN_RVL.md`. Do not add banners, progress text, debug logs, or ad-hoc fields.

### Agent Discovery Surfaces

- Keep `rvl --robot-triage`, `rvl capabilities --json`, and `rvl robot-docs guide` read-only.
- These surfaces must not parse CSVs, write witness ledgers, create capsules, touch the network, or change comparison behavior.
- `rvl doctor --fix` is intentionally unavailable; it must exit 2, emit only stderr, and point agents to read-only alternatives.

---

## CSV and Identifier Rules

- Parsing, delimiter detection, and refusal reasons must follow `docs/PLAN_RVL.md`.
- Never silently reinterpret data; refuse with a concrete next step.
- Identifier rendering and JSON encoding must follow the `u8:` / `hex:` rules in `docs/PLAN_RVL.md`.
- Preserve arbitrary CSV bytes where the parser permits them. Do not route identifiers through lossy string transformations unless the existing spec requires it.
- Header canonicalization happens before key lookup, profile scoping, counts, contributor labels, capsules, and witness metadata.

---

## Column Registry Contract

When an active profile has `column_registry`, rvl computes the deterministic registry content hash implemented in [`src/profile.rs`](./src/profile.rs). This is the CRV1 cross-tool contract that `profile`, `shape`, and `rvl` must match byte-for-byte.

CRV1 does not redefine `profile_sha256`. Frozen profiles may carry `column_registry_hash` in their canonical YAML so `profile_sha256` covers registry bytes transitively; rvl still recomputes the registry content hash from the loaded registry files at read time for runtime audit, witness, and capsule metadata.

Registry path resolution:

- Absolute `column_registry` references are used as-is.
- Relative references resolve relative to the profile YAML file's parent directory.
- The locator string and resolved absolute directory path do not affect the hash.

File selection and order:

- `registry.json` is first and must exist, parse as JSON, and be a JSON object.
- Then include direct child files with UTF-8 names and `.json` extension.
- Exclude `registry.json` and `_build.json`.
- Ignore subdirectories.
- Sort included mapping files by file name ascending.

Framing and hash:

```text
relative_path_bytes 0x00 ascii_decimal_byte_len 0x00 raw_file_bytes 0xff
```

- Repeat the frame for each selected file.
- Hash the framed byte stream with BLAKE3.
- `hash_bytes()` returns lowercase hex with no prefix.
- rvl stores the registry hash as `blake3:<hex>`.
- Hashing uses raw `fs::read()` bytes, not parsed JSON strings, canonical JSON, or normalized text.

Alias semantics:

- Included mapping files must parse as JSON arrays of entries with `input`, `canonical_id`, `canonical_type`, and `rule_id`.
- Only `canonical_type == "column_name"` entries populate header aliases.
- Duplicate alias inputs are first-wins in sorted file order.
- Non-column entries are skipped for aliasing but still affect the registry content hash because their file bytes are framed.

---

## Project Structure

| Path | Purpose |
|------|---------|
| `src/main.rs` | CLI entry point |
| `src/lib.rs` | Top-level dispatch and output routing |
| `src/cli/` | Argument parsing, exit code mapping |
| `src/csv/` | CSV parsing, dialect detection, record handling |
| `src/normalize/` | Byte-level trimming and normalization helpers |
| `src/alignment/` | Key parsing, row-order/key/composite-key alignment |
| `src/numeric/` | Numeric parsing and missing-token handling |
| `src/diff/` | Numeric comparison, contributors, coverage |
| `src/output/` | Human and JSON output formatting |
| `src/refusal/` | Refusal codes, details, remediation text |
| `src/profile.rs` | Profile loading and column registry canonicalization |
| `src/orchestrator.rs` | End-to-end comparison orchestration |
| `src/orchestrator/capsule.rs` | Replay capsule generation |
| `src/witness/` | Witness records, hashing, ledger query |
| `tests/` | Integration, golden, regression, and contract tests |
| `docs/PLAN_RVL.md` | Authoritative implementation plan |
| `operator.json` | Machine-readable CLI/operator contract |

---

## Toolchain

- **Language:** Rust
- **Package manager:** Cargo only
- **Toolchain:** stable via [`rust-toolchain.toml`](./rust-toolchain.toml)
- **Edition:** Rust 2024
- **Unsafe code:** forbidden by inherited monorepo policy
- **Dependency policy:** keep dependencies explicit, small, and pinned enough for repeatable release builds

Key dependencies:

| Crate | Purpose |
|-------|---------|
| `clap` | CLI parsing |
| `csv` | CSV parsing |
| `serde` / `serde_json` | JSON output, registry parsing, witness/capsule metadata |
| `blake3` | Content hashes for registries, capsules, and witness artifacts |
| `jsonschema` | JSON schema validation in tests |
| `arrow-csv`, `polars`, `simd-csv` | Parser bakeoff benchmarks only |

Release profile is size-oriented and must remain aligned with `../AGENTS.md`:

```toml
[profile.release]
opt-level = "z"
lto = true
codegen-units = 1
panic = "abort"
strip = true
```

---

## Build, Test, and Lint

Run the full quality gate after any substantive change, including doc changes tied to release work:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Focused checks:

| Change area | Useful command |
|-------------|----------------|
| CLI flags / exits | `cargo test --test cli_exit` |
| Output routing | `cargo test --test exit_routing` |
| Human/JSON goldens | `cargo test --test output_golden` |
| CSV parsing / dialects | `cargo test --test corpus_parse` and `cargo test --test csv_dialect` |
| Key alignment | `cargo test --test keys` and `cargo test --test row_order` |
| Numeric parsing | `cargo test --test numeric_parsing` and `cargo test --test numeric_missing` |
| Profiles / registries | `cargo test --test profile_integration` |
| Exhaustive audit | `cargo test --test exhaustive` |
| Capsules | `cargo test --test capsule_replay` |
| Witness | `cargo test --test witness` and `cargo test --test witness_schema` |
| Discovery/doctor | `cargo test --test doctor` |

Before committing changed code, also run UBS on changed files when available:

```bash
ubs $(git diff --name-only --cached)
```

---

## CI and Release

| Workflow | Trigger | Purpose | Blocking |
|----------|---------|---------|----------|
| `.github/workflows/ci.yml` `fmt` | PR, push to `main`, published release | `cargo fmt --check` | Yes |
| `.github/workflows/ci.yml` `clippy` | PR, push to `main`, published release | `cargo clippy --all-targets -- -D warnings` | Yes |
| `.github/workflows/ci.yml` `test` | PR, push to `main`, published release | `cargo test` | Yes |
| `.github/workflows/ci.yml` `build` | Published release or tag ref | Release build smoke test on Linux/macOS/Windows | Yes when run |
| `.github/workflows/release.yml` | Manual dispatch or push to `main` affecting `Cargo.toml` | Builds archives, signs checksums, publishes release, updates Homebrew tap | Release only |

Release constraints:

- Do not change `Cargo.toml` version unless the release bead or orchestrator explicitly asks.
- Keep `Cargo.lock` synced before release workflows that use `--locked`.
- The orchestrator cuts cross-repo releases after reviewing work.

---

## Beads Workflow

Use Beads as the source of truth for task state. Beads is non-invasive: it never runs git commands.

```bash
br ready --limit 0 --json
br show <id> --json
br update <id> --status in_progress --json
br comments add <id> "status or evidence note"
br close <id> --reason "Completed: <tests/evidence>"
br sync --flush-only
```

Rules:

- Always use JSON output for agent-readable Beads commands.
- `br ready --limit 0` avoids silent truncation at 20 items.
- Claim a bead with `--status in_progress` before editing.
- Close with cited evidence: test names and commit SHA when available.
- Run `br dep cycles --json` before closing doc-audit work.
- After `br sync --flush-only`, stage `.beads/` yourself; Beads does not commit.

Use `bv` only with robot flags:

```bash
bv --robot-next
bv --robot-triage
bv --robot-plan
```

Never run bare `bv`; it opens an interactive TUI.

---

## Agent Coordination

Current CRV1 cross-repo initiative rule: one agent works only in this repo and there are no shared files across `profile`, `verify`, `lock`, `rvl`, and `shape`, so Agent Mail file reservations are not needed for that initiative.

For same-repo multi-agent sessions:

- Register in Agent Mail for this repository.
- Reserve only the exact files you will edit.
- Use the bead ID as the Mail `thread_id`, subject prefix, and reservation reason.
- Send start/finish updates in the bead thread.
- Release reservations when done.

Working tree rules:

- Never stash, revert, delete, or overwrite another agent's work.
- Treat unexpected changes as concurrent work unless evidence proves otherwise.
- If unrelated files are dirty, leave them alone.
- If touched files are dirty, read them and work with the existing changes.

---

## Tool Guidance

- Use `rg` / `rg --files` first for text and file discovery.
- Use `ast-grep` when Rust syntax structure matters, especially for refactors or finding API shapes without matching comments.
- Use structured parsers for JSON/YAML/TOML when changing machine-readable data.
- Use UBS on changed files before commit when available.
- Use CASS/session search only to recover historical context; do not substitute it for reading the current source and spec.

Examples:

```bash
rg -n "E_PROFILE_REGISTRY|column_registry" src tests docs
ast-grep run -l Rust -p 'aliases.entry($KEY).or_insert($VALUE)'
ubs src/profile.rs tests/profile_integration.rs
```

---

## Harness Docs

Harness-specific notes live in:

- [`CLAUDE.md`](./CLAUDE.md)
- [`GEMINI.md`](./GEMINI.md)
- [`CODEX.md`](./CODEX.md)

These files point back here for shared policy. Do not duplicate the full root guidance into harness files.

---

## Session Completion

Work is not complete until the requested code/docs, Beads state, git commit, and push are all done.

1. Confirm the work matches `docs/PLAN_RVL.md` and any current orchestrator instructions.
2. Run the quality gate:
   ```bash
   cargo fmt --check
   cargo clippy --all-targets -- -D warnings
   cargo test
   ```
3. Run focused tests relevant to the change if the full gate is not enough to identify coverage.
4. Run `br dep cycles --json`.
5. Close completed beads with evidence, or leave them open with a precise comment saying what remains.
6. Run `br sync --flush-only`.
7. Commit:
   ```bash
   git add .beads/ <changed-files>
   git commit -m "<type>: <summary> (<bead-id>)"
   git push origin main
   ```
8. For ordinary monorepo closeout, sync the legacy branch after `main` per `../AGENTS.md`:
   ```bash
   git push origin main:master
   ```
9. Verify `git status` shows the branch up to date with `origin/main`.
10. Report what changed, what was validated, which beads closed, and any release warnings.

When an orchestrator supplies a narrower landing protocol, follow the newest user instruction and call out any skipped inherited step in the handoff.
