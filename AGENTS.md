# AGENTS.md — rvl

> Repo-specific guidelines. Inherits shared rules from [`../AGENTS.md`](../AGENTS.md).

---

## rvl — What This Project Does

`rvl` compares two CSVs and reveals the smallest set of numeric changes that explain what actually changed.

### Source of Truth

- **Spec:** [`docs/PLAN_RVL.md`](./docs/PLAN_RVL.md) — CLI behavior, parsing, refusal codes, and output formatting. Follow it verbatim.
- Do not invent behavior not present in the plan.

### Core Behavior (v0)

- Align rows by `--key` (when provided) or by row order.
- Compare only common numeric columns.
- Emit exactly one outcome: `REAL CHANGE`, `NO REAL CHANGE`, or `REFUSAL`.

### Key Files

| File | Purpose |
|------|---------|
| `src/main.rs` | CLI entry and orchestration |
| `src/csv/` | Parsing and dialect detection |
| `src/diff/` | Numeric comparison and contributors |

---

## Output Contract

rvl has two output modes:

- **Human (default):** Emit exactly one outcome: `REAL CHANGE`, `NO REAL CHANGE`, or `REFUSAL`.
  - `REAL CHANGE` / `NO REAL CHANGE` go to stdout; `REFUSAL` goes to stderr.
- **`--json`:** Emit exactly one JSON object on stdout for all outcomes; stderr is reserved for process-level failures only.

Follow the exact headers, wording, and schema in `docs/PLAN_RVL.md` — no extra banners or ad-hoc text.

---

## CSV Parsing Notes

- Parsing, delimiter detection, and refusal reasons must follow `docs/PLAN_RVL.md`.
- Never silently reinterpret data; refuse with a concrete next step.
- Identifier rendering and JSON encoding must follow the `u8:` / `hex:` rules in `docs/PLAN_RVL.md`.

---

## Performance Goals

- Stream inputs where possible; avoid loading both files unless `--key` requires it.
- Keep hot loops allocation-light and deterministic.
