# Regression fixtures

These fixtures model representative CSV diffs from existing workflows.
Each case has expected human and JSON outputs checked by `tests/regression.rs`.

## basic
- Files: `basic_old.csv`, `basic_new.csv`
- Mode: row-order (no key)
- Change: single numeric delta in `amount`
- Expected outputs:
  - `basic.human.txt`
  - `basic.json`

## missingness_key
- Files: `missingness_key_old.csv`, `missingness_key_new.csv`
- Mode: key (`--key id`)
- Change: old has numeric value, new has missing value — triggers `E_MISSINGNESS`
- Verifies key value (not record number) appears in the refusal example
- Expected outputs:
  - `missingness_key.human.txt`
  - `missingness_key.json`

Notes
- Paths in the golden outputs are relative (e.g., `tests/fixtures/regression/basic_old.csv`) to keep tests stable across machines.
