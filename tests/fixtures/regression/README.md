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

Notes
- Paths in the golden outputs are relative (e.g., `tests/fixtures/regression/basic_old.csv`) to keep tests stable across machines.
