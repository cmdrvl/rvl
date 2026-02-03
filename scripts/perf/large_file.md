# Large-file performance smoke test

This is an opt-in, local sanity check to confirm the row-order path remains I/O bound and stable.
It is not meant for CI and does not set hard performance budgets.

## Goals
- Ensure the row-order path can process large CSVs without unexpected slowdowns.
- Capture a repeatable baseline for comparison after major changes.

## Generate Large Fixtures (1M rows)
Pick a working directory, for example `"/tmp/rvl-perf"`, then run:

```bash
mkdir -p /tmp/rvl-perf
python3 - <<'PY'
import os

out_dir = "/tmp/rvl-perf"
old_path = os.path.join(out_dir, "old.csv")
new_path = os.path.join(out_dir, "new.csv")

rows = 1_000_000
cols = 10
change_row = 500_000  # set to 0 for NO REAL CHANGE

header = "id," + ",".join(f"c{j}" for j in range(1, cols + 1))

with open(old_path, "w", encoding="utf-8") as f_old, open(new_path, "w", encoding="utf-8") as f_new:
    f_old.write(header + "\n")
    f_new.write(header + "\n")
    for i in range(1, rows + 1):
        values = [str(i * j) for j in range(1, cols + 1)]
        old_row = str(i) + "," + ",".join(values) + "\n"

        if change_row and i == change_row:
            values = values.copy()
            values[0] = str(int(values[0]) + 1)
        new_row = str(i) + "," + ",".join(values) + "\n"

        f_old.write(old_row)
        f_new.write(new_row)

print("Wrote:", old_path, new_path)
PY
```

Notes:
- `change_row = 0` yields identical files (expected `NO REAL CHANGE`).
- A non-zero `change_row` yields a single-cell delta (expected `REAL CHANGE`).

## Build rvl (release)
```bash
cargo build --release
```

## Run the smoke test
```bash
/usr/bin/time -l ./target/release/rvl /tmp/rvl-perf/old.csv /tmp/rvl-perf/new.csv
```

Linux alternative:
```bash
/usr/bin/time -v ./target/release/rvl /tmp/rvl-perf/old.csv /tmp/rvl-perf/new.csv
```

Optional key-mode pass (higher memory use):
```bash
./target/release/rvl /tmp/rvl-perf/old.csv /tmp/rvl-perf/new.csv --key id
```

## Record results
Capture the following:
- Rows and columns tested.
- Elapsed time.
- Max RSS (from `time` output).
- Throughput (rows/sec = rows / elapsed time).

## Sample results (2026-02-03, macOS)
- Build: `cargo build --release` (rustup `cargo 1.94.0-nightly`)
- Rows/cols: 1,000,000 rows, 11 numeric columns (1 change)
- Command: `/usr/bin/time -l ./target/release/rvl /tmp/rvl-perf/old.csv /tmp/rvl-perf/new.csv`
- Elapsed time: 47.05s
- Max RSS: 2,660,175,872 bytes (~2.48 GiB)
- Throughput: ~21.3k rows/sec

## Interpretation
- The row-order path should stay I/O bound with a stable wall-clock time.
- Key-mode uses an in-memory map and will require more RAM (expected).
