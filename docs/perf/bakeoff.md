# Parser Bakeoff (v0 Gate)

## Goal
Benchmark the current Rust `csv` parser against viable alternatives and decide
whether to switch before declaring v0 complete. The bakeoff must measure:

- Throughput (rows/sec and MB/sec)
- Peak RSS (memory)
- Compatibility vs the rvl CSV corpus (parse/REFUSAL expectations)

Decision rule (from bd-1eh scope): switch only if a candidate is >=25% faster
with equal compatibility and acceptable memory.

## Candidates
Baseline:
- Rust `csv` crate (current)

Candidates (evaluate if practical):
- SIMD CSV (Rust implementation, if viable)
- Arrow / Polars CSV readers (if they can be used deterministically)

## Environment (record actual values)
- Machine: macbookpro.lan (x86_64)
- OS: macOS 26.2 (Build 25C56)
- Rust: rustc 1.94.0-nightly (f6a07efc8 2026-01-16)
- Cargo: cargo 1.94.0-nightly (6d1bd93c4 2026-01-10)
- Compiler flags: release profile (see Cargo.toml)

## Datasets
1. **Compatibility corpus**
   - `tests/fixtures/corpus/*.csv`
   - Expected outcomes are defined in `tests/fixtures/corpus/README.md`

2. **Large-file throughput set**
   - Use the procedure in `scripts/perf/large_file.md` to generate large CSVs
   - Use both row-order and key-style shapes where possible

## Methodology
For each candidate:
1. Parse the corpus and record:
   - Parse success/refusal vs expected outcome
   - Any mismatches (must be zero to pass)
2. Parse large-file datasets and record:
   - Rows/sec and MB/sec
   - Peak RSS

Harness:
- `cargo bench --bench bakeoff`
- Env:
  - `RVL_BAKEOFF_PARSER` (`csv` or `simd_csv`, default `csv`)
  - `RVL_BAKEOFF_INPUTS` (comma-separated file paths)
  - `RVL_BAKEOFF_ITERS` (default 5)
  - `RVL_BAKEOFF_WARMUP` (default 1)
  - `RVL_BAKEOFF_DELIMITER` (optional: `comma|tab|semicolon|pipe|caret|0xNN|<char>`)

Measure throughput with a consistent tool (e.g., `time` or `hyperfine`) and
repeat runs to smooth variance. Use the same input files and capture command
lines in the report.

Bakeoff harness usage (opt-in):
```bash
cargo bench --bench bakeoff
```

Select parser:
- `RVL_BAKEOFF_PARSER=csv` (default)
- `RVL_BAKEOFF_PARSER=simd_csv` (requires `simd-csv`; skips backslash-escape cases)
- `RVL_BAKEOFF_PARSER=arrow` (Arrow `arrow-csv`)
- `RVL_BAKEOFF_PARSER=polars` (Polars CSV reader; skips backslash-escape cases)

Other knobs:
- `RVL_BAKEOFF_ITERS` (default 5)
- `RVL_BAKEOFF_WARMUP` (default 1)
- `RVL_BAKEOFF_DELIMITER` (forces delimiter, e.g. `comma`, `tab`, `0x1F`)
- `RVL_BAKEOFF_INPUTS` (comma-separated list of file paths)

## Results

### Compatibility
| Parser | Corpus pass | Mismatches | Notes |
| --- | --- | --- | --- |
| csv (baseline) | Pass (`cargo test --test corpus_parse`) | 0 | Corpus parse/REFUSAL expectations matched. |
| simd-csv 0.10.3 | Partial | 1 | Fails `backslash_escape.csv` (no backslash-escape support). |
| arrow-csv | TBD | TBD | TBD |
| polars | TBD | TBD | TBD |
| candidate B | TBD | TBD | TBD |

### Throughput / Memory
| Parser | Rows/sec | MB/sec | Peak RSS | Notes |
| --- | --- | --- | --- | --- |
| csv (baseline) | 1.97M | 165.10 | n/a | `/tmp/rvl-perf/{old,new}.csv` (1,000,001 rows incl header, 83.63 MB). `RVL_BAKEOFF_PARSER=csv RVL_BAKEOFF_INPUTS=/tmp/rvl-perf/old.csv,/tmp/rvl-perf/new.csv RVL_BAKEOFF_ITERS=5 RVL_BAKEOFF_WARMUP=1 cargo bench --bench bakeoff` (avg_ms ~506.7, avg of old/new cases). |
| simd-csv 0.10.3 | 2.35M | 196.22 | n/a | Same inputs; `RVL_BAKEOFF_PARSER=simd_csv RVL_BAKEOFF_INPUTS=/tmp/rvl-perf/old.csv,/tmp/rvl-perf/new.csv RVL_BAKEOFF_ITERS=5 RVL_BAKEOFF_WARMUP=1 cargo bench --bench bakeoff` (avg_ms ~426.3, avg of old/new cases). Harness skips backslash-escape files. |
| arrow-csv | TBD | TBD | TBD | TBD |
| polars | TBD | TBD | TBD | TBD |
| candidate B | TBD | TBD | TBD | TBD |

### Bakeoff Harness Run (2026-02-04)
Ran `cargo bench --bench bakeoff` with large inputs:
- `RVL_BAKEOFF_INPUTS=/tmp/rvl-perf/old.csv,/tmp/rvl-perf/new.csv` (`RVL_BAKEOFF_ITERS=5`, `RVL_BAKEOFF_WARMUP=1`)
- `RVL_BAKEOFF_PARSER=csv`: avg_ms ~506.7, rows=1,000,001, rows/sec ~1.97M, MB/sec ~165.10
- `RVL_BAKEOFF_PARSER=simd_csv`: avg_ms ~426.3, rows=1,000,001, rows/sec ~2.35M, MB/sec ~196.22
 - `RVL_BAKEOFF_PARSER=arrow` / `polars`: pending (dependencies not available in offline environment)

Note: the bakeoff harness is in-memory and does not include disk I/O.

## Conclusion
Baseline Rust `csv` passes the corpus (0 mismatches). simd-csv is ~18.9% faster
in the parser-only bakeoff but skips backslash-escape cases in the harness and
does not meet the >=25% throughput gate. Keep Rust `csv` for v0.

## Next Steps
- If needed, evaluate Arrow/Polars CSV readers and record results.
- If a candidate wins (>=25% faster + 0 mismatches), draft an integration plan and update the spec/roadmap.
