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
  - `RVL_BAKEOFF_INPUTS` (comma-separated file paths)
  - `RVL_BAKEOFF_ITERS` (default 5)
  - `RVL_BAKEOFF_WARMUP` (default 1)
  - `RVL_BAKEOFF_DELIMITER` (optional: `comma|tab|semicolon|pipe|caret|0xNN|<char>`)

Measure throughput with a consistent tool (e.g., `time` or `hyperfine`) and
repeat runs to smooth variance. Use the same input files and capture command
lines in the report.

## Results

### Compatibility
| Parser | Corpus pass | Mismatches | Notes |
| --- | --- | --- | --- |
| csv (baseline) | Pass (`cargo test --test corpus_parse`) | 0 | Corpus parse/REFUSAL expectations matched. |
| candidate A | TBD | TBD | TBD |
| candidate B | TBD | TBD | TBD |

### Throughput / Memory
| Parser | Rows/sec | MB/sec | Peak RSS | Notes |
| --- | --- | --- | --- | --- |
| csv (baseline) | 26.2k | 4.38 | n/a | 1,000,000 rows, 11 cols; /usr/bin/time -l (RSS unavailable: sysctl kern.clockrate permission error); input size 167.27 MB; real 38.21s. |
| candidate A | TBD | TBD | TBD | TBD |
| candidate B | TBD | TBD | TBD | TBD |

## Conclusion
Baseline throughput measured for Rust `csv`; corpus compatibility currently
passes with 0 mismatches. Candidate parsers still TBD; do not switch unless a
candidate is >=25% faster with equal compatibility and acceptable memory.

## Next Steps
- Evaluate at least one alternative parser and record results.
- If a candidate wins, draft an integration plan and update the spec/roadmap.
