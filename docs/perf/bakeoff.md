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
- Machine: TBD
- OS: TBD
- Rust: TBD
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

Measure throughput with a consistent tool (e.g., `time` or `hyperfine`) and
repeat runs to smooth variance. Use the same input files and capture command
lines in the report.

## Results (fill in)

### Compatibility
| Parser | Corpus pass | Mismatches | Notes |
| --- | --- | --- | --- |
| csv (baseline) | TBD | TBD | TBD |
| candidate A | TBD | TBD | TBD |
| candidate B | TBD | TBD | TBD |

### Throughput / Memory
| Parser | Rows/sec | MB/sec | Peak RSS | Notes |
| --- | --- | --- | --- | --- |
| csv (baseline) | TBD | TBD | TBD | TBD |
| candidate A | TBD | TBD | TBD | TBD |
| candidate B | TBD | TBD | TBD | TBD |

## Conclusion
TBD after measurements. Default position: keep Rust `csv` unless a candidate
meets the >=25% throughput gain with equal compatibility and acceptable memory.

## Next Steps
- Run the bakeoff and fill in the results tables.
- If a candidate wins, draft an integration plan and update the spec/roadmap.
