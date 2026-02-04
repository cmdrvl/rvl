# rvl

**Reveal the smallest set of numeric changes that explain what actually changed.**

*Built for teams who reconcile CSV exports and need a deterministic verdict fast.*

---

## Why This Exists

Comparing CSV exports by hand is slow and noisy — Excel hell, brittle scripts, eyeballing numbers. `rvl` replaces all of that with one trusted command that gives a single, copy-paste-able verdict:

- **REAL CHANGE** — the smallest ranked set of numeric deltas that explain the change.
- **NO REAL CHANGE** — confirmed within tolerance, with proof.
- **REFUSAL** — when alignment or parsing is ambiguous, with a concrete next step (never a dead end).

No dashboards. No probabilistic scoring. Just deterministic arithmetic or a refusal.

---

## Install

**Homebrew (macOS / Linux):**

```bash
brew install cmdrvl/tap/rvl
```

**Shell script (macOS / Linux):**

```bash
curl -fsSL https://raw.githubusercontent.com/cmdrvl/rvl/main/scripts/install.sh | bash
```

**Windows (PowerShell):**

```powershell
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process -Force; iex ((New-Object System.Net.WebClient).DownloadString('https://raw.githubusercontent.com/cmdrvl/rvl/main/scripts/install.ps1'))
```

**From source:**

```bash
cargo build --release
./target/release/rvl --help
```

Prebuilt binaries are available for x86_64 and ARM64 on Linux, macOS, and Windows (x86_64). Each release includes SHA256 checksums, cosign signatures, and an SBOM.

---

## Quickstart

Compare two CSVs by row order:

```bash
rvl old.csv new.csv
```

Align rows by a key column:

```bash
rvl old.csv new.csv --key id
```

Machine-readable JSON:

```bash
rvl old.csv new.csv --json
```

---

## CLI Reference

```
rvl <old.csv> <new.csv> [OPTIONS]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--key <column>` | string | *(none)* | Align rows by key column value. Without this, rows align by position (1st↔1st, 2nd↔2nd, etc.). |
| `--threshold <float>` | float | `0.95` | Coverage target (0 < x ≤ 1.0). The minimum fraction of total numeric change that the top contributors must explain. |
| `--tolerance <float>` | float | `1e-9` | Per-cell noise floor (x ≥ 0). Absolute deltas ≤ this value are treated as zero. |
| `--delimiter <delim>` | string | *(auto-detect)* | Force CSV delimiter for both files. See [Delimiter](#delimiter) below. |
| `--json` | flag | `false` | Emit a single JSON object on stdout instead of human-readable output. |

Invalid `--threshold` or `--tolerance` values are CLI argument errors (exit 2).

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | NO REAL CHANGE |
| `1` | REAL CHANGE |
| `2` | REFUSAL or CLI error |

### Output Routing

| Mode | REAL CHANGE | NO REAL CHANGE | REFUSAL |
|------|-------------|----------------|---------|
| Human (default) | stdout | stdout | stderr |
| `--json` | stdout | stdout | stdout |

In `--json` mode, stderr is reserved for process-level failures only (CLI parse errors, panics).

---

## The Three Outcomes

`rvl` always produces exactly one of three outcomes. There are no partial results, "and N more" buckets, or probabilistic scores.

### 1. REAL CHANGE

Printed when the top contributors (up to 25) explain ≥ threshold of total numeric change.

```
RVL

REAL CHANGE

Compared: old.csv -> new.csv
Alignment: key=id
Columns: common=15 old_only=2 new_only=1
Checked: 4,183 rows, 12 numeric columns (50,196 cells)
Dialect(old): delimiter=, quote=" escape=none
Dialect(new): delimiter=, quote=" escape=none
Ranking: abs(delta) (unscaled)
Settings: threshold=95.0% tolerance=1e-9

3 cells explain 95.2% of total numeric change (threshold 95.0%):

1. NVDA.market_value  +1842100  (123 -> 1842223)
2. UST10Y.price       -0.37     (4.21 -> 3.84)
3. EURUSD.fx_rate     +0.0013   (1.0842 -> 1.0855)

Everything else in common numeric columns is <= tolerance or in the tail (not required to reach threshold).
```

**How to read this:**
- **3 cells explain 95.2%** — only 3 numeric cells (out of 50,196) account for 95.2% of all numeric change.
- **Contributors** — ranked by `abs(delta)`, largest first. Each shows the cell label (`row_id.column`), signed delta, and old → new values.
- **Coverage** — cumulative share of total change (L1 distance). rvl prints the smallest prefix of contributors whose cumulative coverage reaches the threshold.
- **Threshold** — if the top 25 contributors can't reach 95%, rvl refuses (`E_DIFFUSE`) instead of printing a misleading partial list.

### 2. NO REAL CHANGE

Printed when all numeric deltas are within tolerance.

```
RVL

NO REAL CHANGE

Compared: old.csv -> new.csv
Alignment: row-order (no key)
Columns: common=15 old_only=2 new_only=1
Checked: 4,183 rows, 12 numeric columns (50,196 cells)
Dialect(old): delimiter=, quote=" escape=none
Dialect(new): delimiter=, quote=" escape=none
Ranking: abs(delta) (unscaled)
Settings: threshold=95.0% tolerance=1e-9
Max abs delta: 7e-10 (<= tolerance 1e-9).
No numeric deltas above tolerance in common numeric columns.
```

**How to read this:**
- **Max abs delta** — the largest absolute difference observed across all cells (before tolerance zeroing). Proves nothing slipped through.
- This is a deterministic guarantee: every common numeric cell was checked.

### 3. REFUSAL

Printed when rvl cannot produce a deterministic verdict. Always includes a concrete next step.

```
RVL ERROR (E_KEY_DUP)

Compared: old.csv -> new.csv
Alignment: key=id
Dialect(old): delimiter=, quote=" escape=none
Dialect(new): delimiter=, quote=" escape=none
Settings: threshold=95.0% tolerance=1e-9

Cannot align rows: key "id" is not unique in old.csv (first duplicate: "A123" at data record 184).
Next: choose a unique key column or dedupe the data, then rerun.
```

**How to read this:**
- **Error code** — machine-stable identifier (e.g., `E_KEY_DUP`). See [Refusal Codes](#refusal-codes).
- **Example** — first concrete instance of the problem (file, record number, value).
- **Next** — a concrete rerun command or remediation step. Refusals are operator handoffs, never dead ends.

---

## Key Concepts

### Alignment

**Row-order mode** (no `--key`): rows align by position. Requires identical non-blank row counts. If rvl detects that rows are shuffled (via key discovery), it refuses with `E_NEED_KEY` and suggests a `--key` to use.

**Key mode** (`--key <column>`): rows align by matching key values. Key values are ASCII-trimmed, must be non-empty and unique within each file, and must match exactly between files. Any violation produces a specific refusal (`E_NO_KEY`, `E_KEY_EMPTY`, `E_KEY_DUP`, `E_KEY_MISMATCH`).

### Numeric Columns

Only columns present in **both** files are compared. Only numeric columns are diffed. A column is numeric if every aligned row is either missing on both sides or parseable finite numbers on both sides.

**Supported numeric formats:**
- Plain: `123`, `-123.45`, `1e6`, `-1.2E-3`
- Thousands separators: `1,234`, `-1,234,567.89` (US-style, 3-digit groups)
- Currency prefix: `$123.45`, `-$1,234.56`, `$-100`
- Accounting parentheses: `(123.45)` → parsed as `-123.45`
- Leading `+` is allowed: `+123`, `+$1,234.56`

**Missing tokens** (case-insensitive): empty string, `-`, `NA`, `N/A`, `NULL`, `NAN`, `NONE`.

**Refusal triggers:**
- Mixed numeric and non-numeric values in the same column → `E_MIXED_TYPES`
- Numeric on one side, missing on the other → `E_MISSINGNESS`
- No numeric columns in common after filtering → `E_NO_NUMERIC`

### Tolerance

Absolute noise floor applied per-cell. If `abs(new - old) <= tolerance`, the delta is treated as zero (no contribution). Default: `1e-9`. There is no relative/percentage tolerance in v0.

`max_abs_delta` in the output tracks the largest raw delta observed (before zeroing) for transparency.

### Threshold and Coverage

- **Total change** = sum of all `abs(delta)` values above tolerance (L1 distance across all common numeric cells).
- **Contribution** = `abs(delta)` for a single cell (after tolerance).
- **Coverage** = sum of top contributor contributions / total change.
- **Threshold** (default `0.95`) = minimum coverage required for a REAL CHANGE verdict.
- **MAX_CONTRIBUTORS** = 25 (hard cap, not configurable in v0).

If the top 25 contributors can't reach the threshold, rvl refuses with `E_DIFFUSE` rather than printing an incomplete explanation. Lower the threshold explicitly if needed: `--threshold 0.80`.

### Contributor Ranking

Contributors are ranked by `abs(delta)` descending (unscaled — large-magnitude columns dominate by design). Ties are broken by row ID ascending, then column name ascending (byte order). rvl prints only the smallest prefix of contributors whose cumulative coverage reaches the threshold.

---

## Delimiter

### Auto-Detection (default)

Each file's delimiter is detected independently by sampling the header plus up to 200 data records (or ~64KB). Candidate delimiters are tried in order: `,` → `\t` → `;` → `|` → `^`. The candidate with the best score (most records parsed, most consistent field count, most fields) wins.

If multiple candidates tie and produce different parsed output, rvl refuses with `E_DIALECT`. If they produce identical output, the tie breaks by candidate order (comma first).

If auto-detection yields only 1 column, rvl refuses with `E_DIALECT` (the file may use an unsupported delimiter).

### `sep=` Directive

If the first non-blank line of a file is `sep=<char>` (e.g., `sep=;`), rvl uses that delimiter for the file (unless `--delimiter` overrides it). The `sep=` line is skipped during parsing.

### `--delimiter` (forced)

Overrides both auto-detection and `sep=` directives for **both** files. Accepted values:

| Format | Examples |
|--------|----------|
| Named | `comma`, `tab`, `semicolon`, `pipe`, `caret` (case-insensitive) |
| Hex | `0x09` (tab), `0x1f` (unit separator), `0x2c` (comma) |
| Single ASCII char | `,`, `\|`, `;` |

Valid range: ASCII `0x01`–`0x7F`, excluding `"` (`0x22`), `\r` (`0x0D`), `\n` (`0x0A`). Invalid values are CLI argument errors (exit 2). Use `tab` or `0x09`, not `\t` (no escape sequences).

---

## Refusal Codes

Every refusal includes the error code, first concrete example, and a `Next:` remediation step.

| Code | Meaning | Next Step |
|------|---------|-----------|
| `E_IO` | File read error | Check file path and permissions |
| `E_ENCODING` | Unsupported encoding (UTF-16/32 BOM or NUL bytes) | Convert/re-export as UTF-8 |
| `E_CSV_PARSE` | CSV parse failure (invalid quoting/escaping) | Re-export as standard RFC4180 CSV |
| `E_HEADERS` | Missing header, duplicate headers, or rows wider than header | Fix headers or re-export |
| `E_DIALECT` | Delimiter ambiguous or undetectable | Use `--delimiter <delim>` or add `sep=<char>` to file |
| `E_NO_KEY` | `--key` column not found in one or both files | Use a column name that exists in both files |
| `E_KEY_EMPTY` | Empty key value in a non-blank row | Choose a key column with no empty values, or fill missing keys |
| `E_KEY_DUP` | Duplicate key values within a file | Choose a unique key column or dedupe the data |
| `E_KEY_MISMATCH` | Key sets differ between files (missing/extra keys) | Export comparable scopes or fix the join key |
| `E_ROWCOUNT` | Row count mismatch (row-order mode) | Use `--key <column>` for a missing/extra-keys report |
| `E_NEED_KEY` | Detected row reorder without `--key` | Use `--key <suggested>` (rvl prints candidates) |
| `E_MIXED_TYPES` | Column has both numeric and non-numeric values | Normalize column values to numeric or exclude the column |
| `E_NO_NUMERIC` | No numeric columns in common | Ensure both files share at least one numeric column |
| `E_MISSINGNESS` | Numeric value vs. missing token in aligned cell | Fill missing values or exclude the column |
| `E_DIFFUSE` | Top 25 contributors can't reach threshold | Use `--threshold 0.80` (or lower) to accept less coverage |

---

## JSON Output (`--json`)

A single JSON object on stdout. If the process fails before domain evaluation (e.g., invalid CLI args), JSON may not be emitted.

```jsonc
{
  "version": "rvl.v0",
  "outcome": "REAL_CHANGE",            // "REAL_CHANGE" | "NO_REAL_CHANGE" | "REFUSAL"
  "files": {
    "old": "old.csv",
    "new": "new.csv"
  },
  "alignment": {
    "mode": "key",                      // "key" | "row_order"
    "key_column": "u8:id"              // encoded identifier, or null
  },
  "dialect": {
    "old": { "delimiter": ",", "quote": "\"", "escape": null },
    "new": { "delimiter": ",", "quote": "\"", "escape": null }
  },
  "threshold": 0.95,
  "tolerance": 1e-9,
  "counts": {
    "rows_old": 4183,
    "rows_new": 4183,
    "rows_aligned": 4183,
    "columns_old": 17,
    "columns_new": 16,
    "columns_common": 15,
    "columns_old_only": 2,
    "columns_new_only": 1,
    "numeric_columns": 12,
    "numeric_cells_checked": 50196,
    "numeric_cells_changed": 3
  },
  "metrics": {
    "total_change": 1842100.3713,       // L1 distance (sum of abs deltas above tolerance)
    "max_abs_delta": 1842100.0,         // largest abs(delta) observed (pre-zeroing)
    "top_k_coverage": 0.952             // coverage of top MAX_CONTRIBUTORS
  },
  "limits": {
    "max_contributors": 25
  },
  "contributors": [                     // empty unless REAL_CHANGE
    {
      "row_id": "u8:NVDA",
      "column": "u8:market_value",
      "old": 123.0,
      "new": 1842223.0,
      "delta": 1842100.0,
      "contribution": 1842100.0,
      "share": 0.9998,                  // contribution / total_change
      "cumulative_share": 0.9998
    }
    // ... more contributors, ranked by contribution desc
  ],
  "refusal": null                       // null unless REFUSAL
  // When REFUSAL:
  // "refusal": {
  //   "code": "E_KEY_DUP",
  //   "message": "duplicate key values",
  //   "detail": { "file": "old.csv", "key_samples": ["A123"], ... }
  // }
}
```

### Identifier Encoding (JSON)

Row IDs and column names in JSON use unambiguous encoding:
- `u8:<string>` — valid UTF-8 with no ASCII control bytes (e.g., `u8:NVDA`, `u8:market_value`)
- `hex:<hex-bytes>` — anything else (e.g., `hex:ff00ab`)

Copy the encoded identifier directly into `--key` to avoid ambiguity.

### Nullable Fields

On REFUSAL, `counts` and `metrics` fields may be `null` if they couldn't be computed (e.g., `rows_aligned` is `null` for `E_ROWCOUNT`; all `metrics` are `null` for `E_NEED_KEY`).

---

## Scripting Examples

Check if files changed (exit code only):

```bash
rvl old.csv new.csv > /dev/null 2>&1
echo $?  # 0 = no change, 1 = changed, 2 = refused
```

Extract top contributor from JSON:

```bash
rvl old.csv new.csv --json | jq '.contributors[0]'
```

Get total change magnitude:

```bash
rvl old.csv new.csv --json | jq '.metrics.total_change'
```

Handle refusals programmatically:

```bash
rvl old.csv new.csv --json | jq 'select(.outcome == "REFUSAL") | .refusal'
```

Force a tab-delimited comparison with relaxed threshold:

```bash
rvl old.tsv new.tsv --delimiter tab --key account_id --threshold 0.80
```

---

## Spec

The full specification is `docs/PLAN_RVL.md`. This README covers everything needed to use the tool; the spec adds implementation details, edge-case definitions, and testing requirements.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
