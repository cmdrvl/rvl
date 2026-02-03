# rvl — Reveal What’s Real

## One-line promise
**Reveal the smallest set of numeric changes that explain what actually changed.**

If nothing meaningful changed, say so clearly.

Second promise: **Stop reconciling. Paste the verdict.**

---

## Problem (clearly understood)
Finance teams constantly compare:
- yesterday vs today
- vendor A vs vendor B
- pre-close vs post-close
- report v1 vs report v2

Today this means:
- Excel hell
- brittle scripts
- eyeballing numbers
- low certainty saying “nothing changed”

`rvl` replaces that with **one trusted command**.

---

## Non-goals (explicit)
`rvl` is NOT:
- analytics
- attribution
- validation
- semantics
- finance-specific logic
- dashboards

It does not explain *why the market moved*.  
It explains *why the numbers changed*.

---

## CLI (v0)
```bash
rvl <old.csv> <new.csv> [--key <column>] [--threshold <float>] [--tolerance <float>] [--delimiter <delim>] [--json]
```

If alignment or a deterministic verdict is impossible: **refuse loudly** with a single reason + the first actionable detail.

Flags (keep minimal)
- `--key <column>`: align rows by key value (otherwise align by row order)
- `--threshold <float>`: coverage target (`0 < x <= 1.0`, default `0.95`)
- `--tolerance <float>`: per-cell noise floor (`x >= 0`, default `1e-9`)
  - invalid numeric values for `--threshold` / `--tolerance` are CLI argument errors (exit 2)
- `--delimiter <delim>`: force CSV delimiter for both files (otherwise auto-detect per file)
  - accepted values: `comma`, `tab`, `semicolon`, `pipe`, `caret`, `0xNN` (case-insensitive hex; two-digit ASCII byte in `0x01-0x7F`, excluding line endings and `"`), or a single ASCII character in `0x01-0x7F` (except `"`, `\r`, `\n`)
  - note: the value is interpreted literally (no escape sequences); use `tab` / `0x09`, not `\t`
  - invalid values are treated as CLI argument errors (exit 2)
- `--json`: machine output (stable schema; no human formatting)

Exit codes (diff-like)
- `0`: NO REAL CHANGE
- `1`: REAL CHANGE
- `2`: REFUSAL / error

Streams
- Human mode: REAL CHANGE / NO REAL CHANGE go to stdout; REFUSAL goes to stderr.
- `--json` mode: emit exactly one JSON object on stdout for all domain outcomes (REAL CHANGE / NO REAL CHANGE / REFUSAL); stderr is reserved for process-level failures only (e.g., CLI parse errors, panics).

---

## Outcomes (exactly one)
1) REAL CHANGE
- minimal ranked list of numeric contributors
- cumulative coverage percentage (toward `--threshold`)
- explicit statement that everything else in common numeric columns is below tolerance or in the tail

2) NO REAL CHANGE
- explicit confirmation
- counts checked
- deterministic statement (tolerance + max abs delta) for common numeric columns

3) REFUSAL
- reason (ambiguous alignment, no numeric overlap, diffuse change, etc.)

No other outcomes.

Schema differences (old_only/new_only columns) are reported when both headers are parsed, but v0 outcomes are based only on common numeric columns.

---

## Definitions (v0)
- **Row ID**
  - with `--key`: the ASCII-trimmed key value
  - without `--key`: the 1-based data record index (header excluded; blank records skipped)
- **Data record number (errors)**: the 1-based data record index within its source file (header excluded; blank records skipped)
- **Blank record**: a data record (never the header record) where every field is empty after ASCII-trim (empty string only; missing tokens like `NA`/`NULL` are not blank records; ignored before alignment, counting, and key validation)
- **ASCII-trim**: trim ASCII spaces and tabs (`0x20`, `0x09`) from both ends
- **Cell label (display only)**: `<row_id>.<column>` (do not parse; use `--json` for machine use)
- **Delta**: `new - old`
- **Contribution**: `abs(delta)` (after tolerance is applied)
- **Total change**: L1 distance = `sum(contribution)` across numeric cells (unscaled; after tolerance)
- **Coverage**: `sum(contribution of reported cells) / total_change` (share of L1 distance)

v0 is unscaled: contributions are compared directly across columns. This favors large-magnitude columns and is intentional.

Coverage is defined only when `total_change > 0`.

---

## Input Contract (CSV Only)
File rules (strict, boring, reliable)
- Byte-oriented CSV (no encoding assumption; UTF-8 BOM allowed and stripped before checking for `sep=` / parsing the header)
- header required (the first record after optional `sep=`; leading ASCII spaces/tabs-only lines are ignored)
- optional Excel-style delimiter directive is allowed as the first non-blank line: `sep=<char>` (single ASCII byte delimiter in `0x01-0x7F`, not `"`/`\r`/`\n`; line is skipped; delimiter is forced unless `--delimiter` is provided)
  - recognized only if the first non-blank line (after stripping a single trailing `\r` if present) is exactly `sep=<single ASCII byte>` (no surrounding quotes, no whitespace) AND `<char>` is a valid delimiter (otherwise treat it as a normal header line)
  - "non-blank line" means, after stripping a single trailing `\r` and ASCII spaces/tabs, the line has at least one byte
- Delimiter is determined per file (auto-detect or `sep=`). If `--delimiter` is provided, it forces the delimiter for both files. The chosen delimiter(s) are printed in output.
- Delimiter must be a single ASCII byte (`0x01-0x7F`) and must not be `"` or a line-ending (`\r` / `\n`).
  - Invalid `--delimiter` values are CLI argument errors (exit 2; JSON may not be emitted).
- Quoting defaults to RFC4180 double-quotes (`"` + `""` escaping). A backslash-escape fallback is allowed only when RFC4180 parsing hard-fails.
- header names are ASCII-trimmed; empty header names are normalized to `__rvl_col_<1-based index>` (normalized names are used in output)
- column matching uses normalized header names
- header matching is byte-for-byte after normalization (case-sensitive; no Unicode normalization)
- column names must be unique within each file after normalization (duplicate headers => REFUSAL (`E_HEADERS`))
- empty file (no header record after optional `sep=` and leading ASCII spaces/tabs-only line skipping) => REFUSAL (`E_HEADERS`)

Value rules
- Numeric parsing ASCII-trims the cell.
- Missing tokens (after ASCII-trim; ASCII case-insensitive for letter tokens): empty string, `-`, `NA`, `N/A`, `NULL`, `NAN`, `NONE`
- Missing-token matching runs before numeric parsing.
- Numeric values must parse as **finite** `f64` (no `NaN`, no +/-inf).
- Precision note: `f64` is exact for integers up to 2^53 (~9e15). Above that, unit precision can be lost; set `--tolerance` accordingly or pre-scale to integers.

Accepted numeric formats (v0) (finance-friendly, still deterministic)
- Plain: `123`, `-123`, `123.45`, `-123.45`, `1e6`, `-1.2E-3`
- Thousands separators (US): `1,234`, `-1,234`, `+1,234`, `1,234,567.89`, `-1,234,567.89`
  - commas must be in 3-digit groups (reject `12,34`)
- Currency prefix: `$123.45`, `$1,234.56`, `-$1,234.56`, `$-1,234.56`
- Accounting parentheses: `(123.45)`, `(1,234.56)`, `($1,234.56)` => parse inner numeric value, then force negative sign (`value = -abs(inner)`)
- Leading `+` is allowed (same as no sign): `+123`, `+1e6`, `+$1,234.56`, `$+1,234.56`

Not supported (v0): `%` suffix, currency codes, locale decimals (e.g., `1.234,56`), any non-ASCII symbols.

Key rules (`--key`)
- Key column must exist in both files (else REFUSAL (`E_NO_KEY`)).
- Key values are compared after ASCII-trim; empty key => REFUSAL (`E_KEY_EMPTY`).
- Key values are not interpreted as missing tokens.
- Blank records (all fields empty after ASCII-trim) are ignored before key validation (empty keys in blank records do not trigger refusal).
- Keys must be unique within each file (duplicates => REFUSAL (`E_KEY_DUP`)).
- Key sets must match exactly (missing/extra keys => REFUSAL (`E_KEY_MISMATCH`)).
- `--key` refers to normalized header names (ASCII-trim + empty header => `__rvl_col_<n>`).
- `--key` accepts an encoded normalized header identifier:
  - plain UTF-8 string (treated as `u8:<...>`)
  - `u8:<utf8-string>` (explicit UTF-8)
  - `hex:<hex-bytes>` (raw bytes; hex is case-insensitive; output uses lowercase)
- If a printed header starts with `u8:` or `hex:`, copy/paste it into `--key` to avoid ambiguity.

---

## Column Selection (Numeric Intersection Only)
- Eligible columns are those present in **both** files after header normalization (excluding `--key` column).
- Columns present only in one file are never compared in v0; they must be reported (counts, plus an optional deterministic sample list of up to 10 column names).
- A column is treated as numeric if every aligned row is either:
  - missing on both sides, or
  - parseable finite numbers on both sides
- A numeric column must contain at least one numeric value pair somewhere (a column that is missing/missing for every row is treated as non-numeric and ignored).
- If a column contains any non-missing, non-numeric token:
  - if the column also contains numeric values anywhere => **REFUSAL** (`E_MIXED_TYPES`)
  - otherwise the column is non-numeric and ignored
- If a cell is missing on one side and numeric on the other: **REFUSAL** (`E_MISSINGNESS`)
  - This is a meaningful change, but v0 refuses rather than invent semantics (missing != 0).
- If, after filtering, there are no numeric columns: **REFUSAL** (`E_NO_NUMERIC`)

---

## Alignment
Without `--key` (fast path)
- Requires a header in both files.
- Requires identical row count (non-blank data records). If not: REFUSAL (`E_ROWCOUNT`).
- Align rows by position (row 1 with row 1, etc.).
- Columns are matched by header name intersection; extra columns are ignored for computation (but reported).
- Assumption: row order is stable/aligned by the producer. If rows can reorder, use `--key` (no-key mode treats row order as truth).
- Contributor IDs look like: `4183.market_value`

With `--key`
- Align rows by key value.
- Column order may differ; numeric columns are matched by header name.
- Contributor IDs look like: `<key_value>.market_value` (e.g., `NVDA.market_value`)

Alignment determinism (no-key mode)
- If rvl can deterministically detect a reorder under a discovered perfect key candidate (same keys, different row-order sequence) and `total_change > 0` under row-order alignment, it must REFUSE with `E_NEED_KEY` and print the suggested `--key`.
- rvl must never claim "REAL CHANGE" or emit `E_DIFFUSE` on row-order alignment when it can deterministically detect such a reorder (`E_NEED_KEY`).
- Not emitting `E_NEED_KEY` does not assert rows are aligned; if rows can reorder, use `--key`.

Key discovery (advisory; for refusals and shuffle detection)
- rvl attempts key discovery only to suggest a `--key` rerun and to detect reorders deterministically (`E_NEED_KEY`). It must never auto-select a key.
- Key discovery uses the same key normalization as `--key` mode (ASCII-trim; no missing-token interpretation).
- Joinable key candidate (for suggestions; must all hold):
  - column exists in both files (after header normalization)
  - every non-blank data record has a non-empty (post-ASCII-trim) key value
  - values are unique within each file
- Perfect key candidate (for deterministic alignment / shuffle detection): a joinable candidate where the key sets match exactly between files.
- Joinable candidates are still useful when key sets differ: rerun with `--key` will REFUSE with `E_KEY_MISMATCH` and show missing/extra keys.
- If one or more joinable candidates exist, print up to 3 candidates and a concrete rerun command (perfect candidates first, then header order).
- Suggested rerun commands must use the printed identifier (including any `u8:` / `hex:` prefix) so they are copy/paste-safe.

---

## CSV Parser Strategy (v0)
Goal: parse the CSV people actually have, not the CSV we wish they had.

Hard rules
- Never silently reinterpret data. If parsing is ambiguous, refuse and tell the operator exactly how to disambiguate.
- Prefer compatibility over strict RFC purity, but keep behavior deterministic and printed (delimiter + escape mode are part of the receipt).

Delimiter auto-detection (default)
- Auto-detection runs independently for the old and new files (unless `--delimiter` forces both).
- Delimiter candidates (in order): `,`, `\t`, `;`, `|`, `^`
- Auto-detect only considers these candidates; for any other delimiter, use `--delimiter` or `sep=<char>`.
- If the first non-blank line matches `sep=<char>` (after stripping a single trailing `\r` if present) and `<char>` is a valid delimiter, skip it; if `--delimiter` is not set, treat it as authoritative.
- Precedence: `--delimiter` > `sep=` directive > auto-detect (a valid `sep=` line is skipped when present).
- Sample: parse the header record plus up to 200 data records (blank records are skipped), stopping once ~64KB of input has been consumed (whichever comes first).
- If there is no header record after leading ASCII spaces/tabs-only line skipping (and optional `sep=`), REFUSE with `E_HEADERS`.
- For each candidate delimiter, score the sample under RFC4180 quoting (`escape=none`). If parsing fails before the sample limit, also score the same delimiter with backslash escape enabled. For that delimiter, keep the better score (tie-break: prefer RFC4180). Record:
  - `records_parsed` (until sample limit or first parse error)
  - a field-count histogram across parsed records (including the header); for scoring, a record counts as `header_fields` if it has fewer fields than the header (trailing padding), or if it has extra trailing fields that are empty after ASCII-trim
    - let `mode_count` be the max frequency and `mode_fields` its field count (ties pick larger `mode_fields`)
- Choose the delimiter with the best score tuple `(records_parsed, mode_count, mode_fields)` (lexicographic).
- For scoring only, each delimiter candidate uses its best parse of the sample (RFC4180 first; backslash-escape only if RFC4180 fails before the sample limit). Final parsing still follows the quote/escape strategy and prints the actual escape mode used.
- Candidates that cannot parse the header record (`records_parsed == 0`) are disqualified; if all candidates are disqualified: REFUSE with `E_CSV_PARSE`.
- If multiple delimiters tie, compare their parsed sample outputs (using each delimiter's best-scoring parse variant) after record-width normalization (pad short rows to the header width; drop extra trailing empty fields). If every sampled record is byte-for-byte identical across the tied delimiters, break ties by candidate order (comma > tab > semicolon > pipe > caret).
- Guard (avoid silent mis-detection): if the selected delimiter (after tie-breaking) yields `header_fields == 1` and delimiter source is auto-detect (no `sep=` and no `--delimiter`), REFUSE with `E_DIALECT` (the file may be single-column or may use an unsupported delimiter) and print `Next: rvl old.csv new.csv --delimiter <...>` (or add `sep=<char>` and rerun).
- Otherwise: REFUSE with `E_DIALECT` and print:
  - which file is ambiguous (old or new)
  - the tied delimiters
  - `Next:` either `--delimiter <...>` (only if both files truly use the same delimiter) or a `sep=<char>` directive / re-export for the ambiguous file

Quote/escape strategy (default)
- Quote character is always `"` (RFC4180). Default is double-quote escaping (`""` inside quoted fields).
- Only if RFC4180 parsing hard-fails, retry with backslash escape enabled (common in exports).
- If the backslash-escape fallback is used, it must be printed in the receipt (e.g., `Dialect(old/new): ... escape=\\`).
- If both attempts fail: REFUSE with `E_CSV_PARSE` and include the first failing record/line number (when available) + a concrete remediation (re-export as standard CSV).

Blank lines / blank records
- Before the header: ignore leading ASCII spaces/tabs-only lines (per Input Contract).
- After the header: ignore blank data records (every field is empty after ASCII-trim). The header record is never skipped, even if all header fields are empty.

Encoding guardrails
- If a UTF-16/UTF-32 BOM is detected, REFUSE with `E_ENCODING` and tell the operator to re-export/convert to UTF-8.
- If a NUL byte is detected in the first 8KB, treat it as an encoding issue and REFUSE with `E_ENCODING`.

Variable record widths (header vs rows)
- If a row has fewer fields than the header, missing trailing fields are treated as empty string.
- If a row has more fields than the header:
  - if all extra trailing fields are empty after ASCII-trim => ignore them
  - otherwise REFUSE with `E_HEADERS` (unaddressable columns; first offending data record number must be shown)

Implementation baseline
- Use the Rust `csv` + `csv-core` engine (fast, streaming, mature), configured with:
  - flexible records (variable field counts) to avoid false "malformed CSV" for common exports
  - byte-level iteration (`ByteRecord`) to keep hot loop allocation-free
- Strip UTF-8 BOM on input before parsing.
- Do not require UTF-8; treat input as bytes.

Identifier rendering (human output)
- rvl aligns key values by ASCII-trimmed bytes (deterministic). Column names are matched by normalized header bytes.
- For display, `row_id` and column names must be rendered deterministically:
  - if valid UTF-8 and contains no ASCII control bytes and does not start with `u8:` or `hex:` => print as-is
  - if valid UTF-8 and contains no ASCII control bytes but starts with `u8:` or `hex:` => print `u8:<utf8-string>`
  - otherwise print `hex:<lowercase-hex-bytes>` (never lossy)

Identifier encoding (JSON)
- For `--json`, identifiers are unambiguous strings:
  - `u8:<utf8-string>` if valid UTF-8 and contains no ASCII control bytes
  - otherwise `hex:<lowercase-hex-bytes>`

ASCII control bytes are `0x00-0x1F` and `0x7F`.

Parser bakeoff (required before v0 is declared "done")
- Build a "CSV torture corpus" from real exports (Excel, Google Sheets, vendor dumps, SEC tables, etc.).
- Benchmark candidates on:
  - throughput (rows/sec) on 1M+ rows
  - peak memory
  - compatibility on the corpus (must parse without false errors)
- Candidates to evaluate:
  - Rust `csv` (baseline)
  - Rust SIMD CSV (if viable) as a drop-in reader
  - Arrow/Polars CSV readers (only if they can be embedded without turning rvl into a fat dependency)
  - If a non-Rust parser wins by >= 25% throughput on the corpus with equal compatibility (no new refusals) and acceptable memory (peak RSS <= 2x baseline), plan includes porting/binding below.

If the best parser is not Rust
- Gate: the license must allow redistribution and static linking (no GPL surprises).
- Phase 1 (fastest path): bind via FFI with static linking for macOS releases.
- Phase 2 (the "forever" path): port the winning parser core to Rust and upstream it as `rvl_csv` (so rvl stays single-binary, no native deps).

---

## Tolerance (Machine-Noise Floor)
Per-cell rule
- If `abs(new - old) <= tolerance`, treat the delta as 0 (no contribution).
  - Still track `max_abs_delta` on the raw delta (pre-zeroing) for receipts.

Default
- `tolerance = 1e-9`

Tolerance is absolute (not relative). There is no percentage/relative tolerance in v0.

This is the only "noise" rule. No smoothing. No sampling. No statistics.

---

## Contributor Selection + Diffuse-Change Refusal
The tool must stay explainable in ~15 seconds, so REAL CHANGE output is hard-capped.

Defaults (v0)
- `threshold = 0.95`
- `MAX_CONTRIBUTORS = 25` (hard cap; not a flag in v0)

Streaming strategy (blazing fast)
- Maintain `total_change` (sum of contributions across all aligned cells in common numeric columns).
- Maintain a min-heap of the top `MAX_CONTRIBUTORS` contributors by contribution.

Decision
- If `total_change == 0` => NO REAL CHANGE
- Else compute coverage from the top contributors:
  - If coverage < threshold => REFUSAL (`E_DIFFUSE`: diffuse change; no small explanation set)
  - Else REAL CHANGE and print the smallest prefix of sorted contributors whose cumulative coverage >= threshold

Row-order gate
- In row-order mode, if shuffle-detection detects a reorder under a perfect key candidate, emit `E_NEED_KEY` before emitting REAL CHANGE or `E_DIFFUSE`.

No models. No probabilistic scoring. Just arithmetic + a hard readability cap.

---

## Output (Human-First)
Header lines (REAL CHANGE / NO REAL CHANGE)
- Compared: `<old> -> <new>` (basenames)
- Alignment: `key=<col>` or `row-order (no key)`
- Columns: `common=<n> old_only=<n> new_only=<n>`
- Checked: `<rows> rows, <numeric_columns> numeric columns (<cells> cells)`
- Dialect(old): `delimiter=<visible ASCII char|TAB|0xNN> quote=<char> escape=<char|none>` (non-visible delimiters print as `0xNN`; backslash prints as `\\`)
- Dialect(new): `delimiter=<visible ASCII char|TAB|0xNN> quote=<char> escape=<char|none>` (non-visible delimiters print as `0xNN`; backslash prints as `\\`)
- Ranking: `abs(delta)` (unscaled)
- Settings: `threshold=<x%> tolerance=<y>` (threshold printed as percent)

Formatting rules (human output)
- Integers use `,` thousands separators (e.g., `50,196`).
- Floats use the shortest round-trippable decimal representation (no locale separators; no currency symbols).
- Deltas always include an explicit sign (`+` / `-`).
- Percentages (threshold, coverage) print with one decimal place (display only; JSON carries full-precision floats).

Column counts exclude `--key` (if provided).
`old_only` / `new_only` refer to columns present only in the old/new file (after header normalization).
Row counts exclude blank data records.

Refusal header (minimum)
- Compared: `<old> -> <new>` (basenames)
- Alignment: `key=<col>` or `row-order (no key)`
- Settings: `threshold=<x%> tolerance=<y>`

If dialect detection succeeded before refusing, also print `Dialect(old)` / `Dialect(new)` lines (same format) in the refusal header.

Real change example
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

No real change example
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

Refusal example
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

---

## Output (JSON: `--json`)
Single JSON object on stdout (no extra text).
If the process fails before domain evaluation (e.g., invalid CLI args), JSON may not be emitted.

Top-level shape (v0)
- `version`: `"rvl.v0"`
- `outcome`: `"REAL_CHANGE" | "NO_REAL_CHANGE" | "REFUSAL"`
- `files`: `{ "old": "<path>", "new": "<path>" }`
- `alignment`: `{ "mode": "key" | "row_order", "key_column": "<encoded normalized name>" | null }`
- `alignment.key_column` uses identifier encoding for JSON (`u8:<...>` or `hex:<...>`).
- `dialect`: `{ "old": { "delimiter": "<char>", "quote": "<char>", "escape": "<char>" | null } | null, "new": { "delimiter": "<char>", "quote": "<char>", "escape": "<char>" | null } | null }`
- `dialect.old.*` / `dialect.new.*` values are single-byte strings when present; examples: tab is `"\t"`, backslash escape is `"\\"`.
- Non-printable delimiters are encoded as a single character and may appear escaped (e.g., `0x1F` => `"\u001f"`).
- `threshold`: `<float>`
- `tolerance`: `<float>`
- `counts`: `{ rows_old, rows_new, rows_aligned, columns_old, columns_new, columns_common, columns_old_only, columns_new_only, numeric_columns, numeric_cells_checked, numeric_cells_changed }` (integers; fields may be null on REFUSAL if not computed)
- `rows_old/rows_new` count non-blank data records (after blank record skipping).
- In key mode, `rows_aligned` is the key count.
- In row-order mode, `rows_aligned` is `rows_old` (= `rows_new`) for REAL_CHANGE/NO_REAL_CHANGE; for `E_ROWCOUNT`, `rows_aligned` must be null.
- `columns_*` counts exclude the key column (if any).
- `columns_old_only` / `columns_new_only` refer to columns present only in the old/new file (after header normalization).
- `numeric_cells_checked = rows_aligned * numeric_columns` when both are known; otherwise null. `numeric_cells_changed` counts cells with `abs(delta) > tolerance` when computed.
- For `E_NEED_KEY`, `numeric_cells_checked` and `numeric_cells_changed` must be null (avoid reporting row-order diffs when a reorder is detected).
- `metrics`: `{ total_change, max_abs_delta, top_k_coverage }` (floats; fields may be null on REFUSAL if not computed)
  - `total_change` is sum of contributions after tolerance (L1 distance over common numeric cells)
  - `max_abs_delta` is maximum `abs(delta)` observed (pre-zeroing)
  - `top_k_coverage` is coverage of the top `MAX_CONTRIBUTORS` contributors (null when `total_change` is null or `0`)
  - For `E_NEED_KEY`, `metrics.*` must be null (avoid reporting row-order diffs when a reorder is detected).
- `limits`: `{ max_contributors }` (v0: `25`)
- `contributors`: `[]` (empty unless REAL CHANGE)
  - each: `{ row_id, column, old, new, delta, contribution, share, cumulative_share }`
  - `row_id` and `column` use identifier encoding for JSON (`u8:<...>` or `hex:<...>`).
  - `old/new/delta/contribution/share/cumulative_share` are JSON numbers (finite).
  - `share = contribution / total_change`; `cumulative_share` is the running sum of `share` in contributor order.
- `refusal`: `null` unless REFUSAL
  - `{ code, message, detail }`
  - `detail` is a code-specific object (e.g., `{ file, line, column, key_samples, tied_delimiters }`)
  - Any identifiers inside `detail` (e.g., `column`, `key_samples`) use the same JSON identifier encoding (`u8:` / `hex:`).

---

## Refusal Codes (v0)
Keep these coarse and stable:
- `E_IO`: file read error
- `E_ENCODING`: unsupported text encoding (convert/re-export as UTF-8)
- `E_CSV_PARSE`: CSV parse failure under supported quote/escape modes (invalid or unsupported quoting/escaping)
- `E_HEADERS`: missing header, duplicate headers, or unaddressable columns (rows wider than header)
- `E_NO_KEY`: `--key` column missing
- `E_KEY_EMPTY`: empty key value in a non-blank data record
- `E_KEY_DUP`: key not unique
- `E_KEY_MISMATCH`: key sets differ (missing/extra)
- `E_ROWCOUNT`: row count mismatch (no-key mode; non-blank data records)
- `E_NEED_KEY`: cannot deterministically align rows without a key (detected reorder under a discovered perfect key candidate; emitted when `total_change > 0` to prevent a misleading row-order verdict/refusal)
- `E_DIALECT`: delimiter cannot be unambiguously determined (ambiguous or undetectable)
- `E_MIXED_TYPES`: column contains both numeric and non-numeric tokens
- `E_NO_NUMERIC`: no numeric overlap after filtering
- `E_MISSINGNESS`: numeric value vs missing token (cannot compute)
- `E_DIFFUSE`: top `MAX_CONTRIBUTORS` cannot reach `threshold`

---

## Refusal Output Contract (v0)
Refusal must be an operator handoff, not a dead end.

This section defines human output (non-`--json`). In `--json` mode, the same information must be represented under `refusal`.

Every REFUSAL prints:
- Compared/Alignment/Settings header lines (and Dialect lines if known)
- one-line reason (with refusal code)
- first concrete example (file + data record number or key, plus column/value when applicable)
- `Next:` a concrete rerun command (or a concrete remediation)

Examples
- `E_ROWCOUNT`: `Next: rerun with --key <candidate> to get a missing/extra-keys report (or export comparable scopes)`
- `E_NEED_KEY`: `Next: rvl old.csv new.csv --key <candidate>`
- `E_KEY_EMPTY`: include the file + data record number; `Next: choose a key column with no empty values (or fill missing keys), then rerun`
- `E_DIALECT`: include which file is ambiguous / undetectable; `Next: rvl old.csv new.csv --delimiter <...>` (forces both; only if both files truly use the same delimiter) or add `sep=<char>` as the first non-blank line of the ambiguous file (no whitespace) and rerun
- `E_MIXED_TYPES`: `Next: normalize column values to numeric (or exclude the column) and rerun`
- `E_ENCODING`: `Next: convert/re-export both files as UTF-8 CSV and rerun`
- `E_CSV_PARSE`: `Next: re-export as standard CSV (RFC4180 quoting) and rerun`
- `E_DIFFUSE`: include `top_k_coverage=<x>` and `threshold=<y>`; `Next: rvl old.csv new.csv --threshold 0.80` (explicitly acknowledges lower coverage)
- `E_KEY_MISMATCH`: include `missing_in_new=<n>` and `extra_in_new=<n>` + a short sample of keys (up to 10, deterministic order); `Next: export comparable scopes or fix the join key, then rerun`

---

## Rust Implementation Sketch
Core crates
- `clap` for CLI
- `csv` for parsing (streaming)
- `serde` + `serde_json` for `--json`

Core data types
- `Contributor { row_id, column, old, new, delta, contribution }`
- Keep top-K contributors in a fixed-size heap; never store all cell deltas.

No-key mode (fastest)
1) Read both headers, normalize, compute common columns (and note old_only/new_only for reporting)
2) Stream lockstep once: skip blank records, enforce row-count match (EOF mismatch => `E_ROWCOUNT`), determine numeric columns, compute `total_change` + top-K heap
3) If `total_change > 0`, run a shuffle-detection pass via key discovery: if a perfect key candidate exists and its order differs, REFUSE (`E_NEED_KEY`) before printing a verdict

Key mode
- Load one side into a `HashMap<key, row_values>` for aligned lookup (v0).
- While joining, compute `total_change` + top-K heap.
- After join, verify no unmatched keys remain.

Determinism
- Stable ordering for display: contribution desc, then row_id asc (key mode: raw row_id bytes asc; row-order mode: numeric row index asc), then raw column bytes asc.
- Top-K selection uses the same total ordering to avoid tie-driven nondeterminism.
- Any printed sample lists (columns, keys) are sorted by raw bytes asc and truncated to a fixed count.

Target
- no-key mode: 1-10M rows; I/O bound on local SSD; main diff pass ~= 2x a raw read of both files; shuffle-detection (no-key mode, when `total_change > 0`) adds one additional full read of both files
- key mode: sized by RAM (v0 uses an in-memory HashMap join)
- avoid allocations in the hot loop where possible (reuse buffers)

---

## Testing Philosophy
Must-pass (v0)
- identical files => NO REAL CHANGE
- single large delta => one-cell REAL CHANGE
- deltas below tolerance => NO REAL CHANGE
- shuffled rows without `--key` (with a discoverable perfect key candidate) => REFUSAL (`E_NEED_KEY`) when row-order alignment has `total_change > 0` (would otherwise be REAL CHANGE or `E_DIFFUSE`)
- empty key value in a non-blank data record => REFUSAL (`E_KEY_EMPTY`)
- key not unique => REFUSAL (`E_KEY_DUP`)
- keys differ => REFUSAL (`E_KEY_MISMATCH`)
- no numeric columns overlap => REFUSAL (`E_NO_NUMERIC`)
- mixed numeric/non-numeric values in a common column => REFUSAL (`E_MIXED_TYPES`)
- many small deltas where top 25 < 95% => REFUSAL (`E_DIFFUSE`)
- numeric parsing: `$1,234.56`, `+$1,234.56`, `$+1,234.56`, `(1,234.56)`, `(-1,234.56)`, and `($-1,234.56)` parse and diff correctly
- delimiter: comma/tab/semicolon/pipe/caret-delimited CSVs parse (auto-detect and `--delimiter`); non-printable ASCII delimiters parse when forced via `--delimiter 0xNN`
- delimiter ambiguity: delimiter tie where parsed sample outputs differ (after record-width normalization) => REFUSAL (`E_DIALECT`) with a `Next:` rerun command
- delimiter tie where parsed sample outputs are byte-for-byte identical (after normalization) => no refusal; tie-break by candidate order (comma > tab > semicolon > pipe > caret)
- parser torture corpus: add at least 50 real-world CSV fixtures; every fixture must parse or refuse for a precise reason (no "malformed" handwaving)
- non-UTF8 bytes in headers/keys => parse succeeds; output uses `hex:` rendering (no lossy output)
- JSON identifier encoding: UTF-8 identifiers with no ASCII control bytes are `u8:<...>`; otherwise `hex:<...>` (no ambiguity)
- UTF-16/UTF-32 encoded CSV => REFUSAL (`E_ENCODING`)

Never allow
- silent guessing
- partial contributor lists (no "and N more"; if threshold can't be met within `MAX_CONTRIBUTORS`, refuse `E_DIFFUSE`)
- "other" buckets as a line item

---

## Success Criteria (Real World)
- users alias it
- people paste output into Slack
- someone deletes a spreadsheet because of it
- first reaction is: "oh thank god"

If any feature reduces that, cut it.

---

## Make It Loved By 100 People (Launch Plan)
This is the go-to-market. Without this, nobody loves it.

Wedge (v0)
- own one ritual: vendor-vs-vendor / pre-vs-post exports where teams currently reconcile in Excel
- the distribution unit is a pasted verdict in Slack (the output is the product)

Non-negotiables for love
- install in under 60 seconds on macOS (prebuilt binaries + Homebrew tap)
- refusals are operator handoffs (print the next command)
- output is screenshot-able and self-contained (counts + thresholds/tolerance + alignment + dialect; NO REAL CHANGE prints max abs delta)

Execution (2 weeks)
- recruit 15 design partners who reconcile weekly/daily; require they paste 3 outputs into Slack
- iterate only on: install friction, refusal clarity, output readability
- ship a release page with 3 examples and a one-line promise (no docs sprawl)

Success metric (v0)
- 100 people who voluntarily paste `rvl` output into Slack at least once/week

---

## v1 Ideas (Only If v0 Is Loved)
- parquet/JSON input (not for v0)
- directory diffs
- numeric diff over time windows

### Decision Notes: Parquet/JSON Input (bd-7t9)
Decision: **Defer**. Only pursue after v0 is loved and CSV remains the clear bottleneck.

Rationale:
- CSV already covers current workflows; adding new formats risks product clarity and increases maintenance surface.
- JSON/Parquet bring schema and type semantics that could conflict with v0’s strict, refusal-first rules.

If/when revisited, propose the following constraints:
- **Explicit opt-in**: do not auto-detect by extension. Add a `--format parquet|json|csv` flag.
- **Strict typing**: numeric columns must be explicit (Parquet numeric types, JSON numbers only). Mixed types refuse (`E_MIXED_TYPES`).
- **Missingness**: keep v0 rule (missing vs numeric => `E_MISSINGNESS`), no silent coercions.
- **Row identity**: key mode required for JSON arrays of objects unless a stable row order is guaranteed by source. Otherwise emit `E_NEED_KEY`.
- **Schema diffs**: treat column intersection the same as CSV, report old_only/new_only.

Spec deltas (if implemented):
- New CLI flag: `--format <csv|parquet|json>` with strict validation (exit 2 on invalid).
- JSON input accepted forms:
  - array of objects (preferred): keys are columns; values must be numbers or missing tokens.
  - array of arrays + explicit header row (otherwise refuse).
- Parquet input: only flat schemas (no nested structs/lists); refuse nested fields with `E_HEADERS`.
- Update refusal details to include `format` field for E_CSV_PARSE equivalents (new codes not required).

### Decision Notes: Directory Diffs (bd-242)
Decision: **Defer**. This feature breaks the “single verdict” contract unless we redesign output.

Rationale:
- Directory diffs imply *many* comparisons, which conflicts with v0’s single, copy‑paste‑able output.
- Pairing rules (by name? checksum? manifest?) are easy to get wrong and hard to explain succinctly.

If/when revisited, propose the following constraints:
- **Explicit manifest**: require a manifest file listing `old_path,new_path` pairs; refuse without it.
- **Deterministic ordering**: sort pairs by manifest order; no implicit filesystem ordering.
- **Single output**: either aggregate summary only (counts + worst offenders) or emit multiple outputs in a JSON array (breaks v0 contract; would require a new top-level schema version).
- **Refusals**: missing files map to `E_IO`; pairing mismatches would likely need a new refusal code (e.g., `E_PAIRING`) instead of overloading `E_ROWCOUNT`.

Spec deltas (if implemented):
- New CLI flag: `--pairs <manifest.csv>` or `--dir` plus `--pairing manifest`.
- JSON output would need a `results[]` array or version bump (e.g., `rvl.v1`).

### Decision Notes: Time Window Diffs (bd-1kq)
Decision: **Defer**. Time windows add aggregation semantics that conflict with v0’s minimal‑change ethos.

Rationale:
- Windowing implies aggregation (sum/avg/last), which changes the meaning of “smallest set of numeric changes.”
- It invites hidden semantics (time alignment, missing periods) that are better handled upstream.

If/when revisited, propose the following constraints:
- **Explicit window spec**: require `--window <period>` plus `--agg <sum|avg|last>`; refuse without both.
- **Explicit time column**: `--time <column>`; no auto-detect.
- **Deterministic bucketing**: fixed UTC boundaries; no locale or TZ inference.
- **Missing buckets**: refuse on missing buckets rather than zero-fill (`E_MISSINGNESS` or new code).

Spec deltas (if implemented):
- New CLI flags: `--time`, `--window`, `--agg`.
- Output must include bucket counts and the chosen aggregation in both human/JSON headers.

### Decision Notes: 60s Install + Distribution (bd-72f)
Decision: **Defer implementation**, but capture the release checklist now so v0 can ship quickly once loved.

Rationale:
- Packaging changes don’t affect core correctness, but they do affect adoption.
- Best handled once v0 output has stabilized to avoid rework on release artifacts.

Proposed release checklist (when ready):
- **Artifacts**: macOS universal binary (x86_64 + arm64) built from the same tag.
- **Versioning**: tag matches `Cargo.toml` version; changelog highlights breaking changes.
- **Homebrew**: tap formula with SHA256 for each artifact; install in <60s.
- **Codesign**: sign macOS binaries to reduce Gatekeeper friction (not required for dev builds).
- **Smoke tests**: run `rvl --help` and a tiny fixture on both architectures.

Spec deltas (if implemented):
- Add a `docs/release.md` checklist and a `scripts/release.sh` helper (opt‑in).
- Add CI workflow that builds artifacts and uploads release assets on tag.

Final rule: If you can’t explain the output to a tired ops person in 15 seconds, it doesn’t ship.
