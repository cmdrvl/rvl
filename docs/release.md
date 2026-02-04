# Release & Distribution Plan (60s install)

Goal: install rvl on macOS in under 60 seconds via prebuilt binaries and a Homebrew tap.

## Deliverables
- Signed macOS binaries (universal or per-arch) attached to GitHub Releases.
- Homebrew tap formula referencing release artifacts with SHA256.
- Minimal install instructions (copy/paste).

## Release Pipeline Outline
1. Tag version in `Cargo.toml` (semver).
2. Build release binaries on CI.
3. Attach artifacts to a GitHub Release.
4. Update Homebrew tap formula with new version + SHA256.
5. Validate install from scratch on a clean macOS machine.

## Release Checklist
1. Verify local quality gates:
   - `cargo fmt --check`
   - `cargo clippy --all-targets -- -D warnings`
   - `cargo test`
2. Update `Cargo.toml` version.
3. Commit with a release summary.
4. `git push origin main` and keep `master` synced: `git push origin main:master`.
5. CI builds release binaries for:
   - `x86_64-apple-darwin`
   - `aarch64-apple-darwin`
6. Create GitHub Release with artifacts and checksums.
7. Update Homebrew tap formula:
   - Source URL points to the release artifact.
   - SHA256 matches artifact.
   - Formula installs `rvl` binary into `bin`.
8. Smoke test:
   - `brew tap <org>/rvl`
   - `brew install rvl`
   - `rvl --help` and a small CSV diff.

## Homebrew Tap Notes
- Keep the formula minimal and deterministic.
- Avoid build-from-source during install (use prebuilt artifacts).
- Ensure `--version` matches the tag in `Cargo.toml`.

## Rollback Plan
- If a release is broken:
  - Yank the Homebrew formula version or revert the tap to the prior SHA.
  - Mark the GitHub Release as broken with a clear note.

## Launch Page Content (Draft)
Goal: a simple, direct landing page with 3 concrete examples and a clear design partner CTA.

### Headline + Subhead
- Headline: "Reveal the smallest set of numeric changes that explain what actually changed."
- Subhead: "Stop reconciling. Paste the verdict. rvl compares two CSVs and tells you what matters."

### Sections (Order)
1. Hero: one-sentence promise + primary CTA (Download) + secondary CTA (Become a design partner).
2. How it works: 3 bullets (align rows, compare numeric columns, rank contributors).
3. Examples: Real change, No real change, Refusal (with short captions).
4. Why it’s different: deterministic, strict refusals, zero guesswork.
5. Design partners: who we want, how to reach us, what you get.
6. FAQ: “Why refuse?”, “What CSVs work?”, “Is it safe to paste outputs?”

### Example Outputs (Draft)
Real change:
```text
RVL

REAL CHANGE

Compared: basic_old.csv -> basic_new.csv
Alignment: row-order (no key)
Columns: common=3 old_only=0 new_only=0
Checked: 2 rows, 1 numeric columns (2 cells)
Dialect(old): delimiter=, quote=" escape=none
Dialect(new): delimiter=, quote=" escape=none
Ranking: abs(delta) (unscaled)
Settings: threshold=95.0% tolerance=1e-9

1 cell explain 100.0% of total numeric change (threshold 95.0%):

1. 2.amount  +60  (200 -> 260)

Everything else in common numeric columns is <= tolerance or in the tail (not required to reach threshold).
```

No real change:
```text
RVL

NO REAL CHANGE

Compared: no_real_change_old.csv -> no_real_change_new.csv
Alignment: row-order (no key)
Columns: common=2 old_only=0 new_only=0
Checked: 2 rows, 2 numeric columns (4 cells)
Dialect(old): delimiter=, quote=" escape=none
Dialect(new): delimiter=, quote=" escape=none
Ranking: abs(delta) (unscaled)
Settings: threshold=95.0% tolerance=1e-9

Max abs delta: 0 (<= tolerance 1e-9).
No numeric deltas above tolerance in common numeric columns.
```

Refusal:
```text
RVL ERROR (E_NO_NUMERIC)

Compared: no_numeric_old.csv -> no_numeric_new.csv
Alignment: key=id
Dialect(old): delimiter=, quote=" escape=none
Dialect(new): delimiter=, quote=" escape=none
Settings: threshold=95.0% tolerance=1e-9

Reason (E_NO_NUMERIC): no numeric columns in common.
Example: no numeric columns in common.
Next: ensure common numeric columns exist (or adjust inputs) and rerun
```

## Design Partner Loop (Draft)
Target: 15 design partners, 100 weekly pasteouts.

### Outreach Checklist
- Identify 15 finance/ops teams that reconcile CSVs weekly.
- Prioritize teams with: multi-vendor feeds, close/pre-close diffs, or high-volume spreadsheets.
- Prepare a 2-minute demo using the three examples above.
- Ask for one real CSV diff per week (or one sanitized example).
- Schedule a 30-min feedback call after the first run.
- Capture: time saved, confidence gain, refusal reasons, desired features.

### Feedback Metrics
- Weekly pasteouts (target: 100).
- % of runs that refuse (target: track, not minimize).
- Median time-to-verdict vs baseline workflow.
- Design partner retention (weekly usage for 4+ weeks).
