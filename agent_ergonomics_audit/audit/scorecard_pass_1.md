# rvl Agent Ergonomics Scorecard - Pass 1

## Summary

- Mode: full
- Surfaces inventoried: 64
- Recommendations applied: 4 / 4
- Intent corpus: 130 entries, 0 silent failures, 0 useless errors
- Version prepared: 0.7.0

## Scores

| Dimension | Before | After | Evidence |
|---|---:|---:|---|
| Self-documentation | 650 | 870 | `rvl capabilities --json`, `rvl robot-docs guide`, `rvl --describe` |
| Output parseability | 780 | 900 | single-object JSON for triage and capabilities |
| Error pedagogy | 520 | 820 | `rvl doctor --fix` names exact alternatives |
| Intent inference | 590 | 820 | top-level first-try commands no longer fall through to CSV comparison |
| Installability | 720 | 850 | release workflow formula generation fails on missing checksums |
| Regression resistance | 680 | 840 | Rust tests plus audit regression scripts |

## Residual Risk

The core comparison path was intentionally left unchanged. Future passes should consider generalized typo recovery for common flag misspellings if that becomes a recurring support issue.
