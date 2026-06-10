#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TOOL="${TOOL:-$ROOT/target/debug/rvl}"

set +e
stderr="$("$TOOL" doctor --fix 2>&1 >/dev/null)"
status=$?
set -e

test "$status" -eq 2
test -z "$("$TOOL" doctor --fix 2>/dev/null || true)"
grep -Fq "rvl doctor --fix is unavailable" <<<"$stderr"
grep -Fq "rvl --robot-triage" <<<"$stderr"
grep -Fq "rvl capabilities --json" <<<"$stderr"
grep -Fq "rvl robot-docs guide" <<<"$stderr"
