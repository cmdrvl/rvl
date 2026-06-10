#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TOOL="${TOOL:-$ROOT/target/debug/rvl}"

stdout="$("$TOOL" robot-docs guide)"

grep -Fq "rvl --robot-triage" <<<"$stdout"
grep -Fq "rvl capabilities --json" <<<"$stdout"
grep -Fq "rvl robot-docs guide" <<<"$stdout"
grep -Fq "rvl doctor --fix is unavailable" <<<"$stdout"
