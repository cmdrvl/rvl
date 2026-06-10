#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TOOL="${TOOL:-$ROOT/target/debug/rvl}"

stdout="$("$TOOL" capabilities --json)"

jq -e '
  .schema_version == "rvl.doctor.capabilities.v1" and
  .read_only == true and
  .fix_mode.available == false and
  .agent_surfaces.capabilities.command == "rvl capabilities --json" and
  .side_effects["rvl capabilities --json"].writes_witness_ledger == false and
  .side_effects["rvl capabilities --json"].uses_network == false
' >/dev/null <<<"$stdout"
