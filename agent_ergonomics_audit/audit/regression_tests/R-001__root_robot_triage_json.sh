#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TOOL="${TOOL:-$ROOT/target/debug/rvl}"

stdout="$("$TOOL" --robot-triage)"

jq -e '
  .schema_version == "rvl.doctor.v1" and
  .read_only == true and
  .capabilities_url == "command:rvl capabilities --json" and
  .capabilities.agent_surfaces.robot_triage.command == "rvl --robot-triage"
' >/dev/null <<<"$stdout"
