# rvl Agent Ergonomics Playbook

Use these surfaces first when operating `rvl` as an agent:

1. `rvl --robot-triage` for one-call read-only health and next-step selection.
2. `rvl capabilities --json` for the full machine-readable command contract.
3. `rvl robot-docs guide` for paste-ready operating notes.
4. `rvl --json <old.csv> <new.csv>` for comparison output that is safe to pipe into `jq`.

Do not use `rvl doctor --fix` for repair. It intentionally exits 2 and points to the read-only alternatives.
