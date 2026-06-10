# Ambition Bar Check

This focused full pass applied four high-leverage changes to a small CLI that already had nested doctor surfaces:

- top-level mega-command: `rvl --robot-triage`
- top-level capability contract: `rvl capabilities --json`
- top-level agent guide: `rvl robot-docs guide`
- safe refusal for `rvl doctor --fix`
- release generator hardening for the Homebrew formula

The "That's it??" self-prompt was run before ending the pass. The remaining deferred class is generalized typo correction for arbitrary flags; this pass focused on making the first commands an agent naturally tries work directly and safely.
