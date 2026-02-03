//! Exit codes & stdout/stderr routing (bd-1b6).

/// Domain outcome produced by the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    NoRealChange,
    RealChange,
    Refusal,
}

/// Output mode chosen by the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Human,
    Json,
}

/// Target stream for output emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Exit code for a given outcome (domain-level only).
pub fn exit_code(outcome: Outcome) -> u8 {
    match outcome {
        Outcome::NoRealChange => 0,
        Outcome::RealChange => 1,
        Outcome::Refusal => 2,
    }
}

/// Output stream for a given outcome and output mode.
///
/// In JSON mode, all domain outcomes go to stdout.
/// In human mode, refusals go to stderr.
pub fn output_stream(outcome: Outcome, mode: OutputMode) -> OutputStream {
    match (mode, outcome) {
        (OutputMode::Json, _) => OutputStream::Stdout,
        (OutputMode::Human, Outcome::Refusal) => OutputStream::Stderr,
        (OutputMode::Human, _) => OutputStream::Stdout,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_match_spec() {
        assert_eq!(exit_code(Outcome::NoRealChange), 0);
        assert_eq!(exit_code(Outcome::RealChange), 1);
        assert_eq!(exit_code(Outcome::Refusal), 2);
    }

    #[test]
    fn json_mode_always_stdout() {
        assert_eq!(
            output_stream(Outcome::NoRealChange, OutputMode::Json),
            OutputStream::Stdout
        );
        assert_eq!(
            output_stream(Outcome::RealChange, OutputMode::Json),
            OutputStream::Stdout
        );
        assert_eq!(
            output_stream(Outcome::Refusal, OutputMode::Json),
            OutputStream::Stdout
        );
    }

    #[test]
    fn human_mode_refusals_to_stderr() {
        assert_eq!(
            output_stream(Outcome::NoRealChange, OutputMode::Human),
            OutputStream::Stdout
        );
        assert_eq!(
            output_stream(Outcome::RealChange, OutputMode::Human),
            OutputStream::Stdout
        );
        assert_eq!(
            output_stream(Outcome::Refusal, OutputMode::Human),
            OutputStream::Stderr
        );
    }
}
