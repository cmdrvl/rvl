use rvl::cli::exit::{Outcome, OutputMode, OutputStream, exit_code, output_stream};

#[test]
fn exit_codes_match_spec() {
    assert_eq!(exit_code(Outcome::NoRealChange), 0);
    assert_eq!(exit_code(Outcome::RealChange), 1);
    assert_eq!(exit_code(Outcome::Refusal), 2);
}

#[test]
fn json_mode_routes_all_to_stdout() {
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
