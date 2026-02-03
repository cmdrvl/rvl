#![forbid(unsafe_code)]

pub mod alignment;
pub mod cli;
pub mod csv;
pub mod diff;
pub mod format;
pub mod normalize;
pub mod numeric;
pub mod orchestrator;
pub mod output;
pub mod refusal;

/// Run the rvl pipeline. Returns exit code (0, 1, or 2).
pub fn run() -> Result<u8, Box<dyn std::error::Error>> {
    // TODO: wire CLI parsing → pipeline → output
    Ok(0)
}
