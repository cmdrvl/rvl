#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    match rvl::run() {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("rvl: {e}");
            ExitCode::from(2)
        }
    }
}
