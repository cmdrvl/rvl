//! Process-level errors vs domain refusals (bd-lfp).
//!
//! Process errors are failures that occur before domain evaluation (CLI parsing,
//! I/O setup, panics). Domain refusals are E_* outcomes and should still emit
//! a JSON object in `--json` mode.

use std::error::Error;
use std::fmt;

use crate::refusal::codes::RefusalCode;

/// A domain refusal (E_*), distinct from process-level failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainRefusal {
    pub code: RefusalCode,
    pub message: String,
}

impl DomainRefusal {
    pub fn new(code: RefusalCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for DomainRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl Error for DomainRefusal {}

/// Process-level error (CLI parse errors, panics, internal failures).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessError {
    message: String,
}

impl ProcessError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ProcessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for ProcessError {}

/// Error that distinguishes domain refusals from process failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineError {
    Refusal(DomainRefusal),
    Process(ProcessError),
}

impl PipelineError {
    #[inline]
    pub fn is_refusal(&self) -> bool {
        matches!(self, PipelineError::Refusal(_))
    }

    #[inline]
    pub fn is_process(&self) -> bool {
        matches!(self, PipelineError::Process(_))
    }

    pub fn as_refusal(&self) -> Option<&DomainRefusal> {
        match self {
            PipelineError::Refusal(err) => Some(err),
            PipelineError::Process(_) => None,
        }
    }

    pub fn as_process(&self) -> Option<&ProcessError> {
        match self {
            PipelineError::Process(err) => Some(err),
            PipelineError::Refusal(_) => None,
        }
    }
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineError::Refusal(err) => write!(f, "{err}"),
            PipelineError::Process(err) => write!(f, "{err}"),
        }
    }
}

impl Error for PipelineError {}

impl From<DomainRefusal> for PipelineError {
    fn from(err: DomainRefusal) -> Self {
        PipelineError::Refusal(err)
    }
}

impl From<ProcessError> for PipelineError {
    fn from(err: ProcessError) -> Self {
        PipelineError::Process(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinguishes_refusal_from_process() {
        let refusal = PipelineError::Refusal(DomainRefusal::new(
            RefusalCode::Dialect,
            "delimiter ambiguous",
        ));
        let process = PipelineError::Process(ProcessError::new("cli error"));
        assert!(refusal.is_refusal());
        assert!(!refusal.is_process());
        assert!(process.is_process());
        assert!(!process.is_refusal());
    }

    #[test]
    fn exposes_refusal_and_process_accessors() {
        let refusal = PipelineError::Refusal(DomainRefusal::new(RefusalCode::Io, "disk"));
        let process = PipelineError::Process(ProcessError::new("panic"));
        assert_eq!(refusal.as_refusal().unwrap().message(), "disk");
        assert!(refusal.as_process().is_none());
        assert_eq!(process.as_process().unwrap().message(), "panic");
        assert!(process.as_refusal().is_none());
    }
}
