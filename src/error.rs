//! Structured refusals and retained upstream causes.

use std::{backtrace::Backtrace, fmt};

/// An actionable failure, with a stable code and original cause.
#[derive(Debug)]
pub struct Error {
    code: &'static str,
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
    backtrace: Backtrace,
}

impl Error {
    /// Creates a refusal without discarding already committed work.
    pub fn refusal(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
            backtrace: Backtrace::capture(),
        }
    }

    /// Returns a stable refusal code for transports and recovery tools.
    pub fn code(&self) -> &'static str {
        self.code
    }

    fn caused(code: &'static str, cause: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            code,
            message: cause.to_string(),
            source: Some(Box::new(cause)),
            backtrace: Backtrace::capture(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)?;
        if self.backtrace.status() == std::backtrace::BacktraceStatus::Captured {
            write!(f, "\n{}", self.backtrace)?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| &**e as _)
    }
}

macro_rules! from_error {
    ($t:ty, $code:literal) => {
        impl From<$t> for Error {
            fn from(error: $t) -> Self {
                Self::caused($code, error)
            }
        }
    };
}
from_error!(std::io::Error, "io");
from_error!(serde_json::Error, "invalid_json");
from_error!(iceberg::Error, "iceberg");
from_error!(arrow::error::ArrowError, "arrow");
from_error!(parquet::errors::ParquetError, "parquet");
from_error!(datafusion::error::DataFusionError, "query");
from_error!(tokio::task::JoinError, "worker");

/// A result whose error retains a stable refusal code.
pub type Result<T> = std::result::Result<T, Error>;
