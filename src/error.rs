//! Structured refusals and retained upstream causes.

use std::{backtrace::Backtrace, fmt};

/// High-level categories for errors and transport status mappings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    BadRequest,
    GatewayTimeout,
    Gone,
    NotFound,
    Conflict,
    ServiceUnavailable,
    Internal,
    Unprocessable,
}

impl ErrorKind {
    /// Categorizes a stable refusal code into a typed kind.
    pub fn from_code(code: &str) -> Self {
        match code {
            "invalid_request_timeout"
            | "invalid_argument"
            | "invalid_parameter"
            | "invalid_spec" => Self::BadRequest,
            "request_timeout" => Self::GatewayTimeout,
            "availability_retired" => Self::Gone,
            "missing_reason" => Self::Unprocessable,
            c if c.starts_with("no_") || c.starts_with("missing_") => Self::NotFound,
            c if c.ends_with("_conflict")
                || c.ends_with("_retired")
                || c.starts_with("stale_")
                || c.ends_with("_changed")
                || c.ends_with("_regression")
                || matches!(
                    c,
                    "selected_result"
                        | "operation_active"
                        | "operation_completed"
                        | "operation_abandoned"
                        | "abandonment_conflict"
                        | "correction_required"
                        | "correction_mismatch"
                        | "correction_unknown"
                        | "grant_in_use"
                        | "pending_source"
                        | "source_fenced"
                        | "source_fence_lost"
                        | "unproven_succession"
                        | "continuous_child_owned"
                ) =>
            {
                Self::Conflict
            }
            "capacity" | "metadata_capacity" | "disk_capacity" => Self::ServiceUnavailable,
            "worker" | "io" | "iceberg" | "arrow" | "parquet" => Self::Internal,
            // Explicit pre-admission schema, parameter, and specification refusals (deterministic)
            "input_digest_mismatch"
            | "invalid_correction"
            | "release_mismatch"
            | "invalid_request"
            | "invalid_update"
            | "invalid_schema"
            | "invalid_definition"
            | "invalid_sql"
            | "invalid_id"
            | "invalid_json"
            | "invalid_operation"
            | "invalid_base"
            | "invalid_group"
            | "invalid_relationship"
            | "invalid_backfill"
            | "invalid_comparison"
            | "invalid_compaction"
            | "invalid_export"
            | "invalid_removal"
            | "invalid_evidence" => Self::Unprocessable,
            _ => Self::Internal,
        }
    }
}

/// An actionable failure, with a stable code and original cause.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    code: &'static str,
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
    backtrace: Backtrace,
}

impl Error {
    /// Creates a refusal without discarding already committed work.
    pub fn refusal(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::from_code(code),
            code,
            message: message.into(),
            source: None,
            backtrace: Backtrace::disabled(),
        }
    }

    /// Creates a refusal with an explicit error kind.
    pub fn refusal_with_kind(
        kind: ErrorKind,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            code,
            message: message.into(),
            source: None,
            backtrace: Backtrace::disabled(),
        }
    }

    /// Returns the typed category of this failure.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns a stable refusal code for transports and recovery tools.
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// Returns the diagnostic refusal message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the captured or disabled backtrace for this failure.
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    fn caused(code: &'static str, cause: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            kind: ErrorKind::from_code(code),
            code,
            message: cause.to_string(),
            source: Some(Box::new(cause)),
            backtrace: Backtrace::capture(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
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
from_error!(tokio::task::JoinError, "worker");

impl From<datafusion::error::DataFusionError> for Error {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        let mut cause: &dyn std::error::Error = &error;
        let mut code = "query";
        loop {
            if let Some(refusal) = cause.downcast_ref::<Self>() {
                code = refusal.code;
                break;
            }
            match cause.source() {
                Some(source) => cause = source,
                None => break,
            }
        }
        Self::caused(code, error)
    }
}

/// A result whose error retains a stable refusal code.
pub type Result<T> = std::result::Result<T, Error>;
