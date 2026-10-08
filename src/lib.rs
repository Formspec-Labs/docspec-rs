//! Recoverable dataset operations backed by an injected Iceberg catalog.
//!
//! Lifecycle records live in Iceberg. The catalog database contains only native
//! catalog bookkeeping. Application selection is distinct from native table heads.

pub mod budget;
pub mod catalog;
pub mod engine;
pub mod error;
pub mod http;
pub mod model;
mod query;
pub mod storage;

#[doc(inline)]
pub use engine::Engine;
#[doc(inline)]
pub use error::{Error, Result};
