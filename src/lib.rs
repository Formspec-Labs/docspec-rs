//! Recoverable dataset operations backed by an injected Iceberg catalog.
//!
//! Lifecycle records live in Iceberg. The catalog database contains only native
//! catalog bookkeeping. Application selection is distinct from native table heads.

pub mod budget;
pub mod catalog;
pub mod engine;
pub mod error;
pub mod evidence;
pub mod export;
pub mod http;
pub mod model;
mod progress;
mod query;
pub mod restore;
pub mod storage;

#[doc(inline)]
pub use catalog::LifecycleCompactionResult;
#[doc(inline)]
pub use engine::BackendCapability;
#[doc(inline)]
pub use engine::Engine;
#[doc(inline)]
pub use error::{Error, ErrorKind, Result};
#[doc(inline)]
pub use model::{
    AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition, BinaryOp,
    CaseWhen, ComparisonOp, JoinCardinality, JoinDefinition, JoinField, JoinKey, JoinKind,
    JoinSide, JoinTransform, LiteralValue, NullMatching, NullOrdering, Predicate, RefreshMode,
    RefreshPolicy, RefreshSpec, RefreshTransform, ScalarExpr, SortColumn, SortDirection,
    UpdateMergeSpec, UpdateMergeStrategy, UpdateSpec,
};
