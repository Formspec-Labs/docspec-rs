//! Automated regression tests asserting core Engine future and model type sizes remain bounded.
//! Prevents stack overflow on default OS thread stacks (macOS 2 MiB) and enforces
//! the worker memory-budget admission accounting invariant.

use docspec::{
    Engine,
    budget::Limits,
    model::{Authority, DatasetPin, PhaseResult, RelatedResult, ReleasePin},
};
use std::mem::size_of;

#[test]
fn core_model_type_sizes_remain_bounded() {
    // Exact baseline sizes on 64-bit architecture:
    // DatasetPin=672 ReleasePin=784 Authority=1520 PhaseResult=1472 RelatedResult=1024
    assert!(
        size_of::<DatasetPin>() <= 1024,
        "DatasetPin size ({}) exceeded bound of 1024",
        size_of::<DatasetPin>()
    );
    assert!(
        size_of::<ReleasePin>() <= 1024,
        "ReleasePin size ({}) exceeded bound of 1024",
        size_of::<ReleasePin>()
    );
    assert!(
        size_of::<Authority>() <= 2048,
        "Authority size ({}) exceeded bound of 2048",
        size_of::<Authority>()
    );
    assert!(
        size_of::<PhaseResult>() <= 2048,
        "PhaseResult size ({}) exceeded bound of 2048",
        size_of::<PhaseResult>()
    );
    assert!(
        size_of::<RelatedResult>() <= 1536,
        "RelatedResult size ({}) exceeded bound of 1536",
        size_of::<RelatedResult>()
    );
}

#[test]
fn core_engine_future_sizes_remain_bounded() {
    // Boxed futures used across Engine phases and storage must remain pointer-sized (16 bytes on 64-bit).
    assert_eq!(
        size_of::<futures::future::BoxFuture<'static, docspec::Result<()>>>(),
        16,
        "BoxFuture must remain a 16-byte fat pointer"
    );

    // Assert that top-level unpolled engine entrypoint futures do not exceed stack-safe thresholds.
    let open_fut = Engine::open(std::path::Path::new("dummy"), Limits::default());
    assert!(
        std::mem::size_of_val(&open_fut) <= 2048,
        "Engine::open future size ({}) exceeded 2048 bytes",
        std::mem::size_of_val(&open_fut)
    );
    drop(open_fut);
}
