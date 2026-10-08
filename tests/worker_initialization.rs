//! Initialization retains host ownership when its public caller disappears.

use docspec::{Engine, budget::Limits};
use tempfile::TempDir;

#[tokio::test(flavor = "current_thread")]
async fn canceled_open_finishes_owned_initialization_before_releasing_host() {
    let root = TempDir::new().unwrap();
    let mut opening = Box::pin(Engine::open(root.path(), Limits::default()));
    assert!(futures::poll!(&mut opening).is_pending());
    drop(opening);
    // The private initialization has not been polled on this runtime yet.
    assert_eq!(
        Engine::open(root.path(), Limits::default())
            .await
            .unwrap_err()
            .code(),
        "workspace_busy"
    );
    // No competing mutation is started while waiting: failed opens only try
    // the host lock. The first successful open follows detached setup completion.
    let engine = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            match Engine::open(root.path(), Limits::default()).await {
                Ok(engine) => break engine,
                Err(error) => assert_eq!(error.code(), "workspace_busy"),
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    engine.shutdown().await.unwrap();
}

#[test]
fn forced_shutdown_before_initialization_poll_quarantines_host_until_process_exit() {
    let root = TempDir::new().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut opening = Box::pin(Engine::open(root.path(), Limits::default()));
        assert!(futures::poll!(&mut opening).is_pending());
        drop(opening);
        // Return without yielding, leaving private setup unpolled.
    });
    drop(runtime);
    let replacement = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = replacement
        .block_on(Engine::open(root.path(), Limits::default()))
        .unwrap_err();
    assert_eq!(error.code(), "workspace_busy");
    // This unique fixture deliberately retains its host descriptor until the
    // test process exits. Runtime replacement must not release uncertain actors.
}
