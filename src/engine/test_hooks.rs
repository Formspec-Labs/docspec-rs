//! Fault-injection hooks, test pause barriers, and diagnostic counters.

#[cfg(test)]
use super::Operation;
use super::{Engine, Inner};
use crate::{Error, Result, model::*};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

#[derive(Debug, Default)]
pub(in crate::engine) struct TestHooks {
    #[cfg(test)]
    pub(in crate::engine) release_decodes: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub(in crate::engine) initial_build_queries: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub(in crate::engine) identity_build_fast_path: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub(in crate::engine) routine_checks: std::sync::atomic::AtomicUsize,
    #[cfg(feature = "test-util")]
    pub(in crate::engine) scalar_reuse_pause: tokio::sync::Mutex<Option<ScalarReusePause>>,
}

impl std::ops::Deref for Inner {
    type Target = TestHooks;

    fn deref(&self) -> &Self::Target {
        &self.test_hooks
    }
}

#[cfg(feature = "test-util")]
#[derive(Debug)]
pub(in crate::engine) struct ScalarReusePause {
    dataset: Id,
    operation: Id,
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

impl Engine {
    /// Pauses the next actual scalar file reuse for this operation in tests.
    ///
    /// The first channel signals the hit; sending or dropping the second resumes
    /// work. Installing a pause replaces any unused pause on this engine.
    #[cfg(feature = "test-util")]
    pub async fn pause_scalar_file_reuse(
        &self,
        dataset: Id,
        operation: Id,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached, receiver) = tokio::sync::oneshot::channel();
        let (sender, release) = tokio::sync::oneshot::channel();
        *self.inner.scalar_reuse_pause.lock().await = Some(ScalarReusePause {
            dataset,
            operation,
            reached,
            release,
        });
        (receiver, sender)
    }

    #[cfg(feature = "test-util")]
    pub(in crate::engine) async fn scalar_file_reused(&self, request: &OperationRequest) {
        let pause = {
            let mut pending = self.inner.scalar_reuse_pause.lock().await;
            if pending.as_ref().is_some_and(|pause| {
                &pause.dataset == request.dataset() && &pause.operation == request.operation()
            }) {
                pending.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            let _ = pause.reached.send(());
            let _ = pause.release.await;
        }
    }

    #[cfg(test)]
    pub(in crate::engine) async fn reserve_attempt(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        purpose: &str,
    ) -> Result<PathBuf> {
        // Existing non-Build paths retain their local API and refuse URIs before effects.
        self.inner.retained.local_path()?;
        self.reserve_location(spec, operation, purpose)
            .await?
            .into_local()
    }
}

pub(in crate::engine) fn stop_after(stop: Option<&str>, phase: &str) -> Result<()> {
    if stop == Some(phase) {
        return Err(Error::refusal(
            "injected_failure",
            format!("interrupted after durable {phase}"),
        ));
    }
    Ok(())
}

#[cfg(feature = "test-util")]
fn begin_test_pause(phase: &str) -> Option<std::ffi::OsString> {
    if std::env::var("DOCSPEC_TEST_PAUSE_BEFORE").as_deref() != Ok(phase) {
        return None;
    }
    let marker =
        std::env::var_os("DOCSPEC_TEST_PAUSE_MARKER").expect("test pause needs a marker path");
    let resume =
        std::env::var_os("DOCSPEC_TEST_RESUME_MARKER").expect("test pause needs a resume path");
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(marker)
    {
        Ok(_) => Some(resume),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => None,
        Err(error) => panic!("cannot create test pause marker: {error}"),
    }
}

#[cfg(feature = "test-util")]
async fn wait_for_resume(resume: &std::ffi::OsStr) {
    let started = std::time::Instant::now();
    while !Path::new(resume).exists() {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(60),
            "test pause was not released within 60 seconds"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

// Test-only observations prove actual work entry, independently of retained
// records that deliberately preserve the first successful completion on retry.
#[cfg(feature = "test-util")]
pub(in crate::engine) async fn work_started_async(phase: &str) {
    // A subprocess fixture can stop the real HTTP operation between durable
    // phases without returning an injected error or replacing its worker.
    if let Some(resume) = begin_test_pause(phase) {
        wait_for_resume(&resume).await;
    }
    eprintln!("DOCSPEC_TEST_WORK {phase}");
}

#[cfg(feature = "test-util")]
pub(in crate::engine) fn work_started(phase: &str) {
    if let Some(resume) = begin_test_pause(phase) {
        if let Ok(handle) = tokio::runtime::Handle::try_current()
            && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        {
            tokio::task::block_in_place(|| {
                handle.block_on(wait_for_resume(&resume));
            });
        } else {
            std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_time()
                            .build()
                            .expect("test pause runtime")
                            .block_on(wait_for_resume(&resume));
                    })
                    .join()
                    .expect("test pause thread");
            });
        }
    }
    eprintln!("DOCSPEC_TEST_WORK {phase}");
}
