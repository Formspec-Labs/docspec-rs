//! Owned service lifetimes and shared admission before future destructive maintenance.

#[cfg(test)]
mod tests;

use super::*;
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use std::{future::Future, sync::atomic::Ordering};
use tokio::{runtime::Handle, sync::OwnedSemaphorePermit};

/// Process ownership retained after uncertain private-worker termination.
#[derive(Debug)]
pub(super) struct Host {
    _workspace: WorkspaceLock,
    uncertain: AtomicBool,
}

impl Host {
    pub(super) fn acquire(root: &Path) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            _workspace: WorkspaceLock::acquire(root)?,
            uncertain: AtomicBool::new(false),
        }))
    }

    fn check(&self) -> Result<()> {
        if self.uncertain.load(Ordering::Acquire) {
            return Err(Error::refusal(
                "host_uncertain",
                "a private worker stopped before finishing; restart the process before reopening this workspace",
            ));
        }
        Ok(())
    }
}

/// Holds the OS lock even when initialization or a private task never gets polled.
pub(super) struct Custody {
    host: Arc<Host>,
    completed: bool,
}

impl Custody {
    pub(super) fn new(host: Arc<Host>) -> Self {
        Self {
            host,
            completed: false,
        }
    }

    pub(super) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for Custody {
    fn drop(&mut self) {
        if !self.completed && !self.host.uncertain.swap(true, Ordering::AcqRel) {
            // Tokio shutdown/forced abort may leave blocking or SQLite work alive.
            // Quarantine only the host lock until process exit, not the catalog or
            // execution runtime. No timeout can prove that old effects have ended.
            std::mem::forget(self.host.clone());
        }
    }
}

struct Admitted<G> {
    // Poison the host before releasing maintenance admission on abnormal drop.
    custody: Custody,
    _guard: G,
    _request: OwnedSemaphorePermit,
    _task: OwnedSemaphorePermit,
}

impl<G> Admitted<G> {
    fn complete(mut self) {
        self.custody.completed = true;
    }
}

pub(super) fn runtime() -> Result<Handle> {
    Handle::try_current().map_err(|_| {
        Error::refusal(
            "runtime_required",
            "engine catalog operations require a Tokio runtime",
        )
    })
}

impl Engine {
    pub(super) fn request_memory(&self, request: &impl Serialize) -> Result<OwnedSemaphorePermit> {
        self.check_admission()?;
        let budget = &self.inner.lifecycle.budget;
        let bytes = inputs::metadata_size(request, budget.limits.max_metadata_bytes)?;
        // Account for owned strings, maps and vectors before cloning. Like the
        // metadata reader, this is conservative admission, not a total RSS bound.
        budget.reserve(bytes.saturating_mul(8))
    }

    fn check_admission(&self) -> Result<()> {
        self.inner.host.check()?;
        if self.inner.closing.load(Ordering::Acquire) {
            return Err(Error::refusal(
                "host_closing",
                "engine shutdown has stopped new catalog work",
            ));
        }
        Ok(())
    }

    pub(super) async fn admit_work<T, F, Fut>(
        &self,
        memory: OwnedSemaphorePermit,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Engine) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.check_admission()?;
        let runtime = runtime()?;
        let task = self.inner.lifecycle.budget.reserve(
            std::mem::size_of::<Fut>()
                .saturating_add(std::mem::size_of::<F>())
                .saturating_add(4096),
        )?;
        // Cancellation while waiting starts no work. Once admitted there is no
        // await before spawning the task that owns the complete service future.
        let guard = self.inner.maintenance.clone().read_owned().await;
        self.check_admission()?;
        self.spawn_work(runtime, guard, memory, task, operation)
            .await?
    }

    pub(super) async fn maintain_work<T, F, Fut>(
        &self,
        memory: OwnedSemaphorePermit,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Engine) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.check_admission()?;
        let runtime = runtime()?;
        let task = self.inner.lifecycle.budget.reserve(
            std::mem::size_of::<Fut>()
                .saturating_add(std::mem::size_of::<F>())
                .saturating_add(4096),
        )?;
        let guard = self.inner.maintenance.clone().write_owned().await;
        self.check_admission()?;
        self.spawn_work(runtime, guard, memory, task, operation)
            .await?
    }

    pub(super) async fn admit_stream<F, Fut>(
        &self,
        memory: OwnedSemaphorePermit,
        operation: F,
    ) -> Result<SendableRecordBatchStream>
    where
        F: FnOnce(Engine) -> Fut + Send + 'static,
        Fut: Future<Output = Result<SendableRecordBatchStream>> + Send + 'static,
    {
        self.check_admission()?;
        let runtime = runtime()?;
        let task = self.inner.lifecycle.budget.reserve(
            std::mem::size_of::<Fut>()
                .saturating_add(std::mem::size_of::<F>())
                .saturating_add(4096),
        )?;
        let guard = Arc::new(self.inner.maintenance.clone().read_owned().await);
        self.check_admission()?;
        // Share this admission instead of reacquiring after a writer has queued.
        // The owned opening task keeps custody if its caller is canceled.
        self.spawn_work(
            runtime,
            guard.clone(),
            memory,
            task,
            move |engine| async move {
                let stream = operation(engine.clone()).await?;
                let schema = stream.schema();
                let retained = futures::stream::unfold(
                    (stream, guard, engine),
                    |(mut stream, guard, engine)| async move {
                        stream
                            .next()
                            .await
                            .map(|batch| (batch, (stream, guard, engine)))
                    },
                );
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, retained))
                    as SendableRecordBatchStream)
            },
        )
        .await?
    }

    fn spawn_work<T, G, F, Fut>(
        &self,
        runtime: Handle,
        guard: G,
        memory: OwnedSemaphorePermit,
        task: OwnedSemaphorePermit,
        operation: F,
    ) -> tokio::task::JoinHandle<Result<T>>
    where
        T: Send + 'static,
        G: Send + 'static,
        F: FnOnce(Engine) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let admitted = Admitted {
            custody: Custody::new(self.inner.host.clone()),
            _guard: guard,
            _request: memory,
            _task: task,
        };
        let engine = self.clone();
        runtime.spawn(async move {
            let result = operation(engine).await;
            admitted.complete();
            result
        })
    }

    /// Stops new catalog work and waits for admitted operations to finish.
    ///
    /// Call before shutting down the Tokio runtime. Dropping this wait leaves
    /// admission closed; calling again resumes waiting. Other engine clones also
    /// stop accepting work. Engine read streams retain admission until EOF or
    /// drop. Drop all clones after completion to release the host. Catalog-free
    /// readers keep their existing caller-retained or published-availability rules.
    ///
    /// # Errors
    /// Refuses an uncertain host after private-worker panic or forced termination.
    pub async fn shutdown(&self) -> Result<()> {
        self.inner.closing.store(true, Ordering::Release);
        let _exclusive = self.inner.maintenance.write().await;
        self.inner.host.check()
    }
}
