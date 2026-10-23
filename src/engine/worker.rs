//! Owned service lifetimes and shared admission before future destructive maintenance.

#[cfg(test)]
mod tests;

use super::*;
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use std::{future::Future, sync::atomic::Ordering};
use tokio::{runtime::Handle, sync::OwnedSemaphorePermit};

static CLOCK_ORIGIN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

/// Process ownership retained after uncertain private-worker termination.
#[derive(Debug)]
pub(super) struct Host {
    _workspace: WorkspaceLock,
    uncertain: AtomicBool,
    pub(super) instance: String,
    clock_origin: std::time::Instant,
}

impl Host {
    pub(super) fn acquire(root: &Path) -> Result<Arc<Self>> {
        let origin = *CLOCK_ORIGIN.get_or_init(std::time::Instant::now);
        Ok(Arc::new(Self {
            _workspace: WorkspaceLock::acquire(root)?,
            uncertain: AtomicBool::new(false),
            instance: uuid::Uuid::new_v4().to_string(),
            clock_origin: origin,
        }))
    }

    pub(super) fn monotonic_millis(&self) -> Result<u64> {
        u64::try_from(self.clock_origin.elapsed().as_millis())
            .map_err(|_| Error::refusal("source_fence_clock", "host monotonic clock overflow"))
    }

    pub(super) fn unix_millis() -> Result<u64> {
        let duration = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::refusal("source_fence_clock", "system clock before UNIX epoch"))?;
        u64::try_from(duration.as_millis())
            .map_err(|_| Error::refusal("source_fence_clock", "system clock overflow"))
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
            // Deliberate leak: if a private worker task panics or is aborted during
            // Tokio runtime shutdown while holding catalog/SQLite or blocking pool work,
            // no timeout can prove that background side effects have ceased. Leaking one
            // `Arc<Host>` reference keeps the OS `WorkspaceLock` held until process exit
            // while marking the host `uncertain` so new engine calls fail closed.
            std::mem::forget(self.host.clone());
        }
    }
}

/// Ownership bundle moved into each spawned private worker task.
///
/// # Field Drop Order Invariant
///
/// Rust drops struct fields in declaration order:
/// 1. `custody` MUST be declared first so that on abnormal task termination (panic or
///    cancellation), `Custody::drop` marks `host.uncertain = true` (`Ordering::AcqRel`)
///    and leaks the workspace lock *before* `_guard` is dropped.
/// 2. `_guard` (the shared or exclusive `maintenance` lock guard) drops second, waking
///    any `Engine::shutdown` waiter only after `host.uncertain` is guaranteed visible.
/// 3. `_request` and `_task` semaphore permits drop last after the task body has exited.
struct Admitted<G> {
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

    /// Validates admission state, captures the current Tokio runtime handle, and
    /// reserves the poll-frame memory permit for a private worker task.
    fn prepare_worker_task<F, Fut>(&self) -> Result<(Handle, OwnedSemaphorePermit)> {
        self.check_admission()?;
        let runtime = runtime()?;
        let task = self.inner.lifecycle.budget.reserve(
            std::mem::size_of::<Fut>()
                .saturating_add(std::mem::size_of::<F>())
                .saturating_add(4096),
        )?;
        Ok((runtime, task))
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
        let (runtime, task) = self.prepare_worker_task::<F, Fut>()?;
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
        let (runtime, task) = self.prepare_worker_task::<F, Fut>()?;
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
        let (runtime, task) = self.prepare_worker_task::<F, Fut>()?;
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
