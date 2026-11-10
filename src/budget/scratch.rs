//! Check remaining shared scratch capacity immediately before each physical write.
use super::Budget;
use datafusion::execution::disk_manager::RefCountedTempFile;
use std::{
    fs::File,
    io::{self, Write},
};

pub(crate) struct ScratchWriter {
    file: File,
    temporary: RefCountedTempFile,
    budget: Budget,
}
impl ScratchWriter {
    pub(super) fn new(temporary: RefCountedTempFile, budget: Budget) -> crate::Result<Self> {
        Ok(Self {
            file: temporary.inner().reopen()?,
            temporary,
            budget,
        })
    }
}
impl Write for ScratchWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let _guard = self
            .budget
            .scratch_writes
            .lock()
            .map_err(|_| io::Error::other("scratch writer lock poisoned"))?;
        let manager = &self.budget.runtime.disk_manager;
        let used = manager.used_disk_space();
        if bytes.len() as u64 > manager.max_temp_directory_size().saturating_sub(used) {
            return Err(io::Error::other(
                "scratch capacity exhausted before physical write",
            ));
        }
        let written = self.file.write(bytes)?;
        self.temporary
            .update_disk_usage()
            .map_err(io::Error::other)?;
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
