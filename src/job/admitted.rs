//! Attach byte admission to every returned Arrow buffer, including nested children.
use arrow::{
    array::{Array, ArrayData, ArrayRef, make_array},
    buffer::{BooleanBuffer, Buffer, NullBuffer},
};
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

struct Owner {
    buffer: Buffer,
    _memory: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

fn buffer(buffer: &Buffer, memory: &Arc<OwnedSemaphorePermit>) -> Buffer {
    Buffer::from(bytes::Bytes::from_owner(Owner {
        buffer: buffer.clone(),
        _memory: memory.clone(),
    }))
}

fn data(data: ArrayData, memory: &Arc<OwnedSemaphorePermit>) -> crate::Result<ArrayData> {
    let buffers = data.buffers().iter().map(|b| buffer(b, memory)).collect();
    let children = data
        .child_data()
        .iter()
        .map(|child| self::data(child.clone(), memory))
        .collect::<crate::Result<Vec<_>>>()?;
    let nulls = data.nulls().map(|nulls| {
        let bits = nulls.inner();
        NullBuffer::new(BooleanBuffer::new(
            buffer(bits.inner(), memory),
            bits.offset(),
            bits.len(),
        ))
    });
    Ok(data
        .into_builder()
        .buffers(buffers)
        .child_data(children)
        .nulls(nulls)
        .build()?)
}

pub(super) fn array(array: ArrayRef, memory: OwnedSemaphorePermit) -> crate::Result<ArrayRef> {
    Ok(make_array(data(array.to_data(), &Arc::new(memory))?))
}
