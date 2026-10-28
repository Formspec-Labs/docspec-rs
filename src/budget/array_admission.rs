//! Attach byte admission to every returned Arrow buffer, including nested children.
// Rust guideline compliant 2026-02-21
use crate::Result;
use arrow::{
    array::{Array, ArrayData, ArrayRef, BinaryViewArray, StringViewArray, make_array},
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

/// Retains byte admission through every returned buffer and nested child.
///
/// # Errors
/// Refuses invalid Arrow data while attaching buffer ownership.
pub(crate) fn array(array: ArrayRef, memory: OwnedSemaphorePermit) -> crate::Result<ArrayRef> {
    Ok(make_array(data(array.to_data(), &Arc::new(memory))?))
}

/// Estimates header copies needed to clear ignored null values.
pub(crate) fn dirty_view_bytes(array: &dyn Array) -> usize {
    let dirty = |views: &[u128]| {
        if views
            .iter()
            .enumerate()
            .any(|(index, view)| array.is_null(index) && *view != 0)
        {
            views.len().saturating_mul(16)
        } else {
            0
        }
    };
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        return dirty(array.views());
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return dirty(array.views());
    }
    array
        .to_data()
        .child_data()
        .iter()
        .fold(0usize, |bytes, child| {
            bytes.saturating_add(dirty_view_bytes(
                arrow::array::make_array(child.clone()).as_ref(),
            ))
        })
}

/// Clears ignored null headers before native Arrow view operations.
///
/// # Errors
/// Refuses invalid array data when rebuilding headers and children.
pub(crate) fn clean_null_views(array: ArrayRef) -> Result<ArrayRef> {
    // Arrow58's view cast reserves using every header, including null rows.
    // Clear only ignored headers before that maintained cast, without copying
    // source payloads or making their unknown bytes meaningful.
    macro_rules! clean {
        ($kind:ty) => {
            if let Some(view) = array.as_any().downcast_ref::<$kind>() {
                let views = view
                    .views()
                    .iter()
                    .enumerate()
                    .map(|(index, value)| if view.is_null(index) { 0 } else { *value })
                    .collect::<Vec<_>>();
                return Ok(Arc::new(<$kind>::try_new(
                    views.into(),
                    view.data_buffers().to_vec(),
                    view.nulls().cloned(),
                )?));
            }
        };
    }
    if dirty_view_bytes(array.as_ref()) == 0 {
        return Ok(array);
    }
    clean!(StringViewArray);
    clean!(BinaryViewArray);
    let data = array.to_data();
    let children = data
        .child_data()
        .iter()
        .map(|child| Ok(clean_null_views(arrow::array::make_array(child.clone()))?.to_data()))
        .collect::<Result<Vec<_>>>()?;
    Ok(arrow::array::make_array(
        data.into_builder().child_data(children).build()?,
    ))
}
