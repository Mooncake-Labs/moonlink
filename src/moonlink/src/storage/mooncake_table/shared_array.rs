use std::{cell::UnsafeCell, sync::Arc};

use crate::row::MoonlinkRow;

/// Used to share rows between write and read threads
///
/// It is guaranteed only one thread is writing to the buffer
/// And the buffer never needs to be resized
#[allow(clippy::arc_with_non_send_sync)]
pub(super) struct SharedRowBuffer {
    buffer: Arc<UnsafeCell<Vec<MoonlinkRow>>>,
}

unsafe impl Send for SharedRowBuffer {}
unsafe impl Sync for SharedRowBuffer {}

#[allow(clippy::arc_with_non_send_sync)]
pub(super) struct SharedRowBufferSnapshot {
    pub buffer: Arc<UnsafeCell<Vec<MoonlinkRow>>>,
    pub length: usize,
}

unsafe impl Send for SharedRowBufferSnapshot {}
unsafe impl Sync for SharedRowBufferSnapshot {}

impl SharedRowBuffer {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(capacity: usize) -> Self {
        let vec = Vec::with_capacity(capacity);

        SharedRowBuffer {
            buffer: Arc::new(UnsafeCell::new(vec)),
        }
    }

    pub fn push(&self, row: MoonlinkRow) {
        unsafe {
            (*self.buffer.get()).push(row);
        }
    }

    pub fn get_snapshot(&self) -> SharedRowBufferSnapshot {
        let length = unsafe { (*self.buffer.get()).len() };
        SharedRowBufferSnapshot {
            buffer: self.buffer.clone(),
            length,
        }
    }

    pub fn get_row(&self, index: usize) -> &MoonlinkRow {
        unsafe { &(*self.buffer.get())[index] }
    }
}

impl SharedRowBufferSnapshot {
    pub fn get_buffer(&self, size: usize) -> &[MoonlinkRow] {
        assert!(size <= self.length);
        unsafe { &(*self.buffer.get())[..size] }
    }
}
