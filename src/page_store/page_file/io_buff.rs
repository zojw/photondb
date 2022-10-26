use std::{alloc::Layout, cell::RefCell, rc::Rc};

use buddy_alloc::{buddy_alloc::BuddyAlloc, BuddyAllocParam};
use photonio::fs::Metadata;

const DEFAULT_BLOCK_SIZE: usize = 4096;
const ALLOCATOR_ALIGN: usize = 4096;

pub(crate) async fn logical_block_size(meta: &Metadata) -> usize {
    use std::os::unix::prelude::MetadataExt;
    // same as `major(3)` https://github.com/torvalds/linux/blob/5a18d07ce3006dbcb3c4cfc7bf1c094a5da19540/tools/include/nolibc/types.h#L191
    let major = (meta.dev() >> 8) & 0xfff;
    if let Ok(block_size_str) =
        std::fs::read_to_string(format!("/sys/dev/block/{major}:0/queue/logical_block_size"))
    {
        let block_size_str = block_size_str.trim();
        if let Ok(size) = block_size_str.parse::<usize>() {
            return size;
        }
    }
    DEFAULT_BLOCK_SIZE
}

pub(crate) struct IoBufferAllocator {
    data: std::ptr::NonNull<u8>,
    layout: Layout,
    size: usize,
    allocator: RefCell<BuddyAlloc>,
    buffer_id: Option<u32>,
}

impl Drop for IoBufferAllocator {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

impl IoBufferAllocator {
    pub(crate) fn new(io_memory_size: usize) -> Self {
        let size = ceil_to_block_hi_pos(io_memory_size, 4096);
        let layout = Layout::from_size_align(size, 4096).expect("invalid layout for allocator");
        let (data, allocator) = unsafe {
            let data =
                std::ptr::NonNull::new(std::alloc::alloc(layout)).expect("memory is exhausted");
            let allocator = RefCell::new(BuddyAlloc::new(BuddyAllocParam::new(
                data.as_ptr(),
                layout.size(),
                layout.align(),
            )));
            (data, allocator)
        };
        Self {
            data,
            layout,
            size,
            allocator,
            buffer_id: None,
        }
    }

    pub(crate) fn alloc_buffer(self: &Rc<Self>, n: usize, align: usize) -> IoBuffer {
        let size = ceil_to_block_hi_pos(n, align);
        let mut alloacator = self.allocator.borrow_mut();
        match std::ptr::NonNull::new(alloacator.malloc(size)) {
            Some(data) => IoBuffer::UringBuffer {
                allocator: self.clone(),
                data,
                buffer_id: self.buffer_id,
                size,
            },
            None => Self::alloc_plain(n, align),
        }
    }

    fn alloc_plain(size: usize, align: usize) -> IoBuffer {
        assert!(size > 0);
        let layout = Layout::from_size_align(size, align).expect("Invalid layout");
        let data = unsafe {
            // Safety: it is guaranteed that layout size > 0.
            std::ptr::NonNull::new(std::alloc::alloc(layout)).expect("The memory is exhausted")
        };
        IoBuffer::PlainBuffer { data, layout, size }
    }
}

pub(crate) enum IoBuffer {
    UringBuffer {
        allocator: Rc<IoBufferAllocator>,
        data: std::ptr::NonNull<u8>,
        buffer_id: Option<u32>,
        size: usize,
    },
    PlainBuffer {
        data: std::ptr::NonNull<u8>,
        layout: Layout,
        size: usize,
    },
}

impl Drop for IoBuffer {
    fn drop(&mut self) {
        match self {
            IoBuffer::UringBuffer {
                allocator, data, ..
            } => {
                let ptr = data;
                let mut alloc = allocator.allocator.borrow_mut();
                alloc.free(ptr.as_ptr() as *mut u8);
            }
            IoBuffer::PlainBuffer { data, layout, .. } => unsafe {
                std::alloc::dealloc(data.as_ptr(), *layout);
            },
        }
    }
}

impl IoBuffer {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        match self {
            IoBuffer::UringBuffer { size, .. } => size,
            IoBuffer::PlainBuffer { size, .. } => size,
        }
        .to_owned()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            IoBuffer::UringBuffer { data, size, .. } => unsafe {
                std::slice::from_raw_parts(data.as_ptr(), *size)
            },
            IoBuffer::PlainBuffer { data, size, .. } => unsafe {
                std::slice::from_raw_parts(data.as_ptr(), *size)
            },
        }
    }

    pub(crate) fn as_bytes_mut(&mut self) -> &mut [u8] {
        match self {
            IoBuffer::UringBuffer { data, size, .. } => unsafe {
                std::slice::from_raw_parts_mut(data.as_ptr(), *size)
            },
            IoBuffer::PlainBuffer { data, size, .. } => unsafe {
                std::slice::from_raw_parts_mut(data.as_ptr(), *size)
            },
        }
    }
}

/// # Safety
///
/// [`AlignBuffer`] is [`Send`] since all accesses to the inner buf are
/// guaranteed that the aliases do not overlap.
unsafe impl Send for IoBuffer {}

/// # Safety
///
/// [`AlignBuffer`] is [`Send`] since all accesses to the inner buf are
/// guaranteed that the aliases do not overlap.
unsafe impl Sync for IoBuffer {}

#[inline]
pub(crate) fn floor_to_block_lo_pos(pos: usize, align: usize) -> usize {
    pos - (pos & (align - 1))
}

#[inline]
pub(crate) fn ceil_to_block_hi_pos(pos: usize, align: usize) -> usize {
    ((pos + align - 1) / align) * align
}

#[inline]
pub(crate) fn is_block_algined_pos(pos: usize, align: usize) -> bool {
    (pos & (align - 1)) == 0
}

#[inline]
pub(crate) fn is_block_aligned_ptr(p: *const u8, align: usize) -> bool {
    p.is_aligned_to(align)
}
