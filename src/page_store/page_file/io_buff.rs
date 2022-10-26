use std::alloc::Layout;

use photonio::fs::Metadata;

const DEFAULT_BLOCK_SIZE: usize = 4096;

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

pub(crate) struct AlignBuffer {
    data: std::ptr::NonNull<u8>,
    layout: Layout,
    size: usize,
}

impl AlignBuffer {
    pub(crate) fn new(n: usize, align: usize) -> Self {
        assert!(n > 0);
        let size = ceil_to_block_hi_pos(n, align);
        let layout = Layout::from_size_align(size, align).expect("Invalid layout");
        let data = unsafe {
            // Safety: it is guaranteed that layout size > 0.
            std::ptr::NonNull::new(std::alloc::alloc(layout)).expect("The memory is exhausted")
        };
        Self { data, layout, size }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.size
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.size) }
    }

    pub(crate) fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.size) }
    }
}

impl Drop for AlignBuffer {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

/// # Safety
///
/// [`AlignBuffer`] is [`Send`] since all accesses to the inner buf are
/// guaranteed that the aliases do not overlap.
unsafe impl Send for AlignBuffer {}

/// # Safety
///
/// [`AlignBuffer`] is [`Send`] since all accesses to the inner buf are
/// guaranteed that the aliases do not overlap.
unsafe impl Sync for AlignBuffer {}

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
