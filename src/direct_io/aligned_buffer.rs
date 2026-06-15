// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::alloc::{alloc_zeroed, dealloc, Layout};

/// Heap-allocated buffer whose start address is aligned to a runtime-determined boundary.
///
/// Required for `O_DIRECT` on Linux, which rejects `read`/`write` calls whose user
/// buffer pointer is not aligned to the device's logical block size (typically
/// 512 B or 4 KiB).
pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
}

// SAFETY: AlignedBuffer owns a unique allocation and exposes &mut [u8] only through &mut self,
// so it has the same threading guarantees as Box<[u8]>.
#[expect(unsafe_code, reason = "raw alloc requires unsafe")]
unsafe impl Send for AlignedBuffer {}

// SAFETY: see Send impl.
#[expect(unsafe_code, reason = "raw alloc requires unsafe")]
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Allocates a zeroed buffer of `capacity` bytes, with start address aligned to `alignment`.
    ///
    /// `capacity` must be a multiple of `alignment`; otherwise the OS may reject the I/O
    /// even though the start is aligned.
    ///
    /// # Panics
    ///
    /// Panics if the allocation fails or the arguments do not satisfy `Layout::from_size_align`.
    #[must_use]
    pub fn new(capacity: usize, alignment: usize) -> Self {
        assert!(capacity > 0, "AlignedBuffer capacity must be > 0");
        assert!(
            alignment.is_power_of_two(),
            "AlignedBuffer alignment ({alignment}) must be a power of two",
        );
        assert!(
            capacity.is_multiple_of(alignment),
            "AlignedBuffer capacity ({capacity}) must be a multiple of alignment ({alignment})",
        );

        // Layout::from_size_align rejects non-power-of-two alignments and oversize
        // (rounded-up size that overflows isize). Both are excluded by the asserts
        // above plus the BUFFER_BLOCKS bound on capacity.
        #[expect(clippy::expect_used, reason = "alignment and capacity validated above")]
        let layout = Layout::from_size_align(capacity, alignment)
            .expect("alignment is a power of two and capacity fits");

        // SAFETY: layout has non-zero size (asserted above); alloc_zeroed returns an
        // aligned pointer or null. We check for null and abort if alloc failed.
        #[expect(unsafe_code, reason = "raw alloc requires unsafe")]
        let ptr = unsafe { alloc_zeroed(layout) };

        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        Self { ptr, layout }
    }

    /// Returns the buffer as an immutable slice covering its entire allocated capacity.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr was produced by alloc_zeroed with size=layout.size(); buffer is initialized.
        #[expect(unsafe_code, reason = "raw pointer dereference")]
        unsafe {
            std::slice::from_raw_parts(self.ptr, self.layout.size())
        }
    }

    /// Returns the buffer as a mutable slice covering its entire allocated capacity.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: see as_slice; &mut self enforces unique access.
        #[expect(unsafe_code, reason = "raw pointer dereference")]
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr, self.layout.size())
        }
    }

    /// Returns the buffer capacity in bytes.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.layout.size()
    }

    /// Returns the alignment of the buffer's start pointer.
    #[cfg(test)]
    #[must_use]
    pub fn alignment(&self) -> usize {
        self.layout.align()
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: ptr and layout match the original alloc_zeroed call.
        #[expect(unsafe_code, reason = "raw dealloc requires unsafe")]
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn aligned_buffer_basic() {
        let mut buf = AlignedBuffer::new(4_096, 4_096);
        assert_eq!(buf.capacity(), 4_096);
        assert_eq!(buf.alignment(), 4_096);

        assert!((buf.as_slice().as_ptr() as usize).is_multiple_of(4_096));

        // Round-trip data
        for (i, b) in buf.as_mut_slice().iter_mut().enumerate() {
            #[expect(clippy::cast_possible_truncation, reason = "test loop is small")]
            {
                *b = (i & 0xFF) as u8;
            }
        }
        for (i, &b) in buf.as_slice().iter().enumerate() {
            #[expect(clippy::cast_possible_truncation, reason = "test loop is small")]
            {
                assert_eq!(b, (i & 0xFF) as u8);
            }
        }
    }

    #[test]
    fn aligned_buffer_pointer_is_aligned_to_alignment() {
        for align_log2 in 9..=14 {
            let align = 1usize << align_log2;
            let buf = AlignedBuffer::new(align * 2, align);
            assert_eq!(
                (buf.as_slice().as_ptr() as usize) % align,
                0,
                "buffer pointer not aligned to {align}",
            );
        }
    }

    #[test]
    fn aligned_buffer_starts_zeroed() {
        let buf = AlignedBuffer::new(4_096, 512);
        assert!(buf.as_slice().iter().all(|&b| b == 0));
    }

    #[test]
    #[should_panic(expected = "must be a multiple of alignment")]
    fn aligned_buffer_capacity_not_multiple_of_alignment() {
        let _ = AlignedBuffer::new(3_000, 4_096);
    }

    #[test]
    #[should_panic(expected = "must be > 0")]
    fn aligned_buffer_zero_capacity() {
        let _ = AlignedBuffer::new(0, 4_096);
    }
}
