// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Multi-block bump-allocating arena for skiplist node storage.
//!
//! Blocks are allocated lazily in 4 MiB chunks — the arena never pre-allocates
//! a large contiguous buffer, so it works on 32-bit targets with limited
//! address space.  Once a block is full, a new one is allocated and the
//! remaining space in the old block is abandoned (waste is negligible for
//! typical node allocations of < 100 bytes).

use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

/// Bits used for the within-block offset.
///
/// On 64-bit: 2^26 = 64 MiB per block (1M entries fit in block 0,
/// avoiding multi-block decode overhead).
/// On 32-bit: 2^22 = 4 MiB per block (keeps allocation within
/// the limited virtual address space).
#[cfg(target_pointer_width = "32")]
const BLOCK_SHIFT: u32 = 22;
#[cfg(not(target_pointer_width = "32"))]
const BLOCK_SHIFT: u32 = 26;

/// Size of each arena block in bytes.
const BLOCK_SIZE: u32 = 1 << BLOCK_SHIFT;

/// Bitmask for extracting the within-block offset from an encoded u32.
const BLOCK_MASK: u32 = BLOCK_SIZE - 1;

/// Maximum number of blocks.  Supports up to 4 GiB total arena capacity.
const MAX_BLOCKS: usize = 1 << (32 - BLOCK_SHIFT);

/// A multi-block bump-allocating arena.
///
/// Thread-safe: concurrent allocations are serialised by a CAS loop on the
/// bump cursor.  Blocks are allocated lazily via CAS on `AtomicPtr`, so only
/// the blocks that are actually needed consume memory.
///
/// The u32 offset returned by [`alloc`](Self::alloc) encodes the block
/// index in the high bits and the within-block offset in the low
/// `BLOCK_SHIFT` bits (26 on 64-bit, 22 on 32-bit).
pub struct Arena {
    /// Block pointers.  Null means not yet allocated.  Once set to non-null,
    /// a block pointer is never modified — reads may use `Relaxed` ordering
    /// as long as the caller establishes happens-before via the skiplist CAS
    /// chain.
    blocks: Box<[AtomicPtr<u8>]>,

    /// Allocation cursor.  High 10 bits = block index, low 22 bits = offset
    /// within that block.  Starts at 1 (offset 0 is the UNSET sentinel).
    cursor: AtomicU32,
}

// Send+Sync derived automatically: all fields (Box<[AtomicPtr<_>]>, AtomicU32)
// are Send+Sync.

impl Arena {
    /// Creates a new empty arena.  No memory is allocated until the first
    /// [`alloc`](Self::alloc) call.
    pub fn new() -> Self {
        let mut blocks = Vec::with_capacity(MAX_BLOCKS);
        for _ in 0..MAX_BLOCKS {
            blocks.push(AtomicPtr::new(ptr::null_mut()));
        }

        Self {
            blocks: blocks.into_boxed_slice(),
            // Offset 0 is reserved as the UNSET sentinel.
            cursor: AtomicU32::new(1),
        }
    }

    /// Allocates `size` bytes with the given alignment.
    ///
    /// Returns the encoded offset, or `None` if `size` is zero,
    /// `size >= BLOCK_SIZE`, `align` is not a power of two, or the
    /// arena is exhausted (> 4 GiB total).
    pub fn alloc(&self, size: u32, align: u32) -> Option<u32> {
        if !align.is_power_of_two() || size == 0 || size >= BLOCK_SIZE {
            return None;
        }

        loop {
            // Acquire pairs with the AcqRel CAS below: any thread that reads
            // a cursor value (block_idx, offset) is guaranteed to see the
            // corresponding blocks[block_idx] pointer set by ensure_block,
            // which runs before the CAS that published this cursor value.
            let cur = self.cursor.load(Ordering::Acquire);
            let block_idx = cur >> BLOCK_SHIFT;
            let offset = cur & BLOCK_MASK;
            // Cannot overflow: offset < BLOCK_SIZE (≤ 2^26), align < BLOCK_SIZE.
            let aligned = (offset + align - 1) & !(align - 1);

            if let Some(new_end) = aligned.checked_add(size) {
                if new_end < BLOCK_SIZE {
                    // Strict `<`: when new_end == BLOCK_SIZE the bitwise OR
                    // on the next line would set bit BLOCK_SHIFT in new_end,
                    // colliding with the block_idx bits and wrapping the
                    // cursor back to offset 0 of the *current* block.
                    // Falling through to the next-block path abandons any
                    // remaining bytes in the current block (at most
                    // `BLOCK_SIZE - offset`, including a would-have-fit
                    // allocation at the end).  This waste is acceptable
                    // for typical node sizes.  See #119.
                    //
                    // Ensure the block exists BEFORE publishing the offset via
                    // CAS — otherwise another thread could read the cursor,
                    // compute the same block_idx, and call decode() before the
                    // block pointer is set.
                    self.ensure_block(block_idx as usize);

                    let new_cursor = (block_idx << BLOCK_SHIFT) | new_end;
                    if self
                        .cursor
                        .compare_exchange_weak(cur, new_cursor, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        return Some((block_idx << BLOCK_SHIFT) | aligned);
                    }
                } else {
                    // Advance to the next block.  Ensure it exists BEFORE
                    // publishing the new cursor, so that any thread reading
                    // the cursor will find a valid block pointer.
                    let new_block = block_idx + 1;
                    if new_block as usize >= MAX_BLOCKS {
                        return None;
                    }
                    self.ensure_block(new_block as usize);
                    let new_cursor = new_block << BLOCK_SHIFT;
                    let _ = self.cursor.compare_exchange_weak(
                        cur,
                        new_cursor,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    );
                }
            } else {
                return None;
            }
        }
    }

    /// Returns a shared reference to `len` bytes at the encoded `offset`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `offset..offset+len` was previously
    /// allocated by this arena and fully initialised.  The caller must also
    /// establish happens-before (typically via the skiplist CAS chain) so
    /// that the block pointer is visible.
    pub unsafe fn get_bytes(&self, offset: u32, len: u32) -> &[u8] {
        let (ptr, off) = self.decode(offset);
        debug_assert!(
            off + len as usize <= BLOCK_SIZE as usize,
            "get_bytes: off={off} + len={len} exceeds BLOCK_SIZE={BLOCK_SIZE} (offset={offset})",
        );
        // SAFETY: caller guarantees the range is allocated and initialised.
        std::slice::from_raw_parts(ptr.add(off), len as usize)
    }

    /// Returns an exclusive reference to `len` bytes at the encoded `offset`.
    ///
    /// # Safety
    ///
    /// The caller must ensure exclusive access to the given range.
    #[expect(
        clippy::mut_from_ref,
        reason = "interior mutability by design; caller guarantees exclusive access"
    )]
    pub unsafe fn get_bytes_mut(&self, offset: u32, len: u32) -> &mut [u8] {
        let (ptr, off) = self.decode(offset);
        // SAFETY: caller guarantees exclusive access (typically right after alloc,
        // before the node offset is published to other threads).
        std::slice::from_raw_parts_mut(ptr.add(off), len as usize)
    }

    /// Interprets 4 bytes at `offset` as an [`AtomicU32`] reference.
    ///
    /// # Safety
    ///
    /// - `offset` must be 4-byte aligned.
    /// - The region `[offset, offset+4)` must have been previously allocated.
    /// - No `&mut` reference to the same 4 bytes may exist concurrently.
    pub unsafe fn get_atomic_u32(&self, offset: u32) -> &AtomicU32 {
        let (ptr, off) = self.decode(offset);
        // SAFETY: caller guarantees alignment and prior allocation.
        // alloc(..., 4) ensures within-block alignment; the block base has
        // at least pointer-width alignment from the global allocator.
        #[expect(
            clippy::cast_ptr_alignment,
            reason = "caller guarantees 4-byte alignment via alloc(..., 4)"
        )]
        let atom_ptr = ptr.add(off).cast::<u32>();
        AtomicU32::from_ptr(atom_ptr)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Decodes an encoded offset into `(block_base_ptr, within_block_offset)`.
    ///
    /// Decodes an encoded offset into `(block_base_ptr, within_block_offset)`.
    ///
    /// Hot path: single `Acquire` load returns the cached block pointer.
    /// Cold path: spins until the block pointer becomes visible (another
    /// thread's `ensure_block` is in progress).
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "block_idx < MAX_BLOCKS by construction (alloc enforces this)"
    )]
    unsafe fn decode(&self, offset: u32) -> (*mut u8, usize) {
        let block_idx = (offset >> BLOCK_SHIFT) as usize;
        let off = (offset & BLOCK_MASK) as usize;

        let mut ptr = self.blocks[block_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            // The block is being allocated by another thread's ensure_block.
            // Spin briefly — ensure_block uses CAS with AcqRel, so the
            // pointer will become visible after a few iterations.
            for _ in 0..1000 {
                std::hint::spin_loop();
                ptr = self.blocks[block_idx].load(Ordering::Acquire);
                if !ptr.is_null() {
                    return (ptr, off);
                }
            }
            // If still null after spinning, allocate the block ourselves.
            self.ensure_block(block_idx);
            ptr = self.blocks[block_idx].load(Ordering::Acquire);
        }

        (ptr, off)
    }

    /// Ensures that the block at `idx` is allocated.  Uses CAS to avoid
    /// double-allocation when multiple threads race.
    #[expect(
        clippy::indexing_slicing,
        reason = "idx < MAX_BLOCKS enforced by alloc()"
    )]
    fn ensure_block(&self, idx: usize) {
        if self.blocks[idx].load(Ordering::Acquire).is_null() {
            // Allocate with explicit 4-byte alignment so that AtomicU32
            // accesses within the block are correctly aligned on all targets.
            let layout = Self::block_layout();

            // SAFETY: layout is non-zero (BLOCK_SIZE > 0).
            // alloc_zeroed guarantees zeroed memory (tower pointers = UNSET).
            // Visibility: the CAS below (AcqRel) makes the zeroed contents
            // visible to any thread that Acquire-loads the block pointer.
            let raw = unsafe { std::alloc::alloc_zeroed(layout) };
            if raw.is_null() {
                std::alloc::handle_alloc_error(layout);
            }

            // CAS null → raw.  If another thread won, free our block.
            if self.blocks[idx]
                .compare_exchange(ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                // SAFETY: raw was just allocated with `layout`.
                unsafe {
                    std::alloc::dealloc(raw, layout);
                }
            }
        }
    }

    /// Layout for arena blocks: `BLOCK_SIZE` bytes with 4-byte alignment
    /// (required for `AtomicU32` tower pointers).
    fn block_layout() -> std::alloc::Layout {
        // SAFETY: BLOCK_SIZE > 0 and align (4) is a power of two.
        unsafe { std::alloc::Layout::from_size_align_unchecked(BLOCK_SIZE as usize, 4) }
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let layout = Self::block_layout();
        for block in &*self.blocks {
            let ptr = block.load(Ordering::Relaxed);
            if !ptr.is_null() {
                // SAFETY: `ptr` was allocated for this arena using `block_layout()`,
                // so deallocating with the same layout is valid.
                unsafe {
                    std::alloc::dealloc(ptr, layout);
                }
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests use expect for brevity")]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
    use super::*;

    #[test]
    fn basic_alloc_and_read() {
        let arena = Arena::new();

        let off = arena.alloc(4, 4).expect("should succeed");
        assert!(off >= 1);
        assert_eq!(off & 3, 0);

        // SAFETY: freshly allocated, exclusive access.
        unsafe {
            let bytes = arena.get_bytes_mut(off, 4);
            bytes.copy_from_slice(&[1, 2, 3, 4]);
        }

        let read = unsafe { arena.get_bytes(off, 4) };
        assert_eq!(read, &[1, 2, 3, 4]);
    }

    #[test]
    fn alloc_respects_alignment() {
        let arena = Arena::new();
        let a = arena.alloc(1, 1).expect("ok");
        let b = arena.alloc(4, 4).expect("ok");
        assert_eq!(b & 3, 0);
        assert!(b > a);
    }

    #[test]
    fn alloc_crosses_block_boundary() {
        let arena = Arena::new();
        let big = BLOCK_SIZE - 64;
        let off1 = arena.alloc(big, 1).expect("ok");
        assert_eq!(off1 >> BLOCK_SHIFT, 0);

        let off2 = arena.alloc(128, 4).expect("ok");
        assert_eq!(off2 >> BLOCK_SHIFT, 1);
    }

    #[test]
    fn atomic_u32_round_trip() {
        let arena = Arena::new();
        let off = arena.alloc(4, 4).expect("ok");

        // SAFETY: freshly allocated, 4-byte aligned.
        unsafe {
            let atom = arena.get_atomic_u32(off);
            atom.store(42, Ordering::Relaxed);
            assert_eq!(atom.load(Ordering::Relaxed), 42);
        }
    }

    #[test]
    fn concurrent_alloc() {
        use std::sync::Arc;

        let arena = Arc::new(Arena::new());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let arena = Arc::clone(&arena);
                std::thread::spawn(move || {
                    let mut offsets = Vec::new();
                    for _ in 0..1000 {
                        if let Some(off) = arena.alloc(64, 4) {
                            offsets.push(off);
                        }
                    }
                    offsets
                })
            })
            .collect();

        let mut all_offsets: Vec<u32> = Vec::new();
        for h in handles {
            all_offsets.extend(h.join().expect("thread ok"));
        }

        all_offsets.sort();
        all_offsets.dedup();
        assert_eq!(all_offsets.len(), 8000);
    }

    #[test]
    fn alloc_invalid_alignment_returns_none() {
        let arena = Arena::new();
        assert!(arena.alloc(100, 3).is_none()); // 3 is not a power of two
        assert!(arena.alloc(0, 4).is_none()); // zero size
        assert!(arena.alloc(BLOCK_SIZE, 1).is_none()); // size == BLOCK_SIZE
        assert!(arena.alloc(BLOCK_SIZE + 1, 1).is_none()); // size > BLOCK_SIZE
    }

    #[test]
    fn default_impl() {
        let arena = Arena::default();
        let off = arena.alloc(8, 4).expect("should work");
        assert!(off > 0);
    }

    #[test]
    fn drop_with_multiple_blocks() {
        let arena = Arena::new();
        // Allocate across 2 blocks to exercise Drop on both.
        let big = BLOCK_SIZE - 8;
        let _ = arena.alloc(big, 1).expect("block 0");
        let _ = arena.alloc(64, 4).expect("block 1");
        // Drop runs here — deallocates both blocks.
    }

    /// Regression test for #119: when an allocation fills a block exactly
    /// to BLOCK_SIZE, the cursor OR produced `(block_idx << SHIFT) | BLOCK_SIZE`
    /// which wrapped back to offset 0 of the *same* block, causing subsequent
    /// allocations to overwrite existing data.
    ///
    /// The bug only triggers when block_idx >= 1 because for block 0
    /// `(0 << SHIFT) | BLOCK_SIZE` correctly decodes as block 1, offset 0.
    /// For block_idx >= 1 the BLOCK_SHIFT bit is already set in the block
    /// index, so the OR does not carry and the cursor wraps.
    #[test]
    fn exact_block_fill_does_not_corrupt() {
        let arena = Arena::new();

        // Jump the cursor directly to block 1, offset 0 — avoids allocating
        // an entire block 0 (64 MiB on 64-bit) just to advance past it.
        arena.cursor.store(1 << BLOCK_SHIFT, Ordering::Relaxed);

        // Allocate (BLOCK_SIZE - 4) bytes to bring block 1's cursor to
        // offset BLOCK_SIZE - 4.
        let filler = BLOCK_SIZE - 4;
        let f = arena.alloc(filler, 1).expect("filler");
        assert_eq!(f >> BLOCK_SHIFT, 1, "filler should be in block 1");

        // Write a sentinel pattern into the last allocated byte.
        // SAFETY: `f` was just returned by alloc(filler, 1), so
        // [f, f+filler) is allocated and we have exclusive access.
        unsafe {
            let bytes = arena.get_bytes_mut(f, filler);
            bytes[filler as usize - 1] = 0xAB;
        }

        // Now cursor is at BLOCK_SIZE - 4 within block 1.  Allocate exactly
        // 4 bytes (align=4): new_end = BLOCK_SIZE exactly.  With the fix,
        // this allocation moves to block 2 (the tail bytes in block 1 are
        // sacrificed).
        let boundary = arena.alloc(4, 4).expect("boundary alloc");
        assert_eq!(
            boundary >> BLOCK_SHIFT,
            2,
            "exact-fill allocation must advance to the next block"
        );

        // A further allocation must also be in block 2 (not wrap to block 1).
        let next = arena.alloc(8, 4).expect("next alloc");
        assert_eq!(
            next >> BLOCK_SHIFT,
            2,
            "subsequent allocation must stay in the advanced block"
        );

        // Verify the sentinel byte in block 1 was NOT overwritten.
        // SAFETY: `f` is the offset returned by alloc(filler, 1) above,
        // guaranteeing [f, f+filler) is allocated and initialised.
        let read_sentinel = unsafe { arena.get_bytes(f, filler) };
        assert_eq!(
            read_sentinel[filler as usize - 1],
            0xAB,
            "block 1 data must not be corrupted by subsequent allocations"
        );
    }
}
