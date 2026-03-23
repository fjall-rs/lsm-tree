// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Lock-free segmented value storage for the memtable skiplist.
//!
//! Values are stored in fixed-size segments (64 K entries each), allocated
//! lazily via `AtomicPtr` CAS.  Reads are wait-free (one atomic load +
//! pointer dereference), writes are lock-free (atomic `fetch_add` on the
//! index counter + CAS for new segment allocation).
//!
//! This replaces `Mutex<Vec<UserValue>>` which serialised all value accesses
//! and caused 15-27% throughput regression under concurrent reads.

use crate::value::UserValue;

use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

/// Number of entries per segment.  2^16 = 65 536.
const SEGMENT_SHIFT: u32 = 16;

/// Entries per segment.
const SEGMENT_SIZE: usize = 1 << SEGMENT_SHIFT;

/// Bitmask for within-segment offset.
#[expect(
    clippy::cast_possible_truncation,
    reason = "SEGMENT_SIZE = 65536, fits in u32"
)]
const SEGMENT_MASK: u32 = SEGMENT_SIZE as u32 - 1;

/// Maximum segments.  With 64 K entries/segment this supports ~4 billion entries.
const MAX_SEGMENTS: usize = 1 << (32 - SEGMENT_SHIFT); // 65 536

/// A lock-free append-only store for [`UserValue`] entries.
///
/// Entries are addressed by a u32 index returned from [`append`](Self::append).
/// Reads via [`get`](Self::get) are wait-free.  The store never shrinks —
/// it is dropped in bulk when the memtable is dropped.
pub struct ValueStore {
    /// Segment pointers.  Null = not yet allocated.  Once set, never modified.
    segments: Box<[AtomicPtr<UserValue>]>,

    /// Next index to allocate (monotonically increasing).
    next_idx: AtomicU32,
}

// Send+Sync derived automatically: all fields (Box<[AtomicPtr<_>]>, AtomicU32)
// are Send+Sync.

impl ValueStore {
    /// Creates a new empty store.
    ///
    /// Allocates a fixed-size segment-pointer array (~512 KiB on 64-bit).
    /// This is acceptable: one array per memtable, and memtables are few.
    pub fn new() -> Self {
        // Vec optimizes the repeated-null pattern into a single memset.
        // Using Box::new_zeroed_slice would be cleaner but requires nightly.
        let mut segments = Vec::with_capacity(MAX_SEGMENTS);
        for _ in 0..MAX_SEGMENTS {
            segments.push(AtomicPtr::new(ptr::null_mut()));
        }

        Self {
            segments: segments.into_boxed_slice(),
            next_idx: AtomicU32::new(0),
        }
    }

    /// Appends a value and returns its index.
    ///
    /// The value is cloned into the store (cheap for `ByteView` — atomic
    /// refcount increment only).
    ///
    /// # Panics
    ///
    /// Panics if more than `u32::MAX` values are appended (unreachable in
    /// practice — a memtable with 4 billion entries would exhaust memory
    /// long before this limit).
    #[expect(
        clippy::indexing_slicing,
        reason = "seg_idx < MAX_SEGMENTS enforced by u32 index range"
    )]
    pub fn append(&self, value: &UserValue) -> u32 {
        // Use fetch_update with checked_add to prevent wraparound past u32::MAX
        // (which would reuse indices and cause memory unsafety).
        #[expect(
            clippy::expect_used,
            reason = "a memtable with 4 billion entries would exhaust memory long before this"
        )]
        let idx = self
            .next_idx
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .expect("ValueStore::append: exceeded u32::MAX entries");
        let seg_idx = (idx >> SEGMENT_SHIFT) as usize;
        let slot = (idx & SEGMENT_MASK) as usize;

        self.ensure_segment(seg_idx);

        // SAFETY: ensure_segment guarantees the segment is allocated.
        // The atomic fetch_update guarantees `slot` is unique — no two threads
        // write the same slot.  We write before publishing the node (via the
        // skiplist CAS), so readers see the value only after it's fully
        // written.
        unsafe {
            let seg_ptr = self.segments[seg_idx].load(Ordering::Acquire);
            debug_assert!(!seg_ptr.is_null());
            ptr::write(seg_ptr.add(slot), value.clone());
        }

        idx
    }

    /// Reads a value by index (wait-free).
    ///
    /// # Safety
    ///
    /// `idx` must have been returned by a prior [`append`](Self::append) call,
    /// and the caller must establish happens-before (typically via the skiplist
    /// CAS chain) to ensure the value at `idx` has been fully written.
    #[expect(
        clippy::indexing_slicing,
        reason = "seg_idx < MAX_SEGMENTS enforced by u32 index range"
    )]
    pub unsafe fn get(&self, idx: u32) -> UserValue {
        let seg_idx = (idx >> SEGMENT_SHIFT) as usize;
        let slot = (idx & SEGMENT_MASK) as usize;

        // SAFETY: the caller guarantees happens-before via the skiplist CAS.
        // The value at `idx` was fully written during `append()`.  Acquire
        // pairs with the AcqRel CAS in ensure_segment.
        unsafe {
            let seg_ptr = self.segments[seg_idx].load(Ordering::Acquire);
            debug_assert!(!seg_ptr.is_null());
            (*seg_ptr.add(slot)).clone()
        }
    }

    /// Ensures the segment at `seg_idx` is allocated.
    #[expect(
        clippy::indexing_slicing,
        reason = "seg_idx < MAX_SEGMENTS enforced by caller"
    )]
    fn ensure_segment(&self, seg_idx: usize) {
        if self.segments[seg_idx].load(Ordering::Acquire).is_null() {
            // Allocate a segment of uninitialised UserValue slots.
            // We use alloc_zeroed for the raw memory — the slots will be
            // initialised one-by-one via ptr::write in append().
            #[expect(
                clippy::expect_used,
                reason = "Layout::array with compile-time-known size cannot fail"
            )]
            let layout =
                std::alloc::Layout::array::<UserValue>(SEGMENT_SIZE).expect("segment layout");

            // SAFETY: layout is non-zero (SEGMENT_SIZE > 0, UserValue is non-ZST).
            // The cast to *mut UserValue is safe because alloc_zeroed returns
            // memory with alignment >= align_of::<UserValue>() (Layout::array
            // sets alignment to align_of::<UserValue>()).
            #[expect(
                clippy::cast_ptr_alignment,
                reason = "Layout::array ensures correct alignment"
            )]
            let raw = unsafe { std::alloc::alloc_zeroed(layout) }.cast::<UserValue>();
            if raw.is_null() {
                std::alloc::handle_alloc_error(layout);
            }

            // CAS null → raw.  Loser frees its allocation.
            if self.segments[seg_idx]
                .compare_exchange(ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                // SAFETY: raw was just allocated with the same layout; no
                // slots were initialised (we lost the race before any append).
                unsafe {
                    std::alloc::dealloc(raw.cast::<u8>(), layout);
                }
            }
        }
    }
}

impl Default for ValueStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ValueStore {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "seg_idx < MAX_SEGMENTS (65536), fits in u32"
    )]
    fn drop(&mut self) {
        let total = self.next_idx.load(Ordering::Relaxed);
        if total == 0 {
            return;
        }

        // Only iterate segments that could contain initialised entries.
        let max_seg_idx = ((total - 1) >> SEGMENT_SHIFT) as usize + 1;

        for seg_idx in 0..max_seg_idx {
            #[expect(
                clippy::indexing_slicing,
                reason = "seg_idx < max_seg_idx <= MAX_SEGMENTS"
            )]
            let seg_ptr = self.segments[seg_idx].load(Ordering::Relaxed);

            if seg_ptr.is_null() {
                continue;
            }

            // Drop initialised slots in this segment.
            let seg_start = (seg_idx as u32) << SEGMENT_SHIFT;
            let seg_end = seg_start.saturating_add(SEGMENT_SIZE as u32).min(total);

            if seg_start < total {
                let count = (seg_end - seg_start) as usize;
                for i in 0..count {
                    // SAFETY: slots 0..count were initialised via ptr::write
                    // in append().  We're the only thread running (Drop is &mut).
                    unsafe {
                        ptr::drop_in_place(seg_ptr.add(i));
                    }
                }
            }

            // Deallocate the segment.
            #[expect(
                clippy::expect_used,
                reason = "Layout::array with compile-time-known size cannot fail"
            )]
            let layout =
                std::alloc::Layout::array::<UserValue>(SEGMENT_SIZE).expect("segment layout");
            // SAFETY: `seg_ptr` came from `alloc_zeroed(layout)` in
            // `ensure_segment()`, all initialised entries were dropped above,
            // and `Drop` has exclusive access — so this frees that allocation
            // exactly once with the original layout.
            unsafe {
                std::alloc::dealloc(seg_ptr.cast::<u8>(), layout);
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests use expect for brevity")]
mod tests {
    use super::*;

    fn val(s: &[u8]) -> UserValue {
        UserValue::from(s)
    }

    #[test]
    fn append_and_get() {
        let store = ValueStore::new();
        let i0 = store.append(&val(b"hello"));
        let i1 = store.append(&val(b"world"));

        assert_eq!(&*unsafe { store.get(i0) }, b"hello");
        assert_eq!(&*unsafe { store.get(i1) }, b"world");
    }

    #[test]
    fn empty_value() {
        let store = ValueStore::new();
        let i = store.append(&val(b""));
        assert!(unsafe { store.get(i) }.is_empty());
    }

    #[test]
    fn crosses_segment_boundary() {
        let store = ValueStore::new();

        // Fill first segment + 1
        for i in 0..=SEGMENT_SIZE {
            store.append(&val(format!("v{i}").as_bytes()));
        }

        // Last entry is in segment 1
        let last_idx = SEGMENT_SIZE as u32;
        assert_eq!(
            &*unsafe { store.get(last_idx) },
            format!("v{SEGMENT_SIZE}").as_bytes()
        );
    }

    #[test]
    fn concurrent_append_and_read() {
        use std::sync::Arc;

        let store = Arc::new(ValueStore::new());
        let n_threads = 8usize;
        let n_per_thread = 1000usize;

        // Concurrent appends.
        let handles: Vec<_> = (0..n_threads)
            .map(|t| {
                let store = Arc::clone(&store);
                std::thread::spawn(move || {
                    let mut indices = Vec::with_capacity(n_per_thread);
                    for i in 0..n_per_thread {
                        let v = format!("t{t}_v{i}");
                        indices.push((store.append(&val(v.as_bytes())), v));
                    }
                    indices
                })
            })
            .collect();

        let all: Vec<(u32, String)> = handles
            .into_iter()
            .flat_map(|h| h.join().expect("thread ok"))
            .collect();

        // Verify all values are readable and correct.
        for (idx, expected) in &all {
            assert_eq!(&*unsafe { store.get(*idx) }, expected.as_bytes());
        }

        assert_eq!(all.len(), n_threads * n_per_thread);
    }
}
