// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Arena-based concurrent skiplist for memtable storage.
//!
//! Nodes are allocated from a contiguous [`Arena`] for cache locality and O(1)
//! bulk deallocation when the memtable is dropped.  Concurrent skiplist
//! traversal is lock-free (atomic loads on next-pointers); inserts use CAS with
//! retry on tower links.  Values are stored in a lock-free segmented
//! [`ValueStore`](super::value_store::ValueStore) — reads are wait-free.
//!
//! The design follows the arena-skiplist pattern used by Pebble/CockroachDB
//! and Badger, adapted for Rust's ownership model and the lsm-tree
//! `InternalKey` ordering (`user_key` ASC, seqno DESC).

use super::arena::Arena;
use super::value_store::ValueStore;
use crate::comparator::SharedComparator;
use crate::key::InternalKey;
use crate::value::{SeqNo, UserValue};
use crate::{UserKey, ValueType};

use std::cmp::Ordering as CmpOrdering;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum tower height.  With P = 1/4 this supports ~4^20 ≈ 10^12 entries.
const MAX_HEIGHT: usize = 20;

/// Sentinel offset meaning "no node".  Offset 0 is reserved in the arena.
const UNSET: u32 = 0;

// ---------------------------------------------------------------------------
// Node layout (offsets within a node allocation)
// ---------------------------------------------------------------------------
// All multi-byte fields are stored in **native** byte order (LE on x86/ARM)
// because the arena is never persisted — it lives only in memory.
//
// +0   u32  key_offset    — offset of user_key bytes in the arena
// +4   u32  value_idx     — index into the SkipMap `ValueStore`
// +8   u16  key_len       — user_key length
// +10  u8   value_type    — ValueType discriminant
// +11  u8   height        — tower height (1..=MAX_HEIGHT)
// +12  u32  (reserved)    — padding for alignment
// +16  u64  seqno         — sequence number
// +24  [u32; height]      — tower: next-pointers per level (AtomicU32)
//
// Values are stored in a separate heap-backed Vec so that large values
// don't bloat the arena and cause exhaustion.
//
// Total: 24 + 4 × height   (always 4-byte aligned)

// Layout offsets — only OFF_HEIGHT and OFF_TOWER are used by name in code;
// the rest are accessed via array slicing in the node_*() accessors.
const OFF_HEIGHT: u32 = 11;
const OFF_TOWER: u32 = 24;

/// Byte size of a node with the given tower `height`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "height <= MAX_HEIGHT (20), always fits in u32"
)]
const fn node_size(height: usize) -> u32 {
    OFF_TOWER + (height as u32) * 4
}

// ---------------------------------------------------------------------------
// SkipMap
// ---------------------------------------------------------------------------

/// A concurrent ordered map backed by an arena-allocated skiplist.
///
/// Provides lock-free traversal and CAS-based inserts with O(log n) expected
/// time.  Values are stored in a lock-free segmented [`ValueStore`] so large
/// blobs do not bloat the arena; value reads are wait-free.  Keys are
/// [`InternalKey`] (ordered by `user_key` ascending, then seqno descending).
pub struct SkipMap {
    arena: Arena,
    /// Lock-free segmented storage for values.  Keys live in the arena for
    /// cache locality during comparisons; values live here so large blobs
    /// don't exhaust the arena.  Indexed by `value_idx` stored in each node.
    values: ValueStore,
    /// User key comparator for ordering entries.
    comparator: SharedComparator,
    /// Offset of the sentinel head node in the arena.
    head: u32,
    /// Current maximum height of any inserted node.
    height: AtomicUsize,
    /// Number of entries (not counting the head sentinel).
    len: AtomicUsize,
    /// PRNG counter for height generation (splitmix64-based).
    rng_state: AtomicU64,
}

impl SkipMap {
    /// Creates a new empty skiplist with the given user key comparator.
    ///
    /// The arena grows lazily in 4 MiB blocks — no large upfront allocation.
    pub fn new(comparator: SharedComparator) -> Self {
        let arena = Arena::new();

        // Allocate the head sentinel with MAX_HEIGHT.
        let head_size = node_size(MAX_HEIGHT);
        #[expect(
            clippy::expect_used,
            reason = "arena capacity is a fixed configuration; exhaustion is fatal"
        )]
        let head = arena
            .alloc(head_size, 4)
            .expect("arena must fit at least the head sentinel");

        // Head is zero-initialised by the arena; set the height byte.
        // SAFETY: head was just allocated with size head_size >= OFF_HEIGHT+1;
        // we have exclusive access because no other thread can see this arena yet.
        unsafe {
            let bytes = arena.get_bytes_mut(head, head_size);
            #[expect(
                clippy::indexing_slicing,
                reason = "OFF_HEIGHT (11) < head_size (104) by construction"
            )]
            {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "MAX_HEIGHT = 20, fits in u8"
                )]
                {
                    bytes[OFF_HEIGHT as usize] = MAX_HEIGHT as u8;
                }
            }
        }

        // Seed PRNG with an address-derived non-zero value.
        let seed = {
            let p = (&raw const arena) as u64;
            if p == 0 {
                0xDEAD_BEEF
            } else {
                p
            }
        };

        Self {
            arena,
            values: ValueStore::new(),
            comparator,
            head,
            height: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
            rng_state: AtomicU64::new(seed),
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Inserts a key-value pair into the skiplist.
    ///
    /// Multiple entries with the same `user_key` but different `seqno` are
    /// expected (MVCC).  No deduplication is performed.
    #[expect(
        clippy::indexing_slicing,
        reason = "preds/succs are [u32; MAX_HEIGHT]; level < height <= MAX_HEIGHT"
    )]
    pub fn insert(&self, key: &InternalKey, value: &UserValue) {
        let height = self.random_height();
        let node = self.alloc_node(key, value, height);

        // Raise the list height if needed.
        let mut list_h = self.height.load(Ordering::Relaxed);
        while height > list_h {
            match self.height.compare_exchange_weak(
                list_h,
                height,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(h) => list_h = h,
            }
        }

        // Find predecessors and link the node at each level.
        let mut preds = [self.head; MAX_HEIGHT];
        let mut succs = [UNSET; MAX_HEIGHT];
        self.find_splice(key, &mut preds, &mut succs);

        for level in 0..height {
            loop {
                // SAFETY: `node` was allocated with `height` levels and
                // `level < height`, so `tower_atomic(node, level)` is within
                // the node's arena allocation.
                // new_node.next[level] = succs[level]
                unsafe {
                    self.tower_atomic(node, level)
                        .store(succs[level], Ordering::Release);
                }

                // SAFETY: `preds[level]` is a valid node established by
                // `find_splice` — either the head sentinel (MAX_HEIGHT levels)
                // or a previously inserted node with height > level.
                // CAS pred.next[level] from succs[level] to new_node
                let pred_next = unsafe { self.tower_atomic(preds[level], level) };
                match pred_next.compare_exchange_weak(
                    succs[level],
                    node,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(_) => {
                        // Predecessor changed — re-search at this level.
                        self.find_splice_for_level(key, &mut preds, &mut succs, level);
                    }
                }
            }
        }

        self.len.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Returns `true` if the skiplist is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an iterator over all entries in order.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            map: self,
            front: self.first_node(),
            back: UNSET,
            back_init: false,
            done: false,
        }
    }

    /// Returns an iterator over entries within the given range.
    pub fn range<R: RangeBounds<InternalKey>>(&self, range: R) -> Range<'_> {
        let front = match range.start_bound() {
            Bound::Included(k) => self.seek_ge(k),
            Bound::Excluded(k) => self.seek_gt(k),
            Bound::Unbounded => self.first_node(),
        };

        let end_bound = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Range {
            map: self,
            end_bound,
            front,
            back: UNSET,
            back_init: false,
            done: false,
        }
    }

    // -----------------------------------------------------------------------
    // Internal: node allocation
    // -----------------------------------------------------------------------

    /// Allocates and initialises a node in the arena, returning its offset.
    ///
    /// Key data is stored in the arena for comparison locality.
    /// Value data is appended to the heap-backed `values` Vec.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "key_bytes.len() <= u16::MAX, value idx <= u32::MAX, height <= MAX_HEIGHT (20)"
    )]
    fn alloc_node(&self, key: &InternalKey, value: &UserValue, height: usize) -> u32 {
        let key_bytes: &[u8] = &key.user_key;

        // Allocate key data in the arena.
        #[expect(
            clippy::expect_used,
            reason = "arena capacity is fixed; exhaustion is fatal"
        )]
        let key_offset = self
            .arena
            .alloc(key_bytes.len() as u32, 1)
            .expect("arena exhausted (key data)");
        // SAFETY: key_offset was just allocated with size key_bytes.len();
        // exclusive access before publish.
        unsafe {
            self.arena
                .get_bytes_mut(key_offset, key_bytes.len() as u32)
                .copy_from_slice(key_bytes);
        }

        // Store value in the lock-free segmented store.
        let value_idx = self.values.append(value);

        // Allocate the node header + tower.
        let n_size = node_size(height);
        #[expect(
            clippy::expect_used,
            reason = "arena capacity is fixed; exhaustion is fatal"
        )]
        let node = self.arena.alloc(n_size, 4).expect("arena exhausted (node)");

        // Write immutable metadata using direct byte offsets matching the
        // node layout comment above.  The arena guarantees 24+ bytes at `node`.
        //
        // SAFETY: node was just allocated with size >= OFF_TOWER (24 bytes);
        // exclusive access before publish.
        #[expect(
            clippy::indexing_slicing,
            reason = "meta is exactly OFF_TOWER (24) bytes by construction"
        )]
        unsafe {
            let meta = self.arena.get_bytes_mut(node, OFF_TOWER);
            meta[0..4].copy_from_slice(&key_offset.to_ne_bytes());
            meta[4..8].copy_from_slice(&value_idx.to_ne_bytes());
            // Cast is safe: InternalKey::new() asserts key.len() <= u16::MAX.
            meta[8..10].copy_from_slice(&(key_bytes.len() as u16).to_ne_bytes());
            meta[10] = u8::from(key.value_type);
            meta[11] = height as u8;
            // meta[12..16] reserved padding
            meta[16..24].copy_from_slice(&key.seqno.to_ne_bytes());
            // Tower entries are already zero (= UNSET) from arena zero-init.
        }

        node
    }

    // -----------------------------------------------------------------------
    // Internal: reading node fields
    // -----------------------------------------------------------------------

    /// Reads the immutable metadata header of a node (24 bytes at `node`).
    ///
    /// # Safety
    ///
    /// `node` must be a valid node offset previously returned by `alloc_node`.
    unsafe fn meta(&self, node: u32) -> &[u8] {
        self.arena.get_bytes(node, OFF_TOWER)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: 4-byte slice always converts to [u8; 4]"
    )]
    fn node_key_offset(&self, node: u32) -> u32 {
        let m = unsafe { self.meta(node) };
        u32::from_ne_bytes(m[0..4].try_into().expect("4 bytes"))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: 2-byte slice always converts to [u8; 2]"
    )]
    fn node_key_len(&self, node: u32) -> u16 {
        let m = unsafe { self.meta(node) };
        u16::from_ne_bytes(m[8..10].try_into().expect("2 bytes"))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "ValueType discriminant written during alloc_node is always valid"
    )]
    fn node_value_type(&self, node: u32) -> ValueType {
        let m = unsafe { self.meta(node) };
        let byte = m[10];
        debug_assert!(
            byte <= 4,
            "invalid ValueType byte {byte} at node offset {node}, meta={m:?}",
        );
        ValueType::try_from(byte).expect("valid ValueType discriminant")
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: 4-byte slice always converts to [u8; 4]"
    )]
    fn node_value_idx(&self, node: u32) -> u32 {
        let m = unsafe { self.meta(node) };
        u32::from_ne_bytes(m[4..8].try_into().expect("4 bytes"))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: 8-byte slice always converts to [u8; 8]"
    )]
    fn node_seqno(&self, node: u32) -> SeqNo {
        let m = unsafe { self.meta(node) };
        u64::from_ne_bytes(m[16..24].try_into().expect("8 bytes"))
    }

    /// Returns the raw `user_key` bytes stored in the arena for `node`.
    fn node_user_key_bytes(&self, node: u32) -> &[u8] {
        let off = self.node_key_offset(node);
        let len = u32::from(self.node_key_len(node));
        // SAFETY: `node` is reachable via skiplist links only after publication
        // (CAS with Release), so its metadata (key_offset, key_len) was fully
        // written during alloc_node.  The arena block backing off..off+len is
        // never freed while the SkipMap lives.
        unsafe { self.arena.get_bytes(off, len) }
    }

    /// Reconstructs the [`InternalKey`] for `node` (allocates a new `Slice`).
    fn node_internal_key(&self, node: u32) -> InternalKey {
        let user_key: UserKey = self.node_user_key_bytes(node).into();
        let seqno = self.node_seqno(node);
        let vt = self.node_value_type(node);
        InternalKey {
            user_key,
            seqno,
            value_type: vt,
        }
    }

    /// Reads the value for `node` from the lock-free value store (wait-free).
    fn node_value(&self, node: u32) -> UserValue {
        // SAFETY: node_value_idx was set during alloc_node→ValueStore::append,
        // and this node is only reachable after the skiplist CAS that published
        // it (establishing happens-before for the value write).
        unsafe { self.values.get(self.node_value_idx(node)) }
    }

    // -----------------------------------------------------------------------
    // Internal: tower access
    // -----------------------------------------------------------------------

    /// Returns a reference to the `AtomicU32` next-pointer at `level` for `node`.
    ///
    /// # Safety
    ///
    /// `level` must be < the node's height.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "level < MAX_HEIGHT (20), fits in u32"
    )]
    unsafe fn tower_atomic(&self, node: u32, level: usize) -> &std::sync::atomic::AtomicU32 {
        // SAFETY: caller guarantees level < node height; node + OFF_TOWER + level*4
        // is within the node's arena allocation and 4-byte aligned.
        self.arena
            .get_atomic_u32(node + OFF_TOWER + (level as u32) * 4)
    }

    /// Loads the next-pointer at `level` for `node`.
    /// Returns UNSET (0) if no next node.
    fn next_at(&self, node: u32, level: usize) -> u32 {
        // SAFETY: next_at is only called with levels within the node's height
        // or the head sentinel's MAX_HEIGHT.
        unsafe { self.tower_atomic(node, level).load(Ordering::Acquire) }
    }

    /// The first data node (head.next[0]), or UNSET if empty.
    fn first_node(&self) -> u32 {
        self.next_at(self.head, 0)
    }

    // -----------------------------------------------------------------------
    // Internal: key comparison
    // -----------------------------------------------------------------------

    /// Compares the key stored at `node` with `target` using the pluggable
    /// `UserComparator` for `user_key` ordering, then seqno DESC.
    fn compare_key(&self, node: u32, target: &InternalKey) -> CmpOrdering {
        let node_uk = self.node_user_key_bytes(node);
        let target_uk: &[u8] = &target.user_key;

        match self.comparator.compare(node_uk, target_uk) {
            CmpOrdering::Equal => {
                // Reverse seqno: higher seqno sorts first.
                let node_seq = self.node_seqno(node);
                target.seqno.cmp(&node_seq)
            }
            other => other,
        }
    }

    /// Compares two nodes by key without allocating (reads raw arena bytes).
    ///
    /// Ordering: `(user_key via comparator, Reverse(seqno))`.  `value_type` is
    /// intentionally excluded — it is not part of [`InternalKey::Ord`] or
    /// [`InternalKey::compare_with`], and `(user_key, seqno)` is unique per entry.
    fn compare_nodes(&self, a: u32, b: u32) -> CmpOrdering {
        let a_uk = self.node_user_key_bytes(a);
        let b_uk = self.node_user_key_bytes(b);
        match self.comparator.compare(a_uk, b_uk) {
            CmpOrdering::Equal => {
                let a_seq = self.node_seqno(a);
                let b_seq = self.node_seqno(b);
                b_seq.cmp(&a_seq) // reverse seqno
            }
            other => other,
        }
    }

    // -----------------------------------------------------------------------
    // Internal: search helpers
    // -----------------------------------------------------------------------

    /// Populates `preds` and `succs` arrays with the splice point for `key`.
    #[expect(clippy::indexing_slicing, reason = "level < list_h <= MAX_HEIGHT")]
    fn find_splice(
        &self,
        key: &InternalKey,
        preds: &mut [u32; MAX_HEIGHT],
        succs: &mut [u32; MAX_HEIGHT],
    ) {
        let list_h = self.height.load(Ordering::Acquire);
        let mut node = self.head;

        for level in (0..list_h).rev() {
            // Track the successor from the comparison loop — do NOT re-read
            // from the list, as a concurrent insert could return a node that
            // sorts before our key, leading to an out-of-order CAS.
            let mut next = self.next_at(node, level);
            while next != UNSET && self.compare_key(next, key) == CmpOrdering::Less {
                node = next;
                next = self.next_at(node, level);
            }
            preds[level] = node;
            succs[level] = next;
        }
    }

    /// Re-searches at a single `level` starting from the stored predecessor
    /// (or a higher-level predecessor as fallback).
    #[expect(
        clippy::indexing_slicing,
        reason = "level < MAX_HEIGHT; preds/succs are [u32; MAX_HEIGHT]"
    )]
    fn find_splice_for_level(
        &self,
        key: &InternalKey,
        preds: &mut [u32; MAX_HEIGHT],
        succs: &mut [u32; MAX_HEIGHT],
        level: usize,
    ) {
        // Re-search from the head sentinel (which has MAX_HEIGHT levels).
        // We cannot start from preds[level+1] because that node's tower
        // height may be only level+2, making higher-level reads OOB.
        // Starting from head is safe and still O(log n) via the walk-down.
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        // Walk down from the list height, narrowing the search at each level.
        // Every node reached via next_at(node, lv) was linked at level lv,
        // so its height > lv — tower reads are always in-bounds.
        for lv in (level + 1..list_h).rev() {
            let mut next = self.next_at(node, lv);
            while next != UNSET && self.compare_key(next, key) == CmpOrdering::Less {
                node = next;
                next = self.next_at(node, lv);
            }
        }

        // Final search at the target level.
        let mut next = self.next_at(node, level);
        while next != UNSET && self.compare_key(next, key) == CmpOrdering::Less {
            node = next;
            next = self.next_at(node, level);
        }

        preds[level] = node;
        succs[level] = next;
    }

    /// Finds the first node whose key >= `target`, or UNSET.
    fn seek_ge(&self, target: &InternalKey) -> u32 {
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                let next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                if self.compare_key(next, target) == CmpOrdering::Less {
                    node = next;
                } else {
                    break;
                }
            }
        }

        self.next_at(node, 0)
    }

    /// Finds the first node whose key > `target`, or UNSET.
    fn seek_gt(&self, target: &InternalKey) -> u32 {
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                let next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                if self.compare_key(next, target) == CmpOrdering::Greater {
                    break;
                }
                node = next;
            }
        }

        self.next_at(node, 0)
    }

    /// Finds the last node whose key <= `target`, or UNSET if all nodes > target.
    fn seek_le(&self, target: &InternalKey) -> u32 {
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                let next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                if self.compare_key(next, target) == CmpOrdering::Greater {
                    break;
                }
                node = next;
            }
        }

        if node == self.head {
            UNSET
        } else {
            node
        }
    }

    /// Finds the last node whose key < `target`, or UNSET.
    fn seek_lt(&self, target: &InternalKey) -> u32 {
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                let next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                if self.compare_key(next, target) == CmpOrdering::Less {
                    node = next;
                } else {
                    break;
                }
            }
        }

        if node == self.head {
            UNSET
        } else {
            node
        }
    }

    /// Returns the last node in the skiplist, or UNSET if empty.
    fn last_node(&self) -> u32 {
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                let next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                node = next;
            }
        }

        if node == self.head {
            UNSET
        } else {
            node
        }
    }

    /// Finds the predecessor of `target_node` at level 0 using a top-down
    /// search.  Returns UNSET if `target_node` is the first data node.
    ///
    /// This is O(log n) — used only for `next_back()` which is called
    /// infrequently on memtable iterators.
    fn find_predecessor(&self, target_node: u32) -> u32 {
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                let next = self.next_at(node, level);
                if next == UNSET || next == target_node {
                    break;
                }
                // Compare without allocating InternalKey — reads arena bytes directly.
                if self.compare_nodes(next, target_node) == CmpOrdering::Less {
                    node = next;
                } else {
                    break;
                }
            }
        }

        // At level 0, walk forward until we find the node whose next IS
        // target_node (handles equal-key adjacency).
        loop {
            let next = self.next_at(node, 0);
            if next == UNSET || next == target_node {
                break;
            }
            if self.compare_nodes(next, target_node) == CmpOrdering::Less {
                node = next;
            } else {
                break;
            }
        }

        if node == self.head {
            UNSET
        } else {
            node
        }
    }

    // -----------------------------------------------------------------------
    // Internal: random height
    // -----------------------------------------------------------------------

    /// Generates a random tower height using a geometric distribution (P = 1/4).
    fn random_height(&self) -> usize {
        // Each thread gets a unique seed from fetch_add, then we hash it.
        let state = self.rng_state.fetch_add(1, Ordering::Relaxed);

        // splitmix64 finaliser for good bit mixing
        let mut z = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;

        // Count pairs of trailing zero bits → geometric(P=1/4)
        let tz = z.trailing_zeros() as usize;
        // Each pair of trailing zero bits adds one level
        (1 + tz / 2).min(MAX_HEIGHT)
    }
}

// ---------------------------------------------------------------------------
// Entry reference
// ---------------------------------------------------------------------------

/// A reference to a key-value pair stored in the skiplist arena.
pub struct Entry<'a> {
    map: &'a SkipMap,
    node: u32,
}

impl Entry<'_> {
    /// Reconstructs the [`InternalKey`] (allocates a new `Slice` for `user_key`).
    pub fn key(&self) -> InternalKey {
        self.map.node_internal_key(self.node)
    }

    /// Returns a borrowed reference to the raw `user_key` bytes stored in
    /// the arena.  This is cheaper than [`key()`](Self::key) when only the
    /// `user_key` is needed (avoids allocating a new `Slice`).
    pub fn user_key_bytes(&self) -> &[u8] {
        self.map.node_user_key_bytes(self.node)
    }

    /// Reconstructs the value (allocates a new `Slice`).
    pub fn value(&self) -> UserValue {
        self.map.node_value(self.node)
    }
}

// ---------------------------------------------------------------------------
// Full iterator
// ---------------------------------------------------------------------------

/// Forward + backward iterator over all entries in a [`SkipMap`].
pub struct Iter<'a> {
    map: &'a SkipMap,
    front: u32,
    back: u32,
    back_init: bool,
    done: bool,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.front == UNSET {
            return None;
        }

        let node = self.front;

        // If front and back have converged, this is the last element.
        if self.back_init && node == self.back {
            self.done = true;
        } else {
            self.front = self.map.next_at(node, 0);
            if self.front == UNSET {
                self.done = true;
            }
        }

        Some(Entry {
            map: self.map,
            node,
        })
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if !self.back_init {
            self.back = self.map.last_node();
            self.back_init = true;
        }

        if self.back == UNSET {
            self.done = true;
            return None;
        }

        let node = self.back;

        // If front and back have converged, this is the last element.
        if node == self.front {
            self.done = true;
        } else {
            self.back = self.map.find_predecessor(node);
        }

        Some(Entry {
            map: self.map,
            node,
        })
    }
}

// ---------------------------------------------------------------------------
// Range iterator
// ---------------------------------------------------------------------------

/// Forward + backward iterator over a range of entries in a [`SkipMap`].
pub struct Range<'a> {
    map: &'a SkipMap,
    end_bound: Bound<InternalKey>,
    front: u32,
    back: u32,
    back_init: bool,
    done: bool,
}

impl Range<'_> {
    /// Returns `true` if `node` is within the end bound.
    fn within_end(&self, node: u32) -> bool {
        match &self.end_bound {
            Bound::Unbounded => true,
            Bound::Included(k) => self.map.compare_key(node, k) != CmpOrdering::Greater,
            Bound::Excluded(k) => self.map.compare_key(node, k) == CmpOrdering::Less,
        }
    }
}

impl<'a> Iterator for Range<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.front == UNSET {
            return None;
        }

        let node = self.front;

        // Check end bound.
        if !self.within_end(node) {
            self.front = UNSET;
            self.done = true;
            return None;
        }

        // If front and back have converged, this is the last element.
        if self.back_init && node == self.back {
            self.done = true;
        } else {
            self.front = self.map.next_at(node, 0);
            if self.front == UNSET {
                self.done = true;
            }
        }

        Some(Entry {
            map: self.map,
            node,
        })
    }
}

impl DoubleEndedIterator for Range<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if !self.back_init {
            self.back = match &self.end_bound {
                Bound::Unbounded => self.map.last_node(),
                Bound::Included(k) => self.map.seek_le(k),
                Bound::Excluded(k) => self.map.seek_lt(k),
            };
            self.back_init = true;
        }

        if self.back == UNSET || self.front == UNSET {
            self.done = true;
            return None;
        }

        // If back is before front in key order, the range is empty
        // (e.g., start bound > end bound).
        if self.map.compare_nodes(self.back, self.front) == CmpOrdering::Less {
            self.done = true;
            return None;
        }

        let node = self.back;

        // If front and back have converged, this is the last element.
        if node == self.front {
            self.done = true;
        } else {
            self.back = self.map.find_predecessor(node);
        }

        Some(Entry {
            map: self.map,
            node,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::expect_used,
    reason = "tests use unwrap/indexing/expect for brevity"
)]
mod tests {
    use super::*;
    use crate::ValueType;

    fn new_map() -> SkipMap {
        SkipMap::new(crate::comparator::default_comparator())
    }

    fn make_key(user_key: &[u8], seqno: SeqNo) -> InternalKey {
        InternalKey::new(user_key.to_vec(), seqno, ValueType::Value)
    }

    fn make_value(data: &[u8]) -> UserValue {
        UserValue::from(data)
    }

    #[test]
    fn insert_and_get_single() {
        let map = new_map();
        let key = make_key(b"hello", 1);
        let val = make_value(b"world");
        map.insert(&key, &val);

        assert_eq!(map.len(), 1);
        assert!(!map.is_empty());

        let mut iter = map.iter();
        let entry = iter.next().expect("one entry");
        assert_eq!(&*entry.key().user_key, b"hello");
        assert_eq!(entry.key().seqno, 1);
        assert_eq!(&*entry.value(), b"world");
        assert!(iter.next().is_none());
    }

    #[test]
    fn ordering_user_key_asc_seqno_desc() {
        let map = new_map();

        // Same user_key, different seqnos → should iterate highest seqno first.
        map.insert(&make_key(b"abc", 1), &make_value(b"v1"));
        map.insert(&make_key(b"abc", 3), &make_value(b"v3"));
        map.insert(&make_key(b"abc", 2), &make_value(b"v2"));

        let seqnos: Vec<SeqNo> = map.iter().map(|e| e.key().seqno).collect();
        assert_eq!(seqnos, vec![3, 2, 1]);

        // Different user_keys → ascending.
        map.insert(&make_key(b"zzz", 10), &make_value(b"z"));
        map.insert(&make_key(b"aaa", 10), &make_value(b"a"));

        let keys: Vec<Vec<u8>> = map.iter().map(|e| e.key().user_key.to_vec()).collect();
        assert_eq!(
            keys,
            vec![
                b"aaa".to_vec(),
                b"abc".to_vec(),
                b"abc".to_vec(),
                b"abc".to_vec(),
                b"zzz".to_vec(),
            ]
        );
    }

    #[test]
    fn range_lower_bound() {
        let map = new_map();
        for i in 0u8..10 {
            let key = vec![b'a' + i];
            map.insert(&make_key(&key, 0), &make_value(&[i]));
        }

        // Range from 'e' onwards → e, f, g, h, i, j
        let bound = make_key(b"e", crate::MAX_SEQNO);
        let keys: Vec<u8> = map.range(bound..).map(|e| e.key().user_key[0]).collect();
        assert_eq!(keys, vec![b'e', b'f', b'g', b'h', b'i', b'j']);
    }

    #[test]
    fn range_bounded() {
        let map = new_map();
        for i in 0u8..10 {
            let key = vec![b'a' + i];
            map.insert(&make_key(&key, 0), &make_value(&[i]));
        }

        let lo = make_key(b"c", crate::MAX_SEQNO);
        let hi = make_key(b"f", 0);
        let keys: Vec<u8> = map.range(lo..=hi).map(|e| e.key().user_key[0]).collect();
        assert_eq!(keys, vec![b'c', b'd', b'e', b'f']);
    }

    #[test]
    fn double_ended_iter() {
        let map = new_map();
        for i in 0u8..5 {
            let key = vec![b'a' + i];
            map.insert(&make_key(&key, 0), &make_value(&[i]));
        }

        let mut iter = map.iter();
        assert_eq!(iter.next().unwrap().key().user_key[0], b'a');
        assert_eq!(iter.next_back().unwrap().key().user_key[0], b'e');
        assert_eq!(iter.next().unwrap().key().user_key[0], b'b');
        assert_eq!(iter.next_back().unwrap().key().user_key[0], b'd');
        assert_eq!(iter.next().unwrap().key().user_key[0], b'c');
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn double_ended_range() {
        let map = new_map();
        for i in 0u8..10 {
            let key = vec![b'a' + i];
            map.insert(&make_key(&key, 0), &make_value(&[i]));
        }

        let lo = make_key(b"c", crate::MAX_SEQNO);
        let hi = make_key(b"g", 0);
        let rev: Vec<u8> = map
            .range(lo..=hi)
            .rev()
            .map(|e| e.key().user_key[0])
            .collect();
        assert_eq!(rev, vec![b'g', b'f', b'e', b'd', b'c']);
    }

    #[test]
    fn empty_value() {
        let map = new_map();
        map.insert(&make_key(b"k", 0), &make_value(b""));
        let entry = map.iter().next().unwrap();
        assert!(entry.value().is_empty());
    }

    #[test]
    fn concurrent_inserts() {
        use std::sync::Arc;

        let map = Arc::new(new_map());
        let n_threads = 8;
        let n_per_thread = 1000;

        let handles: Vec<_> = (0..n_threads)
            .map(|t| {
                let map = Arc::clone(&map);
                std::thread::spawn(move || {
                    for i in 0..n_per_thread {
                        let key = format!("t{t:02}_k{i:05}");
                        map.insert(&make_key(key.as_bytes(), i as u64), &make_value(b"v"));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }

        assert_eq!(map.len(), n_threads * n_per_thread);

        // Verify sorted order.
        let entries: Vec<_> = map.iter().collect();
        for pair in entries.windows(2) {
            let a = pair[0].key();
            let b = pair[1].key();
            assert!(a <= b, "out of order: {a:?} > {b:?}");
        }
    }

    #[test]
    fn mvcc_point_lookup_via_range() {
        let map = new_map();

        // Insert 3 versions of "key" at seqnos 1, 2, 3.
        map.insert(&make_key(b"key", 1), &make_value(b"v1"));
        map.insert(&make_key(b"key", 2), &make_value(b"v2"));
        map.insert(&make_key(b"key", 3), &make_value(b"v3"));

        // Memtable MVCC read at read_seqno=3 (visible: seqno <= 2).
        // The memtable uses lower_bound = InternalKey("key", read_seqno - 1).
        // With InternalKey ordering (user_key ASC, seqno DESC), range(("key", 2)..)
        // yields entries starting from seqno=2 downward.
        let lower = InternalKey::new(b"key".to_vec(), 2, ValueType::Value);
        let mut iter = map.range(lower..);
        let entry = iter
            .next()
            .filter(|e| &*e.key().user_key == b"key")
            .expect("should find key");
        assert_eq!(entry.key().seqno, 2);
        assert_eq!(&*entry.value(), b"v2");

        // At read_seqno=2, lower_bound = ("key", 1), yields seqno=1.
        let lower2 = InternalKey::new(b"key".to_vec(), 1, ValueType::Value);
        let entry2 = map
            .range(lower2..)
            .next()
            .filter(|e| &*e.key().user_key == b"key")
            .expect("should find key");
        assert_eq!(entry2.key().seqno, 1);
        assert_eq!(&*entry2.value(), b"v1");

        // At read_seqno=crate::MAX_SEQNO, lower_bound = ("key", MAX-1), yields seqno=3 (latest).
        let lower3 = InternalKey::new(b"key".to_vec(), crate::MAX_SEQNO - 1, ValueType::Value);
        let entry3 = map
            .range(lower3..)
            .next()
            .filter(|e| &*e.key().user_key == b"key")
            .expect("should find key");
        assert_eq!(entry3.key().seqno, 3);
        assert_eq!(&*entry3.value(), b"v3");
    }

    #[test]
    fn empty_iter_next_back() {
        let map = new_map();
        let mut iter = map.iter();
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn empty_range_next_back() {
        let map = new_map();
        let lo = make_key(b"a", crate::MAX_SEQNO);
        let hi = make_key(b"z", 0);
        let mut range = map.range(lo..=hi);
        assert!(range.next().is_none());
        assert!(range.next_back().is_none());
    }

    #[test]
    fn range_excluded_end_next_back() {
        let map = new_map();
        for i in 0u8..5 {
            map.insert(&make_key(&[b'a' + i], 0), &make_value(&[i]));
        }

        // Excluded end "d" → range is [a, d) = a, b, c
        let lo = make_key(b"a", crate::MAX_SEQNO);
        let hi = make_key(b"d", crate::MAX_SEQNO);
        let rev: Vec<u8> = map
            .range(lo..hi)
            .rev()
            .map(|e| e.key().user_key[0])
            .collect();
        assert_eq!(rev, vec![b'c', b'b', b'a']);
    }

    #[test]
    fn seek_le_all_greater_returns_none() {
        let map = new_map();
        map.insert(&make_key(b"m", 0), &make_value(b"v"));

        // All keys > "a", so seek_le("a") returns UNSET → next_back = None
        let hi = make_key(b"a", 0);
        let mut range = map.range(..=hi);
        assert!(range.next_back().is_none());
    }

    #[test]
    fn next_back_on_first_element() {
        let map = new_map();
        map.insert(&make_key(b"only", 0), &make_value(b"v"));

        let mut iter = map.iter();
        // next_back on single-element list
        let entry = iter.next_back().expect("one entry");
        assert_eq!(&*entry.key().user_key, b"only");
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    /// Regression test for SIGBUS on aarch64: concurrent inserts + reads
    /// caused misaligned AtomicU32 access when a skiplist next-pointer
    /// contained a key-data offset (align=1) instead of a node offset
    /// (align=4).
    ///
    /// The test stresses concurrent insert + iteration to surface the
    /// race.  Prior to the fix, this would SIGBUS on Apple Silicon.
    #[test]
    fn concurrent_insert_and_iter_no_sigbus() {
        use std::sync::{Arc, Barrier};

        let map = Arc::new(new_map());
        let barrier = Arc::new(Barrier::new(9)); // 8 writers + 1 reader

        // 8 writer threads
        let writers: Vec<_> = (0..8)
            .map(|t| {
                let map = Arc::clone(&map);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..500 {
                        let key = format!("t{t:02}_k{i:04}");
                        map.insert(&make_key(key.as_bytes(), i as u64), &make_value(b"val"));
                    }
                })
            })
            .collect();

        // 1 reader thread doing concurrent iteration
        let reader = {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let mut count = 0u64;
                for _ in 0..100 {
                    // Iterate the skiplist while writers are active.
                    // This exercises tower_atomic / next_at on every node.
                    for entry in map.iter() {
                        let _ = entry.key();
                        let _ = entry.value();
                        count += 1;
                    }
                }
                count
            })
        };

        for w in writers {
            w.join().expect("writer panicked");
        }
        let reads = reader.join().expect("reader panicked");

        // Sanity: all entries inserted
        assert_eq!(map.len(), 4000);
        // Reader count may be 0 if writers finished before reader iterated.
        // The key assertion is that no SIGBUS/panic occurred during iteration.
        let _ = reads;
    }
}
