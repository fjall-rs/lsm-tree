// Copyright (c) 2024-present, fjall-rs 
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#![allow(unsafe_code)]

use std::{
    alloc::Layout,
    borrow::Borrow,
    hash::Hash,
    marker::PhantomData,
    mem::{offset_of, ManuallyDrop},
    ops::{Bound, RangeBounds},
    sync::{
        atomic::{AtomicPtr, AtomicU32, AtomicUsize, Ordering},
        LazyLock,
    },
};

use super::arena::Arenas;

/// A SkipMap is a concurrent mapping structure like a BTreeMap
/// but it allows for concurrent reads and writes. A tradeoff
/// is that it does not allow for updates or deletions.
pub struct SkipMap<K, V> {
    arena: ArenasAllocator<K, V>,

    head: BoundaryNode<K, V>,
    tail: BoundaryNode<K, V>,

    seed: AtomicU32,
    height: AtomicUsize,
    len: AtomicUsize,
}

impl<K, V> Default for SkipMap<K, V> {
    fn default() -> Self {
        const DEFAULT_SEED: u32 = 1; // arbitrary
        Self::new(DEFAULT_SEED)
    }
}

impl<K, V> SkipMap<K, V> {
    /// New constructs a new `[SkipMap]`.
    pub fn new(seed: u32) -> Self {
        let arena = ArenasAllocator::default();
        let head = arena.alloc(MAX_HEIGHT);
        let head = NodePtr::new(head).unwrap();
        let tail = arena.alloc(MAX_HEIGHT);
        let tail = NodePtr::new(tail).unwrap();
        for i in 0..MAX_HEIGHT {
            head.init_next(i, tail);
            tail.init_prev(i, head);
        }
        Self {
            arena,
            head: BoundaryNode::new(head),
            tail: BoundaryNode::new(tail),
            seed: AtomicU32::new(seed),
            height: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
        }
    }

    /// Iter constructs an iterator over the complete
    /// range.
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter::new(self)
    }
}

impl<K, V> SkipMap<K, V>
where
    K: Ord,
{
    /// Insert a key-value pair into the SkipMap. Returns true
    /// if the entry was inserted.
    pub fn insert(&self, k: K, v: V) -> Result<(), (K, V)> {
        let Some(splices) = self.seek_splices(&k) else {
            return Err((k, v));
        };
        let (node, height) = self.new_node(k, v);
        for level in 0..height {
            let mut splice = match splices[level].clone() {
                Some(splice) => splice,
                // This node increased the height.
                None => Splice {
                    prev: self.head.load(),
                    next: self.tail.load(),
                },
            };

            loop {
                let Splice { next, prev } = splice;
                // +----------------+     +------------+     +----------------+
                // |      prev      |     |     nd     |     |      next      |
                // | prevNextOffset |---->|            |     |                |
                // |                |<----| prevOffset |     |                |
                // |                |     | nextOffset |---->|                |
                // |                |     |            |<----| nextPrevOffset |
                // +----------------+     +------------+     +----------------+
                //
                // 1. Initialize prevOffset and nextOffset to point to prev and next.
                // 2. CAS prevNextOffset to repoint from next to nd.
                // 3. CAS nextPrevOffset to repoint from prev to nd.
                node.init_prev(level, prev);
                node.init_next(level, next);

                // Check whether next has an updated link to prev. If it does not,
                // that can mean one of two things:
                //   1. The thread that added the next node hasn't yet had a chance
                //      to add the prev link (but will shortly).
                //   2. Another thread has added a new node between prev and next.
                let next_prev = next.load_prev(level).unwrap();
                if next_prev != prev {
                    // Determine whether #1 or #2 is true by checking whether prev
                    // is still pointing to next. As long as the atomic operations
                    // have at least acquire/release semantics (no need for
                    // sequential consistency), this works, as it is equivalent to
                    // the "publication safety" pattern.
                    let prev_next = prev.load_next(level).unwrap();
                    if prev_next == next {
                        let _ = next.cas_prev(level, next_prev, prev);
                    }
                }

                if prev.cas_next(level, next, node).is_ok() {
                    // Either we succeed, or somebody else fixed up our link above.
                    let _ = next.cas_prev(level, prev, node);
                    break;
                };

                splice = match self.find_splice_for_level(node.key(), level, prev) {
                    SpliceOrMatch::Splice(splice) => splice,
                    SpliceOrMatch::Match(_non_null) => {
                        if level == 0 {
                            // This means we encountered a race with somebody
                            // else to insert the same key. In that case, we
                            // fail on the insert but we need to make sure that
                            // K and V get returned to the caller so they aren't
                            // leaked. However, it's worth noting that in this
                            // scenario, we have wasted this node object.
                            let NodeData { key, value } =
                                unsafe { ManuallyDrop::take(&mut (*node.0).data) };
                            return Err((key, value));
                        } else {
                            // This shouldn't be possible because we go from level 0
                            // up the tower. If some other insert of the same key
                            // succeeded we should have found it and bailed.
                            panic!("concurrent insert of identical key")
                        }
                    }
                }
            }
        }
        self.len.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Range constructs an iterator over a range of the
    /// SkipMap.
    pub fn range<Q, R>(&self, range: R) -> Range<'_, K, V, Q, R>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        Range {
            map: self,
            range,
            exhaused: false,
            next: None,
            next_back: None,
            called: 0,
            _phantom: Default::default(),
        }
    }

    /// The SkipMap is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The current number of entries in the SkipMap.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// The current height of the SkipMap.
    pub fn height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }

    // Search for the node that comes before the bound in the SkipMap.
    fn find_from_node<Q>(&self, bounds: Bound<&Q>) -> NodePtr<K, V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match bounds {
            std::ops::Bound::Included(v) => match self.seek_for_base_splice(v) {
                SpliceOrMatch::Splice(splice) => splice.prev,
                SpliceOrMatch::Match(node) => {
                    // It is safe to unwrap here because matches can't match a boundary
                    // and there's always a boundary.
                    node.load_prev(0).unwrap()
                }
            },
            std::ops::Bound::Excluded(v) => match self.seek_for_base_splice(v) {
                SpliceOrMatch::Splice(splice) => splice.prev,
                SpliceOrMatch::Match(node) => node,
            },
            std::ops::Bound::Unbounded => self.head.load(),
        }
    }

    // Search for the node that comes after the bound in the SkipMap.
    fn find_to_node<Q>(&self, bounds: Bound<&Q>) -> NodePtr<K, V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match bounds {
            std::ops::Bound::Included(v) => match self.seek_for_base_splice(v) {
                SpliceOrMatch::Splice(splice) => splice.next,
                SpliceOrMatch::Match(node) => node.load_next(0).unwrap(),
            },
            std::ops::Bound::Excluded(v) => match self.seek_for_base_splice(v) {
                SpliceOrMatch::Splice(splice) => splice.next,
                SpliceOrMatch::Match(node) => node,
            },
            std::ops::Bound::Unbounded => self.tail.load(),
        }
    }

    fn new_node(&self, key: K, value: V) -> (NodePtr<K, V>, usize) {
        let height = self.random_height();
        let node = self.arena.alloc(height);
        unsafe { (*node).data = ManuallyDrop::new(NodeData { key, value }) }
        (NodePtr(node), height)
    }

    fn random_height(&self) -> usize {
        // Pseudorandom number generation from "Xorshift RNGs" by George Marsaglia.
        //
        // This particular set of operations generates 32-bit integers. See:
        // https://en.wikipedia.org/wiki/Xorshift#Example_implementation
        let mut num = self.seed.load(Ordering::Relaxed);
        num ^= num << 13;
        num ^= num >> 17;
        num ^= num << 5;
        self.seed.store(num, Ordering::Relaxed);
        let val = num as u32;

        let mut height = 1;
        for &p in PROBABILITIES.iter() {
            if val > p {
                break;
            }
            height += 1;
        }
        // Keep decreasing the height while it's much larger than all towers currently in the
        // skip list.
        let head = self.head.load();
        let tail = self.tail.load();
        while height >= 4 && head.load_next(height - 2) == Some(tail) {
            height -= 1;
        }

        // Track the max height to speed up lookups
        let mut max_height = self.height.load(Ordering::Relaxed);
        while height > max_height {
            match self.height.compare_exchange(
                max_height,
                height,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(h) => max_height = h,
            }
        }
        height
    }

    // Finds the splice between which this key should be placed in the SkipMap,
    // or the Node with the matching key if one exists.
    fn find_splice_for_level<Q>(
        &self,
        key: &Q,
        level: usize,
        start: NodePtr<K, V>,
    ) -> SpliceOrMatch<K, V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut prev = start;
        // We can unwrap here because we know that start must be before
        // our key no matter what, and the tail node is after.
        let mut next = start.load_next(level).unwrap();
        loop {
            // Assume prev.key < key.
            let Some(after_next) = next.load_next(level) else {
                // We know that next must be tail.
                return Splice { prev, next }.into();
            };
            match key.cmp(next.key()) {
                std::cmp::Ordering::Less => return Splice { next, prev }.into(),
                std::cmp::Ordering::Equal => return SpliceOrMatch::Match(next),
                std::cmp::Ordering::Greater => {
                    prev = next;
                    next = after_next;
                }
            }
        }
    }

    // Returns the set of splices for all the levels where a key should be
    // inserted. If the key already exists in the SkipMap, None is returned.
    fn seek_splices(&self, key: &K) -> Option<Splices<K, V>> {
        let mut splices = Splices::default();
        let mut level = self.height() - 1;
        let mut prev = self.head.load();
        loop {
            match self.find_splice_for_level(key.borrow(), level, prev) {
                SpliceOrMatch::Splice(splice) => {
                    prev = splice.prev;
                    splices[level] = Some(splice)
                }
                SpliceOrMatch::Match(_match) => break None,
            }
            if level == 0 {
                break Some(splices);
            }
            level -= 1;
        }
    }

    fn seek_for_base_splice<Q>(&self, key: &Q) -> SpliceOrMatch<K, V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut level = self.height() - 1;
        let mut prev = self.head.load();
        loop {
            match self.find_splice_for_level(key, level, prev) {
                n @ SpliceOrMatch::Match(_) => return n,
                s @ SpliceOrMatch::Splice(_) if level == 0 => return s,
                SpliceOrMatch::Splice(s) => {
                    prev = s.prev;
                    level -= 1;
                }
            }
        }
    }
}

// It is important to run the drop action associated with the data
// inserted into the SkipMap in order to not leak memory.
//
// This implementation is somewhat unfortunate in that it's going to
// bounce around the SkipMap in sorted order.
//
// TODO: Perhaps a better design would be to keep nodes densely in
// the arenas so that it was possible to iterate through the initialized
// nodes without needing to traverse the links when dropping for better
// memory locality. A downside there is that we'd need to keep fixed-sized
// nodes. Perhaps a reasonable solution there might be to have only towers
// taller than 1 out-of-line and then we could iterate all the nodes more
// cheaply.
impl<K, V> Drop for SkipMap<K, V> {
    fn drop(&mut self) {
        if std::mem::needs_drop::<K>() || std::mem::needs_drop::<V>() {
            self.iter()
                .for_each(|entry| unsafe { ManuallyDrop::drop(&mut (*entry.node.0).data) });
        }
    }
}

const MAX_HEIGHT: usize = 20;

// Precompute the value thresholds for given node heights for all levels other
// than the first level, where all nodes will have links.
static PROBABILITIES: LazyLock<[u32; MAX_HEIGHT - 1]> = LazyLock::new(|| {
    let mut probabilities = [0u32; MAX_HEIGHT - 1];
    const P_VALUE: f64 = 1f64 / std::f64::consts::E;
    let mut p = 1f64;
    for i in 0..MAX_HEIGHT {
        if i > 0 {
            probabilities[i - 1] = ((u32::MAX as f64) * p) as u32;
        }
        p *= P_VALUE;
    }
    probabilities
});

#[repr(C)]
struct Node<K, V> {
    data: ManuallyDrop<NodeData<K, V>>,
    // Note that this is a lie! Sometimes this array is shorter than MAX_HEIGHT.
    // and will instead point to garbage. That's okay because we'll use other
    // bookkeeping invariants to ensure that we never actually access the garbage.
    tower: [Links<K, V>; MAX_HEIGHT],
}

struct NodeData<K, V> {
    key: K,
    value: V,
}

// The forward and backward pointers in the tower for nodes.
#[repr(C)]
struct Links<K, V> {
    next: NodeCell<K, V>,
    prev: NodeCell<K, V>,
}

// BoundaryNodePtr points to either the head or tail node. It is never modified
// after it is created, so it can use Ordering::Relaxed without concern. It's
// only using atomics at all because it makes the object Send and Sync and they
// don't really have cost given there won't ever be contention.
struct BoundaryNode<K, V>(AtomicPtr<Node<K, V>>);

impl<K, V> BoundaryNode<K, V> {
    fn load(&self) -> NodePtr<K, V> {
        let Self(ptr) = self;
        NodePtr(ptr.load(Ordering::Relaxed))
    }

    fn new(node: NodePtr<K, V>) -> Self {
        Self(AtomicPtr::new(node.0))
    }
}

struct NodePtr<K, V>(*mut Node<K, V>);

impl<K, V> Clone for NodePtr<K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K, V> Copy for NodePtr<K, V> {}

impl<K, V> Eq for NodePtr<K, V> {}

impl<K, V> PartialEq for NodePtr<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K, V> Hash for NodePtr<K, V> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<K, V> std::fmt::Debug for NodePtr<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<K, V> NodePtr<K, V> {
    fn new(ptr: *mut Node<K, V>) -> Option<Self> {
        (!ptr.is_null()).then_some(Self(ptr))
    }

    fn init_next(self, level: usize, next: Self) {
        self.links(level).next.store(next);
    }

    fn init_prev(self, level: usize, prev: Self) {
        self.links(level).prev.store(prev);
    }

    fn cas_next(self, level: usize, current: Self, new: Self) -> Result<(), Option<Self>> {
        self.links(level).next.cas(current, new)
    }

    fn cas_prev(self, level: usize, current: Self, new: Self) -> Result<(), Option<Self>> {
        self.links(level).prev.cas(current, new)
    }

    fn load_next(self, level: usize) -> Option<NodePtr<K, V>> {
        self.links(level).next.load()
    }

    fn load_prev(self, level: usize) -> Option<NodePtr<K, V>> {
        self.links(level).prev.load()
    }

    fn links(&self, level: usize) -> &'_ Links<K, V> {
        let Self(ptr) = self;
        unsafe { &(**ptr).tower[level] }
    }

    fn key<Q>(&self) -> &Q
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let Self(ptr) = self;
        unsafe { &(**ptr) }.data.key.borrow()
    }
}

#[repr(transparent)]
struct NodeCell<K, V>(AtomicPtr<Node<K, V>>);

impl<K, V> NodeCell<K, V> {
    fn store(&self, value: NodePtr<K, V>) {
        let Self(ptr) = self;
        ptr.store(value.0, Ordering::Release);
    }

    fn cas(&self, current: NodePtr<K, V>, new: NodePtr<K, V>) -> Result<(), Option<NodePtr<K, V>>> {
        let Self(ptr) = self;
        match ptr.compare_exchange(current.0, new.0, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => Ok(()),
            Err(new) => Err(NodePtr::new(new)),
        }
    }

    fn load(&self) -> Option<NodePtr<K, V>> {
        let Self(ptr) = self;
        NodePtr::new(ptr.load(Ordering::Acquire))
    }
}

enum SpliceOrMatch<K, V> {
    Splice(Splice<K, V>),
    Match(NodePtr<K, V>),
}

impl<K, V> From<Splice<K, V>> for SpliceOrMatch<K, V> {
    fn from(value: Splice<K, V>) -> Self {
        SpliceOrMatch::Splice(value)
    }
}

type Splices<K, V> = [Option<Splice<K, V>>; MAX_HEIGHT];

struct Splice<K, V> {
    prev: NodePtr<K, V>,
    next: NodePtr<K, V>,
}

impl<K, V> Clone for Splice<K, V> {
    fn clone(&self) -> Self {
        let &Self { prev, next } = self;
        Self { prev, next }
    }
}

// Iter is an Iterator over all elements of a SkipMap.
pub struct Iter<'map, K, V> {
    // Keeps the map alive.
    _map: &'map SkipMap<K, V>,
    exhausted: bool,
    before: NodePtr<K, V>,
    after: NodePtr<K, V>,
}

impl<'map, K, V> Iter<'map, K, V> {
    fn new(map: &'map SkipMap<K, V>) -> Self {
        Self {
            _map: map,
            exhausted: false,
            before: map.head.load(),
            after: map.tail.load(),
        }
    }
}

impl<'map, K, V> Iterator for Iter<'map, K, V> {
    type Item = Entry<'map, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let next = self.before.load_next(0).unwrap();
        if next == self.after {
            self.exhausted = true;
            return None;
        }
        self.before = next;
        Some(Entry::new(next))
    }
}

impl<'map, K, V> DoubleEndedIterator for Iter<'map, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let next = self.after.load_prev(0).unwrap();
        if next == self.before {
            self.exhausted = true;
            return None;
        }
        self.after = next;
        Some(Entry::new(next))
    }
}

/// Range is an Iterator over a SkipMap for a range.
pub struct Range<'m, K, V, Q: ?Sized, R> {
    map: &'m SkipMap<K, V>,
    range: R,
    exhaused: bool,
    next: Option<NodePtr<K, V>>,
    next_back: Option<NodePtr<K, V>>,
    called: usize,
    _phantom: PhantomData<fn(Q)>,
}

pub struct Entry<'m, K, V> {
    node: NodePtr<K, V>,
    _phantom: PhantomData<&'m ()>,
}

impl<'m, K, V> Entry<'m, K, V> {
    fn new(node: NodePtr<K, V>) -> Self {
        Self {
            node,
            _phantom: PhantomData,
        }
    }

    pub fn key(&self) -> &'m K {
        // Transmute because we're lying about the lifetime.
        unsafe { core::mem::transmute(&(&(*self.node.0).data).key) }
    }

    pub fn value(&self) -> &'m V {
        // Transmute because we're lying about the lifetime.
        unsafe { core::mem::transmute(&(&(*self.node.0).data).value) }
    }
}

impl<'m, K, V, Q: ?Sized, R> Range<'m, K, V, Q, R> {
    fn exhaust(&mut self) {
        self.exhaused = true;
        self.next = None;
        self.next_back = None;
    }
}

impl<'m, K, V, Q, R> Iterator for Range<'m, K, V, Q, R>
where
    K: Borrow<Q> + Ord,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Entry<'m, K, V>;

    #[allow(unsafe_code)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.exhaused {
            return None;
        }
        self.called += 1;
        let next = if let Some(next) = self.next {
            next
        } else {
            let before = self.map.find_from_node(self.range.start_bound());
            match before.load_next(0) {
                Some(next) => next,
                None => {
                    self.exhaust();
                    return None;
                }
            }
        };
        // If after_next is None, then we're at the tail and are done.
        let Some(after_next) = next.load_next(0) else {
            self.exhaust();
            return None;
        };
        // If we're not at the tail, then the key is valid.
        if match self.range.end_bound() {
            Bound::Included(bound) => next.key() > bound,
            Bound::Excluded(bound) => next.key() >= bound,
            Bound::Unbounded => false,
        } {
            self.exhaust();
            return None;
        }
        // Make sure we haven't moved past reverse iteration.
        if self.next_back.is_none_or(|next_back| next_back != next) {
            self.next = Some(after_next);
        } else {
            self.exhaust();
        };
        Some(Entry::new(next))
    }
}

impl<'m, K, V, Q, R> DoubleEndedIterator for Range<'m, K, V, Q, R>
where
    K: Borrow<Q> + Ord,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.exhaused {
            return None;
        }
        let next_back = if let Some(next_back) = self.next_back {
            next_back
        } else {
            let after = self.map.find_to_node(self.range.end_bound());
            match after.load_prev(0) {
                Some(next_back) => next_back,
                None => {
                    self.exhaust();
                    return None;
                }
            }
        };
        let Some(before_next_back) = next_back.load_prev(0) else {
            self.exhaust();
            return None;
        };
        if match self.range.start_bound() {
            Bound::Included(bound) => next_back.key() < bound,
            Bound::Excluded(bound) => next_back.key() <= bound,
            Bound::Unbounded => false,
        } {
            self.exhaust();
            return None;
        }
        if self.next.is_none_or(|next| next_back != next) {
            self.next_back = Some(before_next_back);
        } else {
            self.exhaust();
        };
        Some(Entry::new(next_back))
    }
}

#[cfg(test)]
impl<K, V> SkipMap<K, V>
where
    K: Ord,
{
    pub(crate) fn check_integrity(&mut self) {
        use std::collections::HashSet;
        // We want to check that there are no cycles, that the forward and backwards
        // directions have the same chains at all levels, and that the values are
        // ordered.
        let head_nodes = {
            let mut cur = Some(self.head.load());
            let mut head_forward_nodes = HashSet::new();
            let mut head_nodes = Vec::new();
            while let Some(node) = cur {
                head_nodes.push(node);
                assert!(head_forward_nodes.insert(node), "head");
                cur = node.load_next(0);
            }
            head_nodes
        };

        let mut tail_nodes = {
            let mut cur = Some(self.tail.load());
            let mut tail_backward_nodes = HashSet::new();
            let mut tail_nodes = Vec::new();
            while let Some(node) = cur {
                tail_nodes.push(node);
                assert!(tail_backward_nodes.insert(node), "tail");
                cur = node.load_prev(0);
            }
            tail_nodes
        };
        tail_nodes.reverse();
        assert_eq!(head_nodes, tail_nodes);
    }
}

struct ArenasAllocator<K, V> {
    arenas: Arenas,
    _phantom: PhantomData<fn(K, V)>,
}

impl<K, V> Default for ArenasAllocator<K, V> {
    fn default() -> Self {
        Self {
            arenas: Default::default(),
            _phantom: Default::default(),
        }
    }
}

impl<K, V> ArenasAllocator<K, V> {
    const ALIGNMENT: usize = align_of::<Node<K, V>>();
    const TOWER_OFFSET: usize = offset_of!(Node<K, V>, tower);

    fn alloc(&self, height: usize) -> *mut Node<K, V> {
        let layout = unsafe {
            Layout::from_size_align_unchecked(
                Self::TOWER_OFFSET + (height * size_of::<Links<K, V>>()),
                Self::ALIGNMENT,
            )
        };

        self.arenas.alloc(layout) as *mut Node<K, V>
    }
}
