// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! AVL-balanced interval tree for efficient range tombstone queries in memtables.
//!
//! Keyed by `start`, augmented with `subtree_max_end`, `subtree_max_seqno`,
//! and `subtree_min_seqno` for pruning during queries.

use crate::range_tombstone::{CoveringRt, RangeTombstone};
use crate::{SeqNo, UserKey};
use std::cmp::Ordering;

/// An AVL-balanced BST keyed by range tombstone `start`, augmented with
/// subtree-level metadata for efficient interval queries.
pub struct IntervalTree {
    root: Option<Box<Node>>,
    len: usize,
}

struct Node {
    tombstone: RangeTombstone,

    // AVL metadata
    height: i32,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,

    // Augmented metadata
    subtree_max_end: UserKey,
    subtree_max_seqno: SeqNo,
    subtree_min_seqno: SeqNo,
}

impl Node {
    fn new(tombstone: RangeTombstone) -> Self {
        let subtree_max_end = tombstone.end.clone();
        let seqno = tombstone.seqno;
        Self {
            tombstone,
            height: 1,
            left: None,
            right: None,
            subtree_max_end,
            subtree_max_seqno: seqno,
            subtree_min_seqno: seqno,
        }
    }

    fn update_augmentation(&mut self) {
        self.subtree_max_end = self.tombstone.end.clone();
        self.subtree_max_seqno = self.tombstone.seqno;
        self.subtree_min_seqno = self.tombstone.seqno;
        self.height = 1;

        if let Some(ref left) = self.left {
            if left.subtree_max_end > self.subtree_max_end {
                self.subtree_max_end = left.subtree_max_end.clone();
            }
            if left.subtree_max_seqno > self.subtree_max_seqno {
                self.subtree_max_seqno = left.subtree_max_seqno;
            }
            if left.subtree_min_seqno < self.subtree_min_seqno {
                self.subtree_min_seqno = left.subtree_min_seqno;
            }
            self.height = left.height + 1;
        }

        if let Some(ref right) = self.right {
            if right.subtree_max_end > self.subtree_max_end {
                self.subtree_max_end = right.subtree_max_end.clone();
            }
            if right.subtree_max_seqno > self.subtree_max_seqno {
                self.subtree_max_seqno = right.subtree_max_seqno;
            }
            if right.subtree_min_seqno < self.subtree_min_seqno {
                self.subtree_min_seqno = right.subtree_min_seqno;
            }
            let rh = right.height + 1;
            if rh > self.height {
                self.height = rh;
            }
        }
    }

    fn balance_factor(&self) -> i32 {
        let lh = self.left.as_ref().map_or(0, |n| n.height);
        let rh = self.right.as_ref().map_or(0, |n| n.height);
        lh - rh
    }
}

#[expect(
    clippy::expect_used,
    reason = "rotation invariant: left child must exist"
)]
fn rotate_right(mut node: Box<Node>) -> Box<Node> {
    let mut new_root = node.left.take().expect("rotate_right requires left child");
    node.left = new_root.right.take();
    node.update_augmentation();
    new_root.right = Some(node);
    new_root.update_augmentation();
    new_root
}

#[expect(
    clippy::expect_used,
    reason = "rotation invariant: right child must exist"
)]
fn rotate_left(mut node: Box<Node>) -> Box<Node> {
    let mut new_root = node.right.take().expect("rotate_left requires right child");
    node.right = new_root.left.take();
    node.update_augmentation();
    new_root.left = Some(node);
    new_root.update_augmentation();
    new_root
}

#[expect(
    clippy::expect_used,
    reason = "balance factor guarantees child existence"
)]
fn balance(mut node: Box<Node>) -> Box<Node> {
    node.update_augmentation();
    let bf = node.balance_factor();

    if bf > 1 {
        // Left-heavy
        if let Some(ref left) = node.left {
            if left.balance_factor() < 0 {
                // Left-Right case
                node.left = Some(rotate_left(node.left.take().expect("just checked")));
            }
        }
        return rotate_right(node);
    }

    if bf < -1 {
        // Right-heavy
        if let Some(ref right) = node.right {
            if right.balance_factor() > 0 {
                // Right-Left case
                node.right = Some(rotate_right(node.right.take().expect("just checked")));
            }
        }
        return rotate_left(node);
    }

    node
}

fn insert_node(node: Option<Box<Node>>, tombstone: RangeTombstone) -> Box<Node> {
    let Some(mut node) = node else {
        return Box::new(Node::new(tombstone));
    };

    match tombstone.cmp(&node.tombstone) {
        Ordering::Less => {
            node.left = Some(insert_node(node.left.take(), tombstone));
        }
        Ordering::Greater => {
            node.right = Some(insert_node(node.right.take(), tombstone));
        }
        Ordering::Equal => {
            // Duplicate — replace (shouldn't normally happen)
            node.tombstone = tombstone;
            node.update_augmentation();
            return node;
        }
    }

    balance(node)
}

/// Collects all overlapping tombstones: those where `start <= key < end`
/// and `seqno <= read_seqno`.
fn collect_overlapping(
    node: &Option<Box<Node>>,
    key: &[u8],
    read_seqno: SeqNo,
    result: &mut Vec<RangeTombstone>,
) {
    let Some(n) = node else { return };

    // Prune: no tombstone in subtree is visible at this read_seqno
    if n.subtree_min_seqno > read_seqno {
        return;
    }

    // Prune: max_end <= key means no interval in this subtree covers key
    if n.subtree_max_end.as_ref() <= key {
        return;
    }

    // Recurse left (may have tombstones with start <= key)
    collect_overlapping(&n.left, key, read_seqno, result);

    // Check current node
    if n.tombstone.start.as_ref() <= key {
        if n.tombstone.contains_key(key) && n.tombstone.visible_at(read_seqno) {
            result.push(n.tombstone.clone());
        }
        // Recurse right (may also have tombstones with start <= key, up to key)
        collect_overlapping(&n.right, key, read_seqno, result);
    }
    // If start > key, no need to go right (all entries there have start > key too)
}

/// In-order traversal to produce sorted output.
fn inorder(node: &Option<Box<Node>>, result: &mut Vec<RangeTombstone>) {
    let Some(n) = node else { return };
    inorder(&n.left, result);
    result.push(n.tombstone.clone());
    inorder(&n.right, result);
}

/// Collects tombstones that fully cover `[min, max]` and are visible at `read_seqno`.
fn collect_covering(
    node: &Option<Box<Node>>,
    min: &[u8],
    max: &[u8],
    read_seqno: SeqNo,
    best: &mut Option<CoveringRt>,
) {
    let Some(n) = node else { return };

    // Prune: no tombstone visible at this read_seqno
    if n.subtree_min_seqno > read_seqno {
        return;
    }

    // Prune: max_end <= max means no interval in subtree can fully cover [min, max]
    // (need end > max, i.e., max_end > max for half-open covering)
    if n.subtree_max_end.as_ref() <= max {
        return;
    }

    // Recurse left
    collect_covering(&n.left, min, max, read_seqno, best);

    // Check current node: must have start <= min AND max < end
    if n.tombstone.start.as_ref() <= min
        && n.tombstone.fully_covers(min, max)
        && n.tombstone.visible_at(read_seqno)
    {
        let dominated = best.as_ref().is_some_and(|b| n.tombstone.seqno <= b.seqno);
        if !dominated {
            *best = Some(CoveringRt::from(&n.tombstone));
        }
    }

    // Only go right if some right-subtree entry might have start <= min
    if n.tombstone.start.as_ref() <= min {
        collect_covering(&n.right, min, max, read_seqno, best);
    }
}

impl IntervalTree {
    /// Creates a new empty interval tree.
    #[must_use]
    pub fn new() -> Self {
        Self { root: None, len: 0 }
    }

    /// Inserts a range tombstone into the tree. O(log n).
    pub fn insert(&mut self, tombstone: RangeTombstone) {
        self.root = Some(insert_node(self.root.take(), tombstone));
        self.len += 1;
    }

    /// Returns `true` if the given key at the given seqno is suppressed by
    /// any range tombstone visible at `read_seqno`.
    ///
    /// O(log n + k) where k is the number of overlapping tombstones.
    pub fn query_suppression(&self, key: &[u8], key_seqno: SeqNo, read_seqno: SeqNo) -> bool {
        let mut result = Vec::new();
        collect_overlapping(&self.root, key, read_seqno, &mut result);
        result.iter().any(|rt| rt.seqno > key_seqno)
    }

    /// Returns all tombstones overlapping with `key` and visible at `read_seqno`.
    ///
    /// Used for seek initialization: returns tombstones where `start <= key < end`
    /// and `seqno <= read_seqno`.
    pub fn overlapping_tombstones(&self, key: &[u8], read_seqno: SeqNo) -> Vec<RangeTombstone> {
        let mut result = Vec::new();
        collect_overlapping(&self.root, key, read_seqno, &mut result);
        result
    }

    /// Returns the highest-seqno visible tombstone that fully covers `[min, max]`,
    /// or `None` if no such tombstone exists.
    ///
    /// Used for table-skip decisions.
    pub fn query_covering_rt_for_range(
        &self,
        min: &[u8],
        max: &[u8],
        read_seqno: SeqNo,
    ) -> Option<CoveringRt> {
        let mut best = None;
        collect_covering(&self.root, min, max, read_seqno, &mut best);
        best
    }

    /// Returns all tombstones in sorted order (by `RangeTombstone::Ord`).
    ///
    /// Used for flush.
    pub fn iter_sorted(&self) -> Vec<RangeTombstone> {
        let mut result = Vec::with_capacity(self.len);
        inorder(&self.root, &mut result);
        result
    }

    /// Returns the number of tombstones in the tree.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the tree is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for IntervalTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn rt(start: &[u8], end: &[u8], seqno: SeqNo) -> RangeTombstone {
        RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
    }

    #[test]
    fn empty_tree_no_suppression() {
        let tree = IntervalTree::new();
        assert!(!tree.query_suppression(b"key", 5, 100));
    }

    #[test]
    fn single_tombstone_suppresses() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"b", b"y", 10));
        assert!(tree.query_suppression(b"c", 5, 100));
    }

    #[test]
    fn single_tombstone_no_suppress_newer_kv() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"b", b"y", 10));
        assert!(!tree.query_suppression(b"c", 15, 100));
    }

    #[test]
    fn single_tombstone_exclusive_end() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"b", b"y", 10));
        assert!(!tree.query_suppression(b"y", 5, 100));
    }

    #[test]
    fn single_tombstone_before_start() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"b", b"y", 10));
        assert!(!tree.query_suppression(b"a", 5, 100));
    }

    #[test]
    fn tombstone_not_visible() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"b", b"y", 10));
        assert!(!tree.query_suppression(b"c", 5, 9)); // read_seqno < tombstone_seqno
    }

    #[test]
    fn multiple_tombstones() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"a", b"f", 10));
        tree.insert(rt(b"d", b"m", 20));
        tree.insert(rt(b"p", b"z", 5));

        // "e" covered by both [a,f)@10 and [d,m)@20
        assert!(tree.query_suppression(b"e", 15, 100)); // 15 < 20
        assert!(tree.query_suppression(b"e", 5, 100)); // 5 < 20
        assert!(!tree.query_suppression(b"e", 25, 100)); // 25 > 20

        // "q" covered by [p,z)@5
        assert!(tree.query_suppression(b"q", 3, 100));
        assert!(!tree.query_suppression(b"q", 10, 100));
    }

    #[test]
    fn overlapping_tombstones_query() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"a", b"f", 10));
        tree.insert(rt(b"d", b"m", 20));
        tree.insert(rt(b"p", b"z", 5));

        let overlaps = tree.overlapping_tombstones(b"e", 100);
        assert_eq!(overlaps.len(), 2);
    }

    #[test]
    fn overlapping_tombstones_none() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"d", b"f", 10));
        let overlaps = tree.overlapping_tombstones(b"a", 100);
        assert!(overlaps.is_empty());
    }

    #[test]
    fn covering_rt_found() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"a", b"z", 50));
        tree.insert(rt(b"c", b"g", 10));

        let crt = tree.query_covering_rt_for_range(b"b", b"y", 100);
        assert!(crt.is_some());
        let crt = crt.unwrap();
        assert_eq!(crt.seqno, 50);
    }

    #[test]
    fn covering_rt_not_found_partial() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"c", b"g", 10));

        // [b, y] is not fully covered by [c, g)
        let crt = tree.query_covering_rt_for_range(b"b", b"y", 100);
        assert!(crt.is_none());
    }

    #[test]
    fn covering_rt_highest_seqno() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"a", b"z", 50));
        tree.insert(rt(b"a", b"z", 100));

        let crt = tree.query_covering_rt_for_range(b"b", b"y", 200);
        assert!(crt.is_some());
        assert_eq!(crt.unwrap().seqno, 100);
    }

    #[test]
    fn iter_sorted_empty() {
        let tree = IntervalTree::new();
        assert!(tree.iter_sorted().is_empty());
    }

    #[test]
    fn iter_sorted_multiple() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"d", b"f", 10));
        tree.insert(rt(b"a", b"c", 20));
        tree.insert(rt(b"m", b"z", 5));

        let sorted = tree.iter_sorted();
        assert_eq!(sorted.len(), 3);
        // Should be sorted by RangeTombstone::Ord (start asc, seqno desc, end asc)
        assert_eq!(sorted[0].start.as_ref(), b"a");
        assert_eq!(sorted[1].start.as_ref(), b"d");
        assert_eq!(sorted[2].start.as_ref(), b"m");
    }

    #[test]
    fn avl_balance_maintained() {
        let mut tree = IntervalTree::new();
        // Insert in sorted order — should trigger rotations
        for i in 0u8..20 {
            let s = vec![i];
            let e = vec![i + 1];
            tree.insert(rt(&s, &e, u64::from(i)));
        }
        assert_eq!(tree.len(), 20);
        // If AVL is working, height should be bounded ~log2(20) ≈ 5
        if let Some(ref root) = tree.root {
            assert!(root.height <= 6, "AVL height too large: {}", root.height);
        }
    }

    #[test]
    fn seqno_pruning() {
        let mut tree = IntervalTree::new();
        // Insert tombstones with high seqno only
        tree.insert(rt(b"a", b"z", 100));
        tree.insert(rt(b"b", b"y", 200));

        // Query with read_seqno < all tombstone seqnos — should find nothing
        assert!(!tree.query_suppression(b"c", 5, 50));
        let overlaps = tree.overlapping_tombstones(b"c", 50);
        assert!(overlaps.is_empty());
    }

    #[test]
    fn max_end_pruning() {
        let mut tree = IntervalTree::new();
        // Tombstones with limited end
        tree.insert(rt(b"a", b"c", 10));
        tree.insert(rt(b"b", b"d", 10));

        // Key past all ends — should find nothing
        assert!(!tree.query_suppression(b"e", 5, 100));
    }
}
