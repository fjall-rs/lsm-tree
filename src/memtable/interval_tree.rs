// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! AVL-balanced interval tree for efficient range tombstone queries in memtables.
//!
//! Keyed by `start`, augmented with `subtree_max_end`, `subtree_max_seqno`,
//! and `subtree_min_seqno` for pruning during queries.

use crate::range_tombstone::CoveringRt;
use crate::range_tombstone::RangeTombstone;
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
    left: Option<Box<Self>>,
    right: Option<Box<Self>>,

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
// NOTE: #[allow] not #[expect] — this lint only fires on some Rust versions
// (present locally but absent on CI), so #[expect] causes unfulfilled-lint-expectation errors.
#[allow(
    clippy::unnecessary_box_returns,
    reason = "tree rotations pass Box<Node> through; unboxing would add needless allocation"
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
// NOTE: #[allow] not #[expect] — this lint only fires on some Rust versions
// (present locally but absent on CI), so #[expect] causes unfulfilled-lint-expectation errors.
#[allow(
    clippy::unnecessary_box_returns,
    reason = "tree rotations pass Box<Node> through; unboxing would add needless allocation"
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
// NOTE: #[allow] not #[expect] — this lint only fires on some Rust versions
// (present locally but absent on CI), so #[expect] causes unfulfilled-lint-expectation errors.
#[allow(
    clippy::unnecessary_box_returns,
    reason = "tree rotations pass Box<Node> through; unboxing would add needless allocation"
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

/// Returns `(node, was_new)` — `was_new` is false when a duplicate was replaced.
fn insert_node(node: Option<Box<Node>>, tombstone: RangeTombstone) -> (Box<Node>, bool) {
    let Some(mut node) = node else {
        return (Box::new(Node::new(tombstone)), true);
    };

    let was_new;
    match tombstone.cmp(&node.tombstone) {
        Ordering::Less => {
            let (child, new) = insert_node(node.left.take(), tombstone);
            node.left = Some(child);
            was_new = new;
        }
        Ordering::Greater => {
            let (child, new) = insert_node(node.right.take(), tombstone);
            node.right = Some(child);
            was_new = new;
        }
        Ordering::Equal => {
            // Duplicate — replace (shouldn't normally happen)
            node.tombstone = tombstone;
            node.update_augmentation();
            return (node, false);
        }
    }

    (balance(node), was_new)
}

/// Like `collect_overlapping`, but returns `true` as soon as any overlapping
/// tombstone with `seqno > key_seqno` is found. Avoids Vec allocation on the
/// hot read path.
fn any_overlapping_suppresses(
    node: Option<&Node>,
    key: &[u8],
    key_seqno: SeqNo,
    read_seqno: SeqNo,
) -> bool {
    let Some(n) = node else { return false };

    if n.subtree_min_seqno >= read_seqno {
        return false;
    }

    if n.subtree_max_end.as_ref() <= key {
        return false;
    }

    if any_overlapping_suppresses(n.left.as_deref(), key, key_seqno, read_seqno) {
        return true;
    }

    if n.tombstone.start.as_ref() <= key {
        if n.tombstone.contains_key(key)
            && n.tombstone.visible_at(read_seqno)
            && n.tombstone.seqno > key_seqno
        {
            return true;
        }
        return any_overlapping_suppresses(n.right.as_deref(), key, key_seqno, read_seqno);
    }

    false
}

/// In-order traversal to produce sorted output.
fn inorder(node: Option<&Node>, result: &mut Vec<RangeTombstone>) {
    let Some(n) = node else { return };
    inorder(n.left.as_deref(), result);
    result.push(n.tombstone.clone());
    inorder(n.right.as_deref(), result);
}

/// Collects tombstones that fully cover `[min, max]` and are visible at `read_seqno`.
fn collect_covering(
    node: Option<&Node>,
    min: &[u8],
    max: &[u8],
    read_seqno: SeqNo,
    best: &mut Option<CoveringRt>,
) {
    let Some(n) = node else { return };

    // Prune: no tombstone visible at this read_seqno
    if n.subtree_min_seqno >= read_seqno {
        return;
    }

    // Prune: max_end <= max means no interval in subtree can fully cover [min, max]
    // (need end > max, i.e., max_end > max for half-open covering)
    if n.subtree_max_end.as_ref() <= max {
        return;
    }

    // Recurse left
    collect_covering(n.left.as_deref(), min, max, read_seqno, best);

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
        collect_covering(n.right.as_deref(), min, max, read_seqno, best);
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
        let (root, was_new) = insert_node(self.root.take(), tombstone);
        self.root = Some(root);
        if was_new {
            self.len += 1;
        }
    }

    /// Returns `true` if the given key at the given seqno is suppressed by
    /// any range tombstone visible at `read_seqno`.
    ///
    /// O(log n + k) where k is the number of overlapping tombstones.
    /// Uses early-exit traversal to avoid allocating a Vec.
    pub fn query_suppression(&self, key: &[u8], key_seqno: SeqNo, read_seqno: SeqNo) -> bool {
        any_overlapping_suppresses(self.root.as_deref(), key, key_seqno, read_seqno)
    }

    /// Returns the highest-seqno visible tombstone that fully covers `[min, max]`,
    /// or `None` if no such tombstone exists.
    ///
    /// Used for table-skip decisions.
    #[cfg_attr(not(test), expect(dead_code, reason = "used for table-skip decisions"))]
    pub fn query_covering_rt_for_range(
        &self,
        min: &[u8],
        max: &[u8],
        read_seqno: SeqNo,
    ) -> Option<CoveringRt> {
        let mut best = None;
        collect_covering(self.root.as_deref(), min, max, read_seqno, &mut best);
        best
    }

    /// Returns all tombstones in sorted order (by `RangeTombstone::Ord`).
    ///
    /// Used for flush.
    pub fn iter_sorted(&self) -> Vec<RangeTombstone> {
        let mut result = Vec::with_capacity(self.len);
        inorder(self.root.as_deref(), &mut result);
        result
    }

    /// Returns the number of tombstones in the tree.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the tree is empty.
    #[expect(
        dead_code,
        reason = "tree may have tombstones but is_empty not called in all paths"
    )]
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
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "tests intentionally use direct unwraps and indexing for compact fixtures"
)]
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
        assert!(!tree.query_suppression(b"c", 5, 9));
    }

    #[test]
    fn multiple_tombstones() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"a", b"f", 10));
        tree.insert(rt(b"d", b"m", 20));
        tree.insert(rt(b"p", b"z", 5));

        assert!(tree.query_suppression(b"e", 15, 100));
        assert!(tree.query_suppression(b"e", 5, 100));
        assert!(!tree.query_suppression(b"e", 25, 100));

        assert!(tree.query_suppression(b"q", 3, 100));
        assert!(!tree.query_suppression(b"q", 10, 100));
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
        assert_eq!(sorted[0].start.as_ref(), b"a");
        assert_eq!(sorted[1].start.as_ref(), b"d");
        assert_eq!(sorted[2].start.as_ref(), b"m");
    }

    #[test]
    fn avl_balance_maintained() {
        let mut tree = IntervalTree::new();
        for i in 0u8..20 {
            let s = vec![i];
            let e = vec![i + 1];
            tree.insert(rt(&s, &e, u64::from(i)));
        }
        assert_eq!(tree.len(), 20);
        if let Some(ref root) = tree.root {
            assert!(root.height <= 6, "AVL height too large: {}", root.height);
        }
    }

    #[test]
    fn seqno_pruning() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"a", b"z", 100));
        tree.insert(rt(b"b", b"y", 200));

        assert!(!tree.query_suppression(b"c", 5, 50));
    }

    #[test]
    fn max_end_pruning() {
        let mut tree = IntervalTree::new();
        tree.insert(rt(b"a", b"c", 10));
        tree.insert(rt(b"b", b"d", 10));

        assert!(!tree.query_suppression(b"e", 5, 100));
    }
}
