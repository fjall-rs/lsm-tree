// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};
use crate::merge_operator::MergeOperator;
use crate::range_tombstone::RangeTombstone;
use crate::{InternalValue, SeqNo, UserKey, UserValue, ValueType};
use std::sync::Arc;

/// Consumes a stream of KVs and emits a new stream according to MVCC and tombstone rules
///
/// This iterator is used for read operations.
pub struct MvccStream<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> {
    inner: DoubleEndedPeekable<crate::Result<InternalValue>, I>,
    merge_operator: Option<Arc<dyn MergeOperator>>,

    /// Range tombstones with per-source visibility cutoffs. When set, merge
    /// resolution skips entries suppressed by an RT (treats them as a
    /// tombstone boundary). Each tuple is `(tombstone, cutoff_seqno)`.
    range_tombstones: Vec<(RangeTombstone, SeqNo)>,

    /// Reusable buffer for reverse-iteration merge resolution. Avoids
    /// allocating a fresh `Vec` on every `next_back()` call.
    key_entries_buf: Vec<InternalValue>,
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> MvccStream<I> {
    /// Initializes a new multi-version-aware iterator.
    #[must_use]
    pub fn new(iter: I, merge_operator: Option<Arc<dyn MergeOperator>>) -> Self {
        Self {
            inner: iter.double_ended_peekable(),
            merge_operator,
            range_tombstones: Vec::new(),
            key_entries_buf: Vec::new(),
        }
    }

    /// Installs range tombstones for merge-resolution awareness.
    ///
    /// When set, operands or base values suppressed by a range tombstone are
    /// treated as a deletion boundary (merge stops, base = None).
    #[must_use]
    pub fn with_range_tombstones(mut self, rts: Vec<(RangeTombstone, SeqNo)>) -> Self {
        self.range_tombstones = rts;
        self
    }

    /// Returns true if the entry is suppressed by any installed range tombstone.
    fn is_rt_suppressed(&self, entry: &InternalValue) -> bool {
        self.range_tombstones
            .iter()
            .any(|(rt, cutoff)| rt.should_suppress(&entry.key.user_key, entry.key.seqno, *cutoff))
    }

    /// Collects all entries for the given key and applies the merge operator (forward).
    fn resolve_merge_forward(
        &mut self,
        head: &InternalValue,
        merge_op: &dyn MergeOperator,
    ) -> crate::Result<InternalValue> {
        let user_key = &head.key.user_key;
        let mut operands: Vec<UserValue> = vec![head.value.clone()];
        let mut base_value: Option<UserValue> = None;
        let mut found_base = false;
        let mut saw_indirection_base = false;

        // Collect remaining same-key entries
        loop {
            let Some(next) = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    kv.key.user_key == *user_key
                } else {
                    true
                }
            }) else {
                break;
            };

            let next = next?;

            // Range tombstone suppression: an RT-suppressed entry is logically
            // deleted — treat it as a tombstone boundary (no base value).
            if self.is_rt_suppressed(&next) {
                found_base = true;
                break;
            }

            match next.key.value_type {
                ValueType::MergeOperand => {
                    operands.push(next.value);
                }
                ValueType::Value => {
                    base_value = Some(next.value);
                    found_base = true;
                    break;
                }
                ValueType::Indirection => {
                    // Indirection payloads are internal blob pointers and must not be
                    // used as a merge base user value. Remember that we saw an
                    // indirection base so we can skip merge resolution for this key.
                    found_base = true;
                    saw_indirection_base = true;
                    break;
                }
                ValueType::Tombstone | ValueType::WeakTombstone => {
                    // Tombstone kills base
                    found_base = true;
                    break;
                }
            }
        }

        // Drain any remaining same-key entries
        if found_base {
            self.drain_key_min(user_key)?;
        }

        // If the base would be an indirection, do not attempt to resolve the merge;
        // just return the newest entry unchanged.
        if saw_indirection_base {
            return Ok(head.clone());
        }

        // Reverse to chronological order (ascending seqno)
        operands.reverse();

        let operand_refs: Vec<&[u8]> = operands.iter().map(AsRef::as_ref).collect();
        let merged = merge_op.merge(user_key, base_value.as_deref(), &operand_refs)?;

        Ok(InternalValue::from_components(
            user_key.clone(),
            merged,
            head.key.seqno,
            ValueType::Value,
        ))
    }

    /// Resolves buffered entries for reverse iteration merge.
    /// `entries` are in ascending seqno order (oldest first, as collected by `next_back`).
    fn resolve_merge_buffered(&self, entries: Vec<InternalValue>) -> crate::Result<InternalValue> {
        let Some(merge_op) = &self.merge_operator else {
            // No merge operator — return newest entry (last in ascending order)
            return entries
                .into_iter()
                .last()
                .ok_or(crate::Error::Unrecoverable);
        };

        // entries are in ascending seqno order (oldest→newest)
        // The newest entry (last) has the highest seqno — that's our result seqno.
        let newest = entries.last().ok_or(crate::Error::Unrecoverable)?;
        let mut operands: Vec<UserValue> = Vec::new();
        let mut base_value: Option<UserValue> = None;
        let result_seqno = newest.key.seqno;
        let result_key = newest.key.user_key.clone();

        // Process in descending seqno order (newest first) to match forward merge semantics
        let mut saw_indirection = false;

        for entry in entries.iter().rev() {
            // RT-suppressed entries are logically deleted — treat as tombstone.
            if self.is_rt_suppressed(entry) {
                break;
            }

            match entry.key.value_type {
                ValueType::MergeOperand => {
                    operands.push(entry.value.clone());
                }
                ValueType::Value => {
                    base_value = Some(entry.value.clone());
                    break;
                }
                ValueType::Indirection => {
                    // Do not use indirection bytes as a merge base; stop scanning
                    // older versions.
                    saw_indirection = true;
                    break;
                }
                ValueType::Tombstone | ValueType::WeakTombstone => {
                    break;
                }
            }
        }

        // If the base is an indirection, return the newest entry unchanged.
        if saw_indirection {
            return entries
                .into_iter()
                .last()
                .ok_or(crate::Error::Unrecoverable);
        }

        // Reverse operands to chronological order (ascending seqno)
        operands.reverse();

        let operand_refs: Vec<&[u8]> = operands.iter().map(AsRef::as_ref).collect();
        let merged = merge_op.merge(&result_key, base_value.as_deref(), &operand_refs)?;

        Ok(InternalValue::from_components(
            result_key,
            merged,
            result_seqno,
            ValueType::Value,
        ))
    }

    // Drains all entries for the given user key from the front of the iterator.
    fn drain_key_min(&mut self, key: &UserKey) -> crate::Result<()> {
        loop {
            let Some(next) = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    kv.key.user_key == key
                } else {
                    true
                }
            }) else {
                return Ok(());
            };

            next?;
        }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> Iterator for MvccStream<I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let head = fail_iter!(self.inner.next()?);

        if head.key.value_type.is_merge_operand() {
            // Clone the Arc (not the operator) — resolve_merge_forward needs
            // &mut self which conflicts with borrowing self.merge_operator.
            if let Some(merge_op) = self.merge_operator.clone() {
                if !self.is_rt_suppressed(&head) {
                    let result = self.resolve_merge_forward(&head, merge_op.as_ref());
                    return Some(result);
                }
            }
        }

        // As long as items are the same key, ignore them
        fail_iter!(self.drain_key_min(&head.key.user_key));

        Some(Ok(head))
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> DoubleEndedIterator
    for MvccStream<I>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        // When a merge operator is configured we must buffer ALL entries
        // for a key (not just MergeOperands) because we only learn that
        // merge is needed when we reach the newest entry (last in
        // reverse order). The base Value/Tombstone seen first must be
        // preserved for the merge function.
        //
        // NOTE: Lazy allocation (only buffer after seeing MergeOperand) is
        // incorrect — reverse iteration visits the oldest (base) entry first,
        // so deferring allocation until a MergeOperand is found would lose
        // the base Value needed by the merge function.
        let has_merge_op = self.merge_operator.is_some();
        self.key_entries_buf.clear();

        loop {
            let tail = fail_iter!(self.inner.next_back()?);

            let prev = match self.inner.peek_back() {
                Some(Ok(prev)) => prev,
                Some(Err(_)) => {
                    #[expect(
                        clippy::expect_used,
                        reason = "we just asserted, the peeked value is an error"
                    )]
                    return Some(Err(self
                        .inner
                        .next_back()
                        .expect("should exist")
                        .expect_err("should be error")));
                }
                None => {
                    // Last item — resolve merge only if newest entry is a MergeOperand
                    // and not RT-suppressed.
                    if has_merge_op
                        && tail.key.value_type.is_merge_operand()
                        && !self.is_rt_suppressed(&tail)
                    {
                        self.key_entries_buf.push(tail);
                        let entries = self.key_entries_buf.drain(..).collect();
                        return Some(self.resolve_merge_buffered(entries));
                    }
                    return Some(Ok(tail));
                }
            };

            if prev.key.user_key < tail.key.user_key {
                // `tail` is the newest entry for this key — boundary reached.
                // Only merge if the newest entry is a MergeOperand.
                if has_merge_op
                    && tail.key.value_type.is_merge_operand()
                    && !self.is_rt_suppressed(&tail)
                {
                    self.key_entries_buf.push(tail);
                    let entries = std::mem::take(&mut self.key_entries_buf);
                    return Some(self.resolve_merge_buffered(entries));
                }
                return Some(Ok(tail));
            }

            // Same key — buffer entry when merge operator is configured.
            // We must buffer ALL types (including Value/Tombstone) because
            // we don't yet know if the newest entry will be a MergeOperand.
            if has_merge_op {
                self.key_entries_buf.push(tail);
            }
            // Without merge operator: skip older versions (loop continues)
        }
    }
}

#[cfg(test)]
#[expect(clippy::string_lit_as_bytes)]
mod tests {
    use super::*;
    use crate::{value::InternalValue, ValueType};
    use test_log::test;

    macro_rules! stream {
      ($($key:expr, $sub_key:expr, $value_type:expr),* $(,)?) => {{
          let mut values = Vec::new();
          let mut counters = std::collections::HashMap::new();

          $(
              let key = $key.as_bytes();
              let sub_key = $sub_key.as_bytes();
              let value_type = match $value_type {
                  "V" => ValueType::Value,
                  "T" => ValueType::Tombstone,
                  "W" => ValueType::WeakTombstone,
                  _ => panic!("Unknown value type"),
              };

              let counter = counters.entry($key).and_modify(|x| { *x -= 1 }).or_insert(999);
              values.push(InternalValue::from_components(key, sub_key, *counter, value_type));
          )*

          values
      }};
    }

    macro_rules! iter_closed {
        ($iter:expr) => {
            assert!($iter.next().is_none(), "iterator should be closed (done)");
            assert!(
                $iter.next_back().is_none(),
                "iterator should be closed (done)"
            );
        };
    }

    /// Tests that the iterator emit the same stuff forwards and backwards, just in reverse
    macro_rules! test_reverse {
        ($v:expr) => {
            let iter = Box::new($v.iter().cloned().map(Ok));
            let iter = MvccStream::new(iter, None);
            let mut forwards = iter.flatten().collect::<Vec<_>>();
            forwards.reverse();

            let iter = Box::new($v.iter().cloned().map(Ok));
            let iter = MvccStream::new(iter, None);
            let backwards = iter.rev().flatten().collect::<Vec<_>>();

            assert_eq!(forwards, backwards);
        };
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_stream_error() -> crate::Result<()> {
        {
            let vec = [
                Ok(InternalValue::from_components(
                    "a",
                    "new",
                    999,
                    ValueType::Value,
                )),
                Err(crate::Error::Io(std::io::Error::other("test error"))),
            ];

            let iter = Box::new(vec.into_iter());
            let mut iter = MvccStream::new(iter, None);

            // Because next calls drain_key_min, the error is immediately first, even though
            // the first item is technically Ok
            assert!(matches!(iter.next().unwrap(), Err(crate::Error::Io(_))));
            iter_closed!(iter);
        }

        {
            let vec = [
                Ok(InternalValue::from_components(
                    "a",
                    "new",
                    999,
                    ValueType::Value,
                )),
                Err(crate::Error::Io(std::io::Error::other("test error"))),
            ];

            let iter = Box::new(vec.into_iter());
            let mut iter = MvccStream::new(iter, None);

            assert!(matches!(
                iter.next_back().unwrap(),
                Err(crate::Error::Io(_))
            ));
            assert_eq!(
                InternalValue::from_components(*b"a", *b"new", 999, ValueType::Value),
                iter.next_back().unwrap()?,
            );
            iter_closed!(iter);
        }

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_queue_reverse_almost_gone() -> crate::Result<()> {
        let vec = [
            InternalValue::from_components("a", "a", 0, ValueType::Value),
            InternalValue::from_components("b", "", 1, ValueType::Tombstone),
            InternalValue::from_components("b", "b", 0, ValueType::Value),
            InternalValue::from_components("c", "", 1, ValueType::Tombstone),
            InternalValue::from_components("c", "c", 0, ValueType::Value),
            InternalValue::from_components("d", "", 1, ValueType::Tombstone),
            InternalValue::from_components("d", "d", 0, ValueType::Value),
            InternalValue::from_components("e", "", 1, ValueType::Tombstone),
            InternalValue::from_components("e", "e", 0, ValueType::Value),
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"d", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"e", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_queue_almost_gone_2() -> crate::Result<()> {
        let vec = [
            InternalValue::from_components("a", "a", 0, ValueType::Value),
            InternalValue::from_components("b", "", 1, ValueType::Tombstone),
            InternalValue::from_components("c", "", 1, ValueType::Tombstone),
            InternalValue::from_components("d", "", 1, ValueType::Tombstone),
            InternalValue::from_components("e", "", 1, ValueType::Tombstone),
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"d", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"e", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_queue() -> crate::Result<()> {
        let vec = [
            InternalValue::from_components("a", "a", 0, ValueType::Value),
            InternalValue::from_components("b", "b", 0, ValueType::Value),
            InternalValue::from_components("c", "c", 0, ValueType::Value),
            InternalValue::from_components("d", "d", 0, ValueType::Value),
            InternalValue::from_components("e", "", 1, ValueType::Tombstone),
            InternalValue::from_components("e", "e", 0, ValueType::Value),
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"b", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"c", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"d", *b"d", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"e", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_queue_weak_almost_gone() -> crate::Result<()> {
        let vec = [
            InternalValue::from_components("a", "a", 0, ValueType::Value),
            InternalValue::from_components("b", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("b", "b", 0, ValueType::Value),
            InternalValue::from_components("c", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("c", "c", 0, ValueType::Value),
            InternalValue::from_components("d", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("d", "d", 0, ValueType::Value),
            InternalValue::from_components("e", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("e", "e", 0, ValueType::Value),
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 1, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 1, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"d", *b"", 1, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"e", *b"", 1, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_queue_weak_almost_gone_2() -> crate::Result<()> {
        let vec = [
            InternalValue::from_components("a", "a", 0, ValueType::Value),
            InternalValue::from_components("b", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("c", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("d", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("e", "", 1, ValueType::WeakTombstone),
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 1, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 1, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"d", *b"", 1, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"e", *b"", 1, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_queue_weak_reverse() -> crate::Result<()> {
        let vec = [
            InternalValue::from_components("a", "a", 0, ValueType::Value),
            InternalValue::from_components("b", "b", 0, ValueType::Value),
            InternalValue::from_components("c", "c", 0, ValueType::Value),
            InternalValue::from_components("d", "d", 0, ValueType::Value),
            InternalValue::from_components("e", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("e", "e", 0, ValueType::Value),
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"b", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"c", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"d", *b"d", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"e", *b"", 1, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_stream_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "new", "V",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_stream_simple_multi_keys() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "new", "V",
          "a", "old", "V",
          "b", "new", "V",
          "b", "old", "V",
          "c", "newnew", "V",
          "c", "new", "V",
          "c", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"new", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"newnew", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_stream_tombstone() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_stream_tombstone_multi_keys() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",
          "a", "old", "V",
          "b", "", "T",
          "b", "old", "V",
          "c", "", "T",
          "c", "", "T",
          "c", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_stream_weak_tombstone_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_stream_weak_tombstone_resurrection() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "new", "V",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_stream_weak_tombstone_priority() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",  
          "a", "", "W",
          "a", "new", "V",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_stream_weak_tombstone_multi_keys() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
          "b", "", "W",
          "b", "old", "V",
          "c", "", "W",
          "c", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter, None);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    mod merge_operator_tests {
        use super::*;
        use std::sync::Arc;
        use test_log::test;

        /// Concatenation merge operator for testing
        struct ConcatMerge;

        impl crate::merge_operator::MergeOperator for ConcatMerge {
            fn merge(
                &self,
                _key: &[u8],
                base_value: Option<&[u8]>,
                operands: &[&[u8]],
            ) -> crate::Result<crate::UserValue> {
                let mut result = match base_value {
                    Some(b) => String::from_utf8_lossy(b).to_string(),
                    None => String::new(),
                };
                for op in operands {
                    if !result.is_empty() {
                        result.push(',');
                    }
                    result.push_str(&String::from_utf8_lossy(op));
                }
                Ok(result.into_bytes().into())
            }
        }

        fn merge_op() -> Option<Arc<dyn crate::merge_operator::MergeOperator>> {
            Some(Arc::new(ConcatMerge))
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn mvcc_merge_forward_operands_only() -> crate::Result<()> {
            let vec = vec![
                InternalValue::from_components("a", "op2", 2, ValueType::MergeOperand),
                InternalValue::from_components("a", "op1", 1, ValueType::MergeOperand),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op());

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::Value);
            assert_eq!(&*item.value, b"op1,op2");
            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn mvcc_merge_forward_with_base() -> crate::Result<()> {
            let vec = vec![
                InternalValue::from_components("a", "op2", 3, ValueType::MergeOperand),
                InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
                InternalValue::from_components("a", "base", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op());

            let item = iter.next().unwrap()?;
            assert_eq!(&*item.value, b"base,op1,op2");
            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn mvcc_merge_forward_with_tombstone() -> crate::Result<()> {
            let vec = vec![
                InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
                InternalValue::from_components("a", "", 2, ValueType::Tombstone),
                InternalValue::from_components("a", "old", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op());

            // Merge above tombstone: no base
            let item = iter.next().unwrap()?;
            assert_eq!(&*item.value, b"op1");
            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn mvcc_merge_forward_mixed_keys() -> crate::Result<()> {
            let vec = vec![
                InternalValue::from_components("a", "val_a", 5, ValueType::Value),
                InternalValue::from_components("b", "op2", 4, ValueType::MergeOperand),
                InternalValue::from_components("b", "op1", 3, ValueType::MergeOperand),
                InternalValue::from_components("c", "val_c", 2, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let iter = MvccStream::new(iter, merge_op());
            let out: Vec<_> = iter.map(Result::unwrap).collect();

            assert_eq!(out.len(), 3);
            assert_eq!(&*out[0].value, b"val_a");
            assert_eq!(&*out[1].value, b"op1,op2");
            assert_eq!(&*out[2].value, b"val_c");

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn mvcc_merge_reverse_operands_with_base() -> crate::Result<()> {
            let vec = vec![
                InternalValue::from_components("a", "op2", 3, ValueType::MergeOperand),
                InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
                InternalValue::from_components("a", "base", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op());

            let item = iter.next_back().unwrap()?;
            assert_eq!(&*item.value, b"base,op1,op2");
            assert!(iter.next_back().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn mvcc_merge_reverse_operands_only() -> crate::Result<()> {
            let vec = vec![
                InternalValue::from_components("a", "op2", 2, ValueType::MergeOperand),
                InternalValue::from_components("a", "op1", 1, ValueType::MergeOperand),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op());

            let item = iter.next_back().unwrap()?;
            assert_eq!(&*item.value, b"op1,op2");
            assert!(iter.next_back().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn mvcc_merge_reverse_mixed_keys() -> crate::Result<()> {
            let vec = vec![
                InternalValue::from_components("a", "val_a", 5, ValueType::Value),
                InternalValue::from_components("b", "op2", 4, ValueType::MergeOperand),
                InternalValue::from_components("b", "op1", 3, ValueType::MergeOperand),
                InternalValue::from_components("c", "val_c", 2, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let iter = MvccStream::new(iter, merge_op());
            let out: Vec<_> = iter.rev().map(Result::unwrap).collect();

            // Reverse: c, b(merged), a
            assert_eq!(out.len(), 3);
            assert_eq!(&*out[0].value, b"val_c");
            assert_eq!(&*out[1].value, b"op1,op2");
            assert_eq!(&*out[2].value, b"val_a");

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn mvcc_merge_reverse_single_operand_last() -> crate::Result<()> {
            // Single merge operand as last item in reverse iteration
            let vec = vec![InternalValue::from_components(
                "a",
                "op1",
                1,
                ValueType::MergeOperand,
            )];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op());

            let item = iter.next_back().unwrap()?;
            assert_eq!(&*item.value, b"op1");
            assert_eq!(item.key.value_type, ValueType::Value);

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn mvcc_merge_no_operator_passthrough() -> crate::Result<()> {
            // Without merge operator, MergeOperand entries returned as-is (latest version wins)
            let vec = vec![
                InternalValue::from_components("a", "op2", 2, ValueType::MergeOperand),
                InternalValue::from_components("a", "op1", 1, ValueType::MergeOperand),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, None);

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op2"); // latest only
            assert!(iter.next().is_none());

            Ok(())
        }

        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn mvcc_merge_reverse_single_operand_with_different_key() -> crate::Result<()> {
            // Single merge operand key followed by regular key in reverse
            let vec = vec![
                InternalValue::from_components("a", "val_a", 5, ValueType::Value),
                InternalValue::from_components("b", "op1", 3, ValueType::MergeOperand),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op());

            // Reverse: b(merged), a
            let item = iter.next_back().unwrap()?;
            assert_eq!(&*item.key.user_key, b"b");
            assert_eq!(&*item.value, b"op1");
            assert_eq!(item.key.value_type, ValueType::Value);

            let item = iter.next_back().unwrap()?;
            assert_eq!(&*item.key.user_key, b"a");

            assert!(iter.next_back().is_none());

            Ok(())
        }

        /// Forward: MergeOperand above an Indirection base must return the
        /// MergeOperand unchanged — indirection bytes are internal blob
        /// pointers, not user data.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn merge_forward_indirection_base_returns_head() -> crate::Result<()> {
            let vec = vec![
                InternalValue::from_components("a", "op2", 3, ValueType::MergeOperand),
                InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
                InternalValue::from_components("a", "blob_ptr", 1, ValueType::Indirection),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op());

            let item = iter.next().unwrap()?;
            assert_eq!(&*item.key.user_key, b"a");
            // Must return head MergeOperand unchanged, NOT merged with blob pointer
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op2");

            assert!(iter.next().is_none());
            Ok(())
        }

        /// Reverse: MergeOperand above an Indirection base must return the
        /// newest MergeOperand unchanged.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn merge_reverse_indirection_base_returns_newest() -> crate::Result<()> {
            let vec = vec![
                InternalValue::from_components("a", "op2", 3, ValueType::MergeOperand),
                InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
                InternalValue::from_components("a", "blob_ptr", 1, ValueType::Indirection),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op());

            let item = iter.next_back().unwrap()?;
            assert_eq!(&*item.key.user_key, b"a");
            // Must return newest MergeOperand unchanged
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op2");

            assert!(iter.next_back().is_none());
            Ok(())
        }

        /// Merge operator error must propagate through forward iteration.
        #[test]
        fn merge_forward_error_propagation() {
            struct FailMerge;
            impl crate::merge_operator::MergeOperator for FailMerge {
                fn merge(
                    &self,
                    _key: &[u8],
                    _base_value: Option<&[u8]>,
                    _operands: &[&[u8]],
                ) -> crate::Result<crate::UserValue> {
                    Err(crate::Error::MergeOperator)
                }
            }

            let vec = vec![
                InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
                InternalValue::from_components("a", "base", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let fail_op: Option<Arc<dyn crate::merge_operator::MergeOperator>> =
                Some(Arc::new(FailMerge));
            let mut iter = MvccStream::new(iter, fail_op);

            assert!(matches!(
                iter.next(),
                Some(Err(crate::Error::MergeOperator))
            ));
        }

        /// Merge operator error must propagate through reverse iteration.
        #[test]
        fn merge_reverse_error_propagation() {
            struct FailMerge;
            impl crate::merge_operator::MergeOperator for FailMerge {
                fn merge(
                    &self,
                    _key: &[u8],
                    _base_value: Option<&[u8]>,
                    _operands: &[&[u8]],
                ) -> crate::Result<crate::UserValue> {
                    Err(crate::Error::MergeOperator)
                }
            }

            let vec = vec![
                InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
                InternalValue::from_components("a", "base", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let fail_op: Option<Arc<dyn crate::merge_operator::MergeOperator>> =
                Some(Arc::new(FailMerge));
            let mut iter = MvccStream::new(iter, fail_op);

            assert!(matches!(
                iter.next_back(),
                Some(Err(crate::Error::MergeOperator))
            ));
        }

        /// WeakTombstone stops base search same as regular Tombstone.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn merge_forward_weak_tombstone_stops_base() -> crate::Result<()> {
            let vec = vec![
                InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
                InternalValue::from_components("a", "", 2, ValueType::WeakTombstone),
                InternalValue::from_components("a", "old_base", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op());

            let item = iter.next().unwrap()?;
            // WeakTombstone blocks base — merge with no base
            assert_eq!(item.key.value_type, ValueType::Value);
            assert_eq!(&*item.value, b"op1");

            assert!(iter.next().is_none());
            Ok(())
        }

        /// Forward: RT-suppressed base value is excluded from merge.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn merge_forward_rt_suppresses_base() -> crate::Result<()> {
            use crate::range_tombstone::RangeTombstone;

            // RT covers key "a" at seqno 2 → base@1 is suppressed
            let rt = RangeTombstone::new(b"a".to_vec().into(), b"b".to_vec().into(), 2);

            let vec = vec![
                InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
                InternalValue::from_components("a", "base", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op()).with_range_tombstones(vec![(rt, 4)]);

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::Value);
            // base@1 is RT-suppressed → merge with no base
            assert_eq!(&*item.value, b"op1");

            assert!(iter.next().is_none());
            Ok(())
        }

        /// Forward: RT-suppressed operand stops collection (treated as boundary).
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn merge_forward_rt_suppresses_operand() -> crate::Result<()> {
            use crate::range_tombstone::RangeTombstone;

            // RT at seqno 3 → operand@2 and base@1 are suppressed
            let rt = RangeTombstone::new(b"a".to_vec().into(), b"b".to_vec().into(), 3);

            let vec = vec![
                InternalValue::from_components("a", "op2", 4, ValueType::MergeOperand),
                InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
                InternalValue::from_components("a", "base", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op()).with_range_tombstones(vec![(rt, 5)]);

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::Value);
            // Only op2 survives; op1 and base are RT-suppressed
            assert_eq!(&*item.value, b"op2");

            assert!(iter.next().is_none());
            Ok(())
        }

        /// Reverse: RT-suppressed entries are excluded from merge.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn merge_reverse_rt_suppresses_base() -> crate::Result<()> {
            use crate::range_tombstone::RangeTombstone;

            let rt = RangeTombstone::new(b"a".to_vec().into(), b"b".to_vec().into(), 2);

            let vec = vec![
                InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
                InternalValue::from_components("a", "base", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op()).with_range_tombstones(vec![(rt, 4)]);

            let item = iter.next_back().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::Value);
            // base@1 suppressed → merge with no base
            assert_eq!(&*item.value, b"op1");

            assert!(iter.next_back().is_none());
            Ok(())
        }

        /// Forward: if the newest MergeOperand is RT-suppressed, skip merge
        /// entirely — pass through for the post-filter to suppress.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn merge_forward_rt_suppresses_head() -> crate::Result<()> {
            use crate::range_tombstone::RangeTombstone;

            // RT at seqno 5 covers "a" → head@3 is suppressed
            let rt = RangeTombstone::new(b"a".to_vec().into(), b"b".to_vec().into(), 5);

            let vec = vec![
                InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
                InternalValue::from_components("a", "base", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op()).with_range_tombstones(vec![(rt, 6)]);

            let item = iter.next().unwrap()?;
            // Head is RT-suppressed → merge skipped, head returned as-is
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op1");

            assert!(iter.next().is_none());
            Ok(())
        }

        /// Reverse: if the newest MergeOperand is RT-suppressed, skip merge.
        #[test]
        #[expect(clippy::unwrap_used, reason = "test assertion")]
        fn merge_reverse_rt_suppresses_head() -> crate::Result<()> {
            use crate::range_tombstone::RangeTombstone;

            let rt = RangeTombstone::new(b"a".to_vec().into(), b"b".to_vec().into(), 5);

            let vec = vec![
                InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
                InternalValue::from_components("a", "base", 1, ValueType::Value),
            ];

            let iter = Box::new(vec.into_iter().map(Ok));
            let mut iter = MvccStream::new(iter, merge_op()).with_range_tombstones(vec![(rt, 6)]);

            let item = iter.next_back().unwrap()?;
            // Head is RT-suppressed → merge skipped
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(&*item.value, b"op1");

            assert!(iter.next_back().is_none());
            Ok(())
        }
    }
}
