// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{InternalValue, SeqNo, UserKey, ValueType};
use std::iter::Peekable;

/// Consumes a stream of KVs and emits a new stream according to GC and tombstone rules
///
/// This iterator is used during flushing & compaction.
#[allow(clippy::module_name_repetitions)]
pub struct CompactionStream<I: Iterator<Item = crate::Result<InternalValue>>> {
    inner: Peekable<I>,
    gc_seqno_threshold: SeqNo,
}

impl<I: Iterator<Item = crate::Result<InternalValue>>> CompactionStream<I> {
    /// Initializes a new merge iterator
    #[must_use]
    pub fn new(iter: I, gc_seqno_threshold: SeqNo) -> Self {
        let iter = iter.peekable();

        Self {
            inner: iter,
            gc_seqno_threshold,
        }
    }

    fn drain_key_min(&mut self, key: &UserKey) -> crate::Result<()> {
        loop {
            let Some(next) = self.inner.peek() else {
                return Ok(());
            };

            let Ok(next) = next else {
                // NOTE: We just asserted, the peeked value is an error
                #[allow(clippy::expect_used)]
                return Err(self
                    .inner
                    .next()
                    .expect("should exist")
                    .expect_err("should be error"));
            };

            // Consume version
            if &next.key.user_key == key {
                // NOTE: We know the next value is not empty, because we just peeked it
                #[allow(clippy::expect_used)]
                self.inner.next().expect("should not be empty")?;
            } else {
                return Ok(());
            }
        }
    }
}

impl<I: Iterator<Item = crate::Result<InternalValue>>> Iterator for CompactionStream<I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let head = fail_iter!(self.inner.next()?);

            if let Some(peeked) = self.inner.peek() {
                let Ok(peeked) = peeked else {
                    // NOTE: We just asserted, the peeked value is an error
                    #[allow(clippy::expect_used)]
                    return Some(Err(self
                        .inner
                        .next()
                        .expect("value should exist")
                        .expect_err("should be error")));
                };

                // NOTE: Only item of this key and thus latest version, so return it no matter what
                if peeked.key.user_key > head.key.user_key {
                    return Some(Ok(head));
                }

                // NOTE: Next item is expired is expired,
                // so the tail of this user key is entirely expired, so drain it all
                if peeked.key.seqno < self.gc_seqno_threshold {
                    // NOTE: If next item is an actual value, and current value is weak tombstone,
                    // drop the tombstone
                    let drop_weak_tombstone = peeked.key.value_type == ValueType::Value
                        && head.key.value_type == ValueType::WeakTombstone;

                    fail_iter!(self.drain_key_min(&head.key.user_key));

                    if drop_weak_tombstone {
                        continue;
                    }
                }
            }

            return Some(Ok(head));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{InternalValue, ValueType};

    macro_rules! stream {
        ($($key:expr, $sub_key:expr, $value_type:expr),* $(,)?) => {{
            let mut values = Vec::new();
            let mut counters = std::collections::HashMap::new();

            $(
                #[allow(clippy::string_lit_as_bytes)]
                let key = $key.as_bytes();

                #[allow(clippy::string_lit_as_bytes)]
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
        };
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_queue_weak_tombstones() {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
          "b", "", "W",
          "b", "old", "V",
          "c", "", "W",
          "c", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_050);

        iter_closed!(iter);
    }

    /// GC should not evict tombstones, unless they are covered up
    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_tombstone_no_gc() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",
          "b", "", "T",
          "c", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, SeqNo::MAX);

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

        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_old_tombstone() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",
          "a", "", "T",
          "b", "", "T",
          "b", "", "T",
          "c", "", "T",
          "c", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 998);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 998, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"", 998, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"", 998, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_tombstone_overwrite_gc() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "val", "V",
          "a", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 999);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"val", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_weak_tombstone_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 0);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 998, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_weak_tombstone_no_gc() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 998);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 998, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_weak_tombstone_evict() {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 999);

        // NOTE: Weak tombstone is consumed because value is GC'ed

        iter_closed!(iter);
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_weak_tombstone_evict_next_value() -> crate::Result<()> {
        #[rustfmt::skip]
        let mut vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];
        vec.push(InternalValue::from_components(
            "b",
            "other",
            999,
            ValueType::Value,
        ));

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 999);

        // NOTE: Weak tombstone is consumed because value is GC'ed

        assert_eq!(
            InternalValue::from_components(*b"b", *b"other", 999, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_no_evict_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "old", "V",
          "b", "old", "V",
          "c", "old", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 0);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"old", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"old", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_no_evict_simple_multi_keys() -> crate::Result<()> {
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

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 0);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 998, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"new", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"old", 998, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"newnew", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"new", 998, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"old", 997, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }
}
