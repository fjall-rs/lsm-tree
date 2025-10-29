// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};
use crate::{InternalValue, UserKey};

/// Consumes a stream of KVs and emits a new stream according to MVCC and tombstone rules
///
/// This iterator is used for read operations.
pub struct MvccStream<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> {
    inner: DoubleEndedPeekable<crate::Result<InternalValue>, I>,
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> MvccStream<I> {
    /// Initializes a new merge iterator
    #[must_use]
    pub fn new(iter: I) -> Self {
        Self {
            inner: iter.double_ended_peekable(),
        }
    }

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

        // As long as items are the same key, ignore them
        fail_iter!(self.drain_key_min(&head.key.user_key));

        Some(Ok(head))
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> DoubleEndedIterator
    for MvccStream<I>
{
    fn next_back(&mut self) -> Option<Self::Item> {
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
                    return Some(Ok(tail));
                }
            };

            if prev.key.user_key < tail.key.user_key {
                return Some(Ok(tail));
            }
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
            let iter = MvccStream::new(iter);
            let mut forwards = iter.flatten().collect::<Vec<_>>();
            forwards.reverse();

            let iter = Box::new($v.iter().cloned().map(Ok));
            let iter = MvccStream::new(iter);
            let backwards = iter.rev().flatten().collect::<Vec<_>>();

            assert_eq!(forwards, backwards);
        };
    }

    #[test]
    #[allow(clippy::unwrap_used)]
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

        let mut iter = MvccStream::new(iter);

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
    #[allow(clippy::unwrap_used)]
    fn mvcc_queue_almost_gone_2() -> crate::Result<()> {
        let vec = [
            InternalValue::from_components("a", "a", 0, ValueType::Value),
            InternalValue::from_components("b", "", 1, ValueType::Tombstone),
            InternalValue::from_components("c", "", 1, ValueType::Tombstone),
            InternalValue::from_components("d", "", 1, ValueType::Tombstone),
            InternalValue::from_components("e", "", 1, ValueType::Tombstone),
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter);

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
    #[allow(clippy::unwrap_used)]
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

        let mut iter = MvccStream::new(iter);

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
    #[allow(clippy::unwrap_used)]
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

        let mut iter = MvccStream::new(iter);

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
    #[allow(clippy::unwrap_used)]
    fn mvcc_queue_weak_almost_gone_2() -> crate::Result<()> {
        let vec = [
            InternalValue::from_components("a", "a", 0, ValueType::Value),
            InternalValue::from_components("b", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("c", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("d", "", 1, ValueType::WeakTombstone),
            InternalValue::from_components("e", "", 1, ValueType::WeakTombstone),
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter);

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
    #[allow(clippy::unwrap_used)]
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

        let mut iter = MvccStream::new(iter);

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
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "new", "V",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 999, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
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

        let mut iter = MvccStream::new(iter);

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
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_tombstone() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
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

        let mut iter = MvccStream::new(iter);

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
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_weak_tombstone_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_weak_tombstone_resurrection() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "new", "V",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_weak_tombstone_priority() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",  
          "a", "", "W",
          "a", "new", "V",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
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

        let mut iter = MvccStream::new(iter);

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
}
