use crate::{InternalValue, SeqNo, UserKey, ValueType};
use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};

// TODO: need to differentiate between evicting tombstones and evicting nothing at all
// TODO: even if it's not the last level, we may want to drop weak tombstones...
// TODO: weak tombstone may have to depend on seqno that we can free...

// TODO: port remaining tests from merge.rs

#[must_use]
pub fn seqno_filter(item_seqno: SeqNo, seqno: SeqNo) -> bool {
    item_seqno < seqno
}

/// Consumes a stream of KVs and emits a new stream according to MVCC and tombstone rules
#[allow(clippy::module_name_repetitions)]
pub struct MvccStream<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> {
    inner: DoubleEndedPeekable<I>,
    gc_seqno_threshold: Option<SeqNo>,
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> MvccStream<I> {
    /// Initializes a new merge iterator
    #[must_use]
    pub fn new(iter: I) -> Self {
        let iter = iter.double_ended_peekable();

        Self {
            inner: iter,
            gc_seqno_threshold: None,
        }
    }

    /// Evict old versions by skipping over them, if they are older than this threshold.
    #[must_use]
    pub fn gc_seqno_threshold(mut self, seqno: SeqNo) -> Self {
        self.gc_seqno_threshold = Some(seqno);
        self
    }

    fn drain_key_min(&mut self, key: &UserKey) -> crate::Result<()> {
        loop {
            let Some(next) = self.inner.peek() else {
                return Ok(());
            };

            let Ok(next) = next else {
                return Err(self
                    .inner
                    .next()
                    .expect("should exist")
                    .expect_err("should be error"));
            };

            if &next.key.user_key == key {
                // Consume key
                self.inner.next().expect("should not be empty")?;
            } else {
                return Ok(());
            }
        }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> Iterator for MvccStream<I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(seqno) = self.gc_seqno_threshold {
            loop {
                let head = fail_iter!(self.inner.next()?);

                // TODO: unit test

                if let Some(peeked) = self.inner.peek() {
                    let peeked = match peeked {
                        Ok(v) => v,
                        Err(_) => {
                            return Some(Err(self
                                .inner
                                .next()
                                .expect("value should exist")
                                .expect_err("should be error")))
                        }
                    };

                    // NOTE: Only item of this key and thus latest version, so return it no matter what
                    if peeked.key.user_key > head.key.user_key {
                        return Some(Ok(head));
                    }

                    // NOTE: Next item is expired is expired,
                    // so the tail of this user key is entirely expired, so drain it all
                    if peeked.key.seqno < seqno {
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

        loop {
            let head = fail_iter!(self.inner.next()?);

            // Weak tombstone ("Single delete") logic
            if head.key.value_type == ValueType::WeakTombstone {
                let next = match self.inner.peek() {
                    Some(Ok(next)) => next,
                    Some(Err(_)) => {
                        return Some(Err(self
                            .inner
                            .next()
                            .expect("should exist")
                            .expect_err("should be error")))
                    }
                    None => return Some(Ok(head)),
                };

                if next.key.value_type == ValueType::Value && next.key.user_key == head.key.user_key
                {
                    // Consume value
                    fail_iter!(self.inner.next()?);
                }

                continue;
            }

            // As long as items are the same key, ignore them
            fail_iter!(self.drain_key_min(&head.key.user_key));

            return Some(Ok(head));
        }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> DoubleEndedIterator
    for MvccStream<I>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(_seqno) = self.gc_seqno_threshold {
            panic!("Should probably implement this, but it's not really needed right now");
            // return self.inner.next_back();
        }

        // NOTE: Many versions are pretty unlikely, so we can probably skip a lot of heap allocations
        // when there are only 1-5 versions.
        let mut stack: smallvec::SmallVec<[InternalValue; 5]> = smallvec::smallvec![];

        loop {
            let tail = fail_iter!(self.inner.next_back()?);

            let prev = match self.inner.peek_back() {
                Some(Ok(prev)) => prev,
                Some(Err(_)) => {
                    return Some(Err(self
                        .inner
                        .next_back()
                        .expect("should exist")
                        .expect_err("should be error")))
                }
                None => {
                    if tail.key.value_type == ValueType::WeakTombstone {
                        return stack.pop().map(Ok);
                    }
                    return Some(Ok(tail));
                }
            };

            if prev.key.user_key < tail.key.user_key {
                if tail.key.value_type == ValueType::WeakTombstone {
                    return stack.pop().map(Ok);
                }
                return Some(Ok(tail));
            }

            if !tail.is_tombstone() {
                stack.push(tail);
            }

            if prev.key.value_type == ValueType::WeakTombstone {
                stack.pop();
            }
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

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_no_evict_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "old", "V",
          "b", "old", "V",
          "c", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));
        let mut iter = MvccStream::new(iter).gc_seqno_threshold(0);

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
        assert!(iter.next().is_none(), "iterator should be closed (done)");

        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_no_evict_simple_multi_keys() -> crate::Result<()> {
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

        let mut iter = MvccStream::new(iter).gc_seqno_threshold(0);

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
        assert!(iter.next().is_none(), "iterator should be closed (done)");

        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_evict_simple() -> crate::Result<()> {
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

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_evict_simple_multi_keys() -> crate::Result<()> {
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

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_evict_tombstone() -> crate::Result<()> {
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

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_evict_tombstone_multi_keys() -> crate::Result<()> {
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

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_evict_weak_tombstone_simple() {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter);

        iter_closed!(iter);

        test_reverse!(vec);
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_evict_weak_tombstone_resurrection() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "W",
          "a", "new", "V",
          "a", "old", "V",
        ];

        let iter = Box::new(vec.iter().cloned().map(Ok));

        let mut iter = MvccStream::new(iter);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 997, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        test_reverse!(vec);

        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_evict_weak_tombstone_priority() -> crate::Result<()> {
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

    #[test_log::test]
    #[allow(clippy::unwrap_used)]
    fn mvcc_stream_evict_weak_tombstone_multi_keys() {
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

        iter_closed!(iter);

        test_reverse!(vec);
    }
}
