use crate::{
    merge_peekable::MergePeekable,
    value::{InternalValue, SeqNo},
};

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<InternalValue>> + 'a>;

#[must_use]
pub fn seqno_filter(item_seqno: SeqNo, seqno: SeqNo) -> bool {
    item_seqno < seqno
}

/// Merges multiple iterators
///
/// This iterator can iterate through N iterators simultaneously in order
/// This is achieved by advancing the iterators that yield the lowest/highest item
/// and merging using a simple k-way merge algorithm.
///
/// If multiple iterators yield the same key value, the freshest one (highest seqno) will be picked.
#[allow(clippy::module_name_repetitions)]
pub struct MergeIterator<'a> {
    //iterators: Vec<DoubleEndedPeekable<BoxedIterator<'a>>>,
    inner: MergePeekable<'a>,
    evict_old_versions: bool,
}

impl<'a> MergeIterator<'a> {
    /// Initializes a new merge iterator
    #[must_use]
    pub fn new(iterators: Vec<BoxedIterator<'a>>) -> Self {
        Self {
            inner: MergePeekable::new(iterators),
            evict_old_versions: false,
        }
    }

    /// Evict old versions by skipping over them
    #[must_use]
    pub fn evict_old_versions(mut self, v: bool) -> Self {
        self.evict_old_versions = v;
        self
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok(min_item) => {
                // Tombstone marker OR we want to GC old versions
                // As long as items beneath tombstone are the same key, ignore them
                if self.evict_old_versions {
                    if let Err(e) = self.inner.drain_key_min(&min_item.key.user_key) {
                        return Some(Err(e));
                    };
                }

                Some(Ok(min_item))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> DoubleEndedIterator for MergeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut head;

        match self.inner.next_back()? {
            Ok(max_item) => {
                head = max_item;

                // TODO: function... drain...max?
                if self.evict_old_versions {
                    'inner: while let Some(head_result) = self.inner.peek_back() {
                        match head_result {
                            Ok((_, next)) => {
                                if next.key.user_key == head.key.user_key {
                                    let next = self.inner.next_back().expect("should exist");

                                    let next = match next {
                                        Ok(v) => v,
                                        Err(e) => {
                                            return Some(Err(e));
                                        }
                                    };

                                    // Keep popping off heap until we reach the next key
                                    // Because the seqno's are stored in descending order
                                    // The next item will definitely have a higher seqno, so
                                    // we can just take it
                                    head = next;
                                } else {
                                    // Reached next user key now
                                    break 'inner;
                                }
                            }
                            Err(e) => return Some(Err(e)),
                        }
                    }
                }

                Some(Ok(head))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{InternalValue, ValueType};
    use test_log::test;

    macro_rules! iter_closed {
        ($iter:expr) => {
            assert!($iter.next().is_none(), "iterator should be closed (done)");
            assert!(
                $iter.next_back().is_none(),
                "iterator should be closed (done)"
            );
        };
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_no_evict_simple_forward() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_no_evict_simple_reverse() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

        assert_eq!(
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_no_evict_complex_forward() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_simple_forward() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_simple_reverse() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_very_simple_forward() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_extremely_simple_reverse() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_very_simple_reverse() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_mvcc_very_simple_forward() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let iter0 = Box::new(
            vec0.iter()
                // NOTE: "1" because the seqno starts at 0
                // When we insert an item, the tree LSN is at 1
                // So the snapshot to get all items with seqno = 0 should have seqno = 1
                .filter(|x| seqno_filter(x.key.seqno, 1))
                .cloned()
                .map(Ok),
        );

        let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_mvcc_very_simple_reverse() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let iter0 = Box::new(
            vec0.iter()
                // NOTE: "1" because the seqno starts at 0
                // When we insert an item, the tree LSN is at 1
                // So the snapshot to get all items with seqno = 0 should have seqno = 1
                .filter(|x| seqno_filter(x.key.seqno, 1))
                .cloned()
                .map(Ok),
        );

        let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_complex_forward() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_complex_reverse() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
            InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
            InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
            InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_snapshot_simple_forward() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"", 3, ValueType::Value),
            InternalValue::from_components(*b"a", *b"", 2, ValueType::Value),
            InternalValue::from_components(*b"a", *b"", 1, ValueType::Value),
            InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
        ];

        {
            let iter0 = Box::new(
                vec0.iter()
                    // NOTE: "1" because the seqno starts at 0
                    // When we insert an item, the tree LSN is at 1
                    // So the snapshot to get all items with seqno = 0 should have seqno = 1
                    .filter(|x| seqno_filter(x.key.seqno, 1))
                    .cloned()
                    .map(Ok),
            );

            let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

            assert_eq!(
                InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
                iter.next().unwrap()?,
            );

            iter_closed!(iter);
        }

        {
            let iter0 = Box::new(
                vec0.iter()
                    .filter(|x| seqno_filter(x.key.seqno, 2))
                    .cloned()
                    .map(Ok),
            );

            let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

            assert_eq!(
                InternalValue::from_components(*b"a", *b"", 1, ValueType::Value),
                iter.next().unwrap()?,
            );

            iter_closed!(iter);
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_snapshot_simple_reverse() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"", 3, ValueType::Value),
            InternalValue::from_components(*b"a", *b"", 2, ValueType::Value),
            InternalValue::from_components(*b"a", *b"", 1, ValueType::Value),
            InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
        ];

        {
            let iter0 = Box::new(
                vec0.iter()
                    // NOTE: "1" because the seqno starts at 0
                    // When we insert an item, the tree LSN is at 1
                    // So the snapshot to get all items with seqno = 0 should have seqno = 1
                    .filter(|x| seqno_filter(x.key.seqno, 1))
                    .cloned()
                    .map(Ok),
            );

            let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

            assert_eq!(
                InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
                iter.next_back().unwrap()?,
            );

            iter_closed!(iter);
        }

        {
            let iter0 = Box::new(
                vec0.iter()
                    .filter(|x| seqno_filter(x.key.seqno, 2))
                    .cloned()
                    .map(Ok),
            );

            let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

            assert_eq!(
                InternalValue::from_components(*b"a", *b"", 1, ValueType::Value),
                iter.next_back().unwrap()?,
            );

            iter_closed!(iter);
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_no_evict_tombstone_forward() -> crate::Result<()> {
        let vec0 = [InternalValue::from_components(
            *b"a",
            *b"old",
            0,
            ValueType::Value,
        )];

        let vec1 = [InternalValue::from_components(
            *b"a",
            *b"",
            1,
            ValueType::Tombstone,
        )];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_no_evict_tombstone_reverse() -> crate::Result<()> {
        let vec0 = [InternalValue::from_components(
            *b"a",
            *b"old",
            0,
            ValueType::Value,
        )];

        let vec1 = [InternalValue::from_components(
            *b"a",
            *b"",
            1,
            ValueType::Tombstone,
        )];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 1, ValueType::Tombstone),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_tombstone_forward() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"old", 2, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 1, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [InternalValue::from_components(
            *b"a",
            *b"",
            3,
            ValueType::Tombstone,
        )];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 3, ValueType::Tombstone),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_tombstone_reverse() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"old", 2, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 1, ValueType::Value),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [InternalValue::from_components(
            *b"a",
            *b"",
            3,
            ValueType::Tombstone,
        )];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 3, ValueType::Tombstone),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_value_after_tombstone_forward() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"", 2, ValueType::Tombstone),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [InternalValue::from_components(
            *b"a",
            *b"",
            1,
            ValueType::Tombstone,
        )];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 2, ValueType::Tombstone),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_value_after_tombstone_reverse() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"", 2, ValueType::Tombstone),
            InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [InternalValue::from_components(
            *b"a",
            *b"",
            1,
            ValueType::Tombstone,
        )];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 2, ValueType::Tombstone),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_snapshot_tombstone_too_new_forward() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"", 1, ValueType::Tombstone),
            InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"", 1, ValueType::Tombstone),
            InternalValue::from_components(*b"b", *b"", 0, ValueType::Value),
        ];

        let iter0 = Box::new(
            vec0.iter()
                // NOTE: "1" because the seqno starts at 0
                // When we insert an item, the tree LSN is at 1
                // So the snapshot to get all items with seqno = 0 should have seqno = 1
                .filter(|x| seqno_filter(x.key.seqno, 1))
                .cloned()
                .map(Ok),
        );

        let mut iter = MergeIterator::new(vec![iter0]);

        assert_eq!(*b"a", &*iter.next().unwrap()?.key.user_key);
        assert_eq!(*b"b", &*iter.next().unwrap()?.key.user_key);

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_snapshot_tombstone_too_new_reverse() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"", 1, ValueType::Tombstone),
            InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"", 1, ValueType::Tombstone),
            InternalValue::from_components(*b"b", *b"", 0, ValueType::Value),
        ];

        let iter0 = Box::new(
            vec0.iter()
                // NOTE: "1" because the seqno starts at 0
                // When we insert an item, the tree LSN is at 1
                // So the snapshot to get all items with seqno = 0 should have seqno = 1
                .filter(|x| seqno_filter(x.key.seqno, 1))
                .cloned()
                .map(Ok),
        );

        let mut iter = MergeIterator::new(vec![iter0]);

        assert_eq!(*b"b", &*iter.next_back().unwrap()?.key.user_key);
        assert_eq!(*b"a", &*iter.next_back().unwrap()?.key.user_key);

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_ping_pong() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
            InternalValue::from_components(*b"b", *b"", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"", 0, ValueType::Value),
        ];

        let vec1 = [
            InternalValue::from_components(*b"d", *b"", 0, ValueType::Value),
            InternalValue::from_components(*b"e", *b"", 0, ValueType::Value),
            InternalValue::from_components(*b"f", *b"", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]);

        assert_eq!(*b"a", &*iter.next().unwrap()?.key.user_key);
        assert_eq!(*b"f", &*iter.next_back().unwrap()?.key.user_key);
        assert_eq!(*b"b", &*iter.next().unwrap()?.key.user_key);
        assert_eq!(*b"e", &*iter.next_back().unwrap()?.key.user_key);
        assert_eq!(*b"c", &*iter.next().unwrap()?.key.user_key);
        assert_eq!(*b"d", &*iter.next_back().unwrap()?.key.user_key);

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    fn merge_non_overlapping() -> crate::Result<()> {
        let iter0 = (0u64..5)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), *b"old", 0, ValueType::Value));
        let iter1 = (5u64..10)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), *b"new", 3, ValueType::Value));
        let iter2 = (10u64..15).map(|x| {
            InternalValue::from_components(x.to_be_bytes(), *b"asd", 1, ValueType::Tombstone)
        });
        let iter3 = (15u64..20).map(|x| {
            InternalValue::from_components(x.to_be_bytes(), *b"qwe", 2, ValueType::Tombstone)
        });

        let iter0 = Box::new(iter0.map(Ok));
        let iter1 = Box::new(iter1.map(Ok));
        let iter2 = Box::new(iter2.map(Ok));
        let iter3 = Box::new(iter3.map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1, iter2, iter3]);

        for (idx, item) in merge_iter.enumerate() {
            let item = item?;
            assert_eq!(item.key.user_key, (idx as u64).to_be_bytes().into());
        }

        Ok(())
    }

    #[test]
    fn merge_mixed() -> crate::Result<()> {
        let vec0 = [
            InternalValue::from_components(1u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            InternalValue::from_components(2u64.to_be_bytes(), *b"new", 2, ValueType::Value),
            InternalValue::from_components(3u64.to_be_bytes(), *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            InternalValue::from_components(1u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            InternalValue::from_components(2u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            InternalValue::from_components(3u64.to_be_bytes(), *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);
        let items = merge_iter.collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(
            items,
            vec![
                InternalValue::from_components(1u64.to_be_bytes(), *b"new", 1, ValueType::Value),
                InternalValue::from_components(2u64.to_be_bytes(), *b"new", 2, ValueType::Value),
                InternalValue::from_components(3u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            ]
        );

        Ok(())
    }
}
