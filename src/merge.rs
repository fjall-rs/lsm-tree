use crate::{value::SeqNo, UserKey, Value};
use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};

// TODO: use (ParsedInternalKey, UserValue) instead of Value...

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>;

/// Merges multiple iterators
///
/// This iterator can iterate through N iterators simultaneously in order
/// This is achieved by advancing the iterators that yield the lowest/highest item
/// and merging using a simple k-way merge algorithm.
///
/// If multiple iterators yield the same key value, the freshest one (highest seqno) will be picked.
#[allow(clippy::module_name_repetitions)]
pub struct MergeIterator<'a> {
    iterators: Vec<DoubleEndedPeekable<BoxedIterator<'a>>>,
    evict_old_versions: bool,
    seqno: Option<SeqNo>,
}

impl<'a> MergeIterator<'a> {
    /// Initializes a new merge iterator
    pub fn new(iterators: Vec<BoxedIterator<'a>>) -> Self {
        let iterators = iterators
            .into_iter()
            .map(DoubleEndedPeekableExt::double_ended_peekable)
            .collect::<Vec<_>>();

        Self {
            iterators,
            evict_old_versions: false,
            seqno: None,
        }
    }

    /// Evict old versions by skipping over them
    #[must_use] pub fn evict_old_versions(mut self, v: bool) -> Self {
        self.evict_old_versions = v;
        self
    }

    #[must_use] pub fn snapshot_seqno(mut self, v: SeqNo) -> Self {
        self.seqno = Some(v);
        self
    }

    fn drain_key_min(&mut self, key: &UserKey) -> crate::Result<()> {
        for iter in &mut self.iterators {
            'inner: loop {
                if let Some(item) = iter.peek() {
                    if let Ok(item) = item {
                        if &item.key == key {
                            // Consume key
                            iter.next().expect("should not be empty")?;
                        } else {
                            // Reached next key, go to next iterator
                            break 'inner;
                        }
                    } else {
                        iter.next().expect("should not be empty")?;

                        panic!("logic error");
                    }
                } else {
                    // Iterator is empty, go to next
                    break 'inner;
                }
            }
        }

        Ok(())
    }

    fn get_min(&mut self) -> Option<crate::Result<(usize, Value)>> {
        let mut idx_with_err = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek()).enumerate() {
            if let Some(val) = val {
                if val.is_err() {
                    idx_with_err = Some(idx);
                }
            }
        }

        if let Some(idx) = idx_with_err {
            let err = self
                .iterators
                .get_mut(idx)
                .expect("should exist")
                .next()
                .expect("should not be empty");

            if let Err(e) = err {
                return Some(Err(e));
            }

            panic!("logic error");
        }

        let mut min = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, min_val)) = min {
                            if val < min_val {
                                min = Some((idx, val));
                            }
                        } else {
                            min = Some((idx, val));
                        }
                    }
                    _ => panic!("already checked for errors"),
                }
            }
        }

        if let Some((idx, _)) = min {
            let value = self
                .iterators
                .get_mut(idx)?
                .next()?
                .expect("should not be error");

            Some(Ok((idx, value)))
        } else {
            None
        }
    }

    fn get_max(&mut self) -> Option<crate::Result<(usize, Value)>> {
        let mut idx_with_err = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek_back()).enumerate() {
            if let Some(val) = val {
                if val.is_err() {
                    idx_with_err = Some(idx);
                }
            }
        }

        if let Some(idx) = idx_with_err {
            let err = self
                .iterators
                .get_mut(idx)
                .expect("should exist")
                .next_back()
                .expect("should not be empty");

            if let Err(e) = err {
                return Some(Err(e));
            }

            panic!("logic error");
        }

        let mut max = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek_back()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, max_val)) = max {
                            if val > max_val {
                                max = Some((idx, val));
                            }
                        } else {
                            max = Some((idx, val));
                        }
                    }
                    _ => panic!("already checked for errors"),
                }
            }
        }

        if let Some((idx, _)) = max {
            let value = self
                .iterators
                .get_mut(idx)?
                .next_back()?
                .expect("should not be error");

            Some(Ok((idx, value)))
        } else {
            None
        }
    }

    fn peek_max(&mut self) -> Option<crate::Result<(usize, &Value)>> {
        let mut idx_with_err = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek_back()).enumerate() {
            if let Some(val) = val {
                if val.is_err() {
                    idx_with_err = Some(idx);
                }
            }
        }

        if let Some(idx) = idx_with_err {
            let err = self
                .iterators
                .get_mut(idx)
                .expect("should exist")
                .next_back()
                .expect("should not be empty");

            if let Err(e) = err {
                return Some(Err(e));
            }

            panic!("logic error");
        }

        let mut max = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek_back()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, max_val)) = max {
                            if val > max_val {
                                max = Some((idx, val));
                            }
                        } else {
                            max = Some((idx, val));
                        }
                    }
                    _ => panic!("already checked for errors"),
                }
            }
        }

        if let Some((idx, _)) = max {
            let value = self
                .iterators
                .get_mut(idx)?
                .peek_back()?
                .as_ref()
                .expect("should not be error");

            Some(Ok((idx, value)))
        } else {
            None
        }
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.get_min()? {
                Ok((_, min_item)) => {
                    if let Some(seqno) = self.seqno {
                        if min_item.seqno >= seqno {
                            // Filter out seqnos that are too high
                            continue;
                        }
                    }

                    // Tombstone marker OR we want to GC old versions
                    // As long as items beneath tombstone are the same key, ignore them
                    if self.evict_old_versions {
                        if let Err(e) = self.drain_key_min(&min_item.key) {
                            return Some(Err(e));
                        };
                    }

                    return Some(Ok(min_item));
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl<'a> DoubleEndedIterator for MergeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let mut head;

            match self.get_max()? {
                Ok((_, max_item)) => {
                    head = max_item;

                    if let Some(seqno) = self.seqno {
                        if head.seqno >= seqno {
                            // Filter out seqnos that are too high
                            continue;
                        }
                    }

                    if self.evict_old_versions {
                        'inner: while let Some(head_result) = self.peek_max() {
                            match head_result {
                                Ok((_, next)) => {
                                    if next.key == head.key {
                                        let next = self.get_max().expect("should exist");

                                        let next = match next {
                                            Ok((_, v)) => v,
                                            Err(e) => {
                                                return Some(Err(e));
                                            }
                                        };

                                        if let Some(seqno) = self.seqno {
                                            if next.seqno < seqno {
                                                head = next;
                                            }
                                        } else {
                                            // Keep popping off heap until we reach the next key
                                            // Because the seqno's are stored in descending order
                                            // The next item will definitely have a higher seqno, so
                                            // we can just take it
                                            head = next;
                                        }
                                    } else {
                                        // Reached next user key now
                                        break 'inner;
                                    }
                                }
                                Err(e) => return Some(Err(e)),
                            }
                        }
                    }

                    if let Some(seqno) = self.seqno {
                        if head.seqno >= seqno {
                            // Filter out seqnos that are too high
                            continue;
                        }
                    }

                    return Some(Ok(head));
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{Value, ValueType};
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
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

        assert_eq!(
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"c", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_no_evict_simple_reverse() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

        assert_eq!(
            Value::new(*b"c", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_no_evict_complex_forward() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"newest", 2, ValueType::Value),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"newest", 2, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"newest", 2, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

        assert_eq!(
            Value::new(*b"a", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"c", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"c", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_simple_forward() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_simple_reverse() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_very_simple_forward() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

        assert_eq!(
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_very_simple_reverse() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

        assert_eq!(
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_mvcc_very_simple_forward() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0])
            .evict_old_versions(true)
            // NOTE: "1" because the seqno starts at 0
            // When we insert an item, the tree LSN is at 1
            // So the snapshot to get all items with seqno = 0 should have seqno = 1
            .snapshot_seqno(1);

        assert_eq!(
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"c", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_mvcc_very_simple_reverse() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0])
            .evict_old_versions(true)
            // NOTE: "1" because the seqno starts at 0
            // When we insert an item, the tree LSN is at 1
            // So the snapshot to get all items with seqno = 0 should have seqno = 1
            .snapshot_seqno(1);

        assert_eq!(
            Value::new(*b"c", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_complex_forward() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"newest", 2, ValueType::Value),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"newest", 2, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"newest", 2, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            Value::new(*b"a", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"c", *b"newest", 2, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_complex_reverse() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"newest", 2, ValueType::Value),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            Value::new(*b"b", *b"newest", 2, ValueType::Value),
            Value::new(*b"b", *b"old", 0, ValueType::Value),
            Value::new(*b"c", *b"newest", 2, ValueType::Value),
            Value::new(*b"c", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            Value::new(*b"a", *b"new", 1, ValueType::Value),
            Value::new(*b"b", *b"new", 1, ValueType::Value),
            Value::new(*b"c", *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            Value::new(*b"c", *b"newest", 2, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"b", *b"newest", 2, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"newest", 2, ValueType::Value),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_snapshot_simple_forward() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"", 3, ValueType::Value),
            Value::new(*b"a", *b"", 2, ValueType::Value),
            Value::new(*b"a", *b"", 1, ValueType::Value),
            Value::new(*b"a", *b"", 0, ValueType::Value),
        ];

        {
            let iter0 = Box::new(vec0.iter().cloned().map(Ok));

            let mut iter = MergeIterator::new(vec![iter0])
                // NOTE: "1" because the seqno starts at 0
                // When we insert an item, the tree LSN is at 1
                // So the snapshot to get all items with seqno = 0 should have seqno = 1
                .snapshot_seqno(1)
                .evict_old_versions(true);

            assert_eq!(
                Value::new(*b"a", *b"", 0, ValueType::Value),
                iter.next().unwrap()?,
            );

            iter_closed!(iter);
        }

        {
            let iter0 = Box::new(vec0.iter().cloned().map(Ok));

            let mut iter = MergeIterator::new(vec![iter0])
                // NOTE: "1" because the seqno starts at 0
                // When we insert an item, the tree LSN is at 1
                // So the snapshot to get all items with seqno = 0 should have seqno = 1
                .snapshot_seqno(2)
                .evict_old_versions(true);

            assert_eq!(
                Value::new(*b"a", *b"", 1, ValueType::Value),
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
            Value::new(*b"a", *b"", 3, ValueType::Value),
            Value::new(*b"a", *b"", 2, ValueType::Value),
            Value::new(*b"a", *b"", 1, ValueType::Value),
            Value::new(*b"a", *b"", 0, ValueType::Value),
        ];

        {
            let iter0 = Box::new(vec0.iter().cloned().map(Ok));

            let mut iter = MergeIterator::new(vec![iter0])
                // NOTE: "1" because the seqno starts at 0
                // When we insert an item, the tree LSN is at 1
                // So the snapshot to get all items with seqno = 0 should have seqno = 1
                .snapshot_seqno(1)
                .evict_old_versions(true);

            assert_eq!(
                Value::new(*b"a", *b"", 0, ValueType::Value),
                iter.next_back().unwrap()?,
            );

            iter_closed!(iter);
        }

        {
            let iter0 = Box::new(vec0.iter().cloned().map(Ok));

            let mut iter = MergeIterator::new(vec![iter0])
                // NOTE: "1" because the seqno starts at 0
                // When we insert an item, the tree LSN is at 1
                // So the snapshot to get all items with seqno = 0 should have seqno = 1
                .snapshot_seqno(2)
                .evict_old_versions(true);

            assert_eq!(
                Value::new(*b"a", *b"", 1, ValueType::Value),
                iter.next_back().unwrap()?,
            );

            iter_closed!(iter);
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_no_evict_tombstone_forward() -> crate::Result<()> {
        let vec0 = [Value::new(*b"a", *b"old", 0, ValueType::Value)];

        let vec1 = [Value::new(*b"a", *b"", 1, ValueType::Tombstone)];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

        assert_eq!(
            Value::new(*b"a", *b"", 1, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_no_evict_tombstone_reverse() -> crate::Result<()> {
        let vec0 = [Value::new(*b"a", *b"old", 0, ValueType::Value)];

        let vec1 = [Value::new(*b"a", *b"", 1, ValueType::Tombstone)];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

        assert_eq!(
            Value::new(*b"a", *b"old", 0, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        assert_eq!(
            Value::new(*b"a", *b"", 1, ValueType::Tombstone),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_tombstone_forward() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"old", 2, ValueType::Value),
            Value::new(*b"a", *b"old", 1, ValueType::Value),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [Value::new(*b"a", *b"", 3, ValueType::Tombstone)];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            Value::new(*b"a", *b"", 3, ValueType::Tombstone),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_tombstone_reverse() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"old", 2, ValueType::Value),
            Value::new(*b"a", *b"old", 1, ValueType::Value),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [Value::new(*b"a", *b"", 3, ValueType::Tombstone)];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            Value::new(*b"a", *b"", 3, ValueType::Tombstone),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_value_after_tombstone_forward() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"", 2, ValueType::Tombstone),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [Value::new(*b"a", *b"", 1, ValueType::Tombstone)];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            Value::new(*b"a", *b"", 2, ValueType::Tombstone),
            iter.next().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_evict_value_after_tombstone_reverse() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"", 2, ValueType::Tombstone),
            Value::new(*b"a", *b"old", 0, ValueType::Value),
        ];

        let vec1 = [Value::new(*b"a", *b"", 1, ValueType::Tombstone)];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

        assert_eq!(
            Value::new(*b"a", *b"", 2, ValueType::Tombstone),
            iter.next_back().unwrap()?,
        );

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_snapshot_tombstone_too_new_forward() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"", 1, ValueType::Tombstone),
            Value::new(*b"a", *b"", 0, ValueType::Value),
            Value::new(*b"b", *b"", 1, ValueType::Tombstone),
            Value::new(*b"b", *b"", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0])
            // NOTE: "1" because the seqno starts at 0
            // When we insert an item, the tree LSN is at 1
            // So the snapshot to get all items with seqno = 0 should have seqno = 1
            .snapshot_seqno(1);

        assert_eq!(*b"a", &*iter.next().unwrap()?.key);
        assert_eq!(*b"b", &*iter.next().unwrap()?.key);

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_snapshot_tombstone_too_new_reverse() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"", 1, ValueType::Tombstone),
            Value::new(*b"a", *b"", 0, ValueType::Value),
            Value::new(*b"b", *b"", 1, ValueType::Tombstone),
            Value::new(*b"b", *b"", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0])
            // NOTE: "1" because the seqno starts at 0
            // When we insert an item, the tree LSN is at 1
            // So the snapshot to get all items with seqno = 0 should have seqno = 1
            .snapshot_seqno(1);

        assert_eq!(*b"b", &*iter.next_back().unwrap()?.key);
        assert_eq!(*b"a", &*iter.next_back().unwrap()?.key);

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_ping_pong() -> crate::Result<()> {
        let vec0 = [
            Value::new(*b"a", *b"", 0, ValueType::Value),
            Value::new(*b"b", *b"", 0, ValueType::Value),
            Value::new(*b"c", *b"", 0, ValueType::Value),
        ];

        let vec1 = [
            Value::new(*b"d", *b"", 0, ValueType::Value),
            Value::new(*b"e", *b"", 0, ValueType::Value),
            Value::new(*b"f", *b"", 0, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let mut iter = MergeIterator::new(vec![iter0, iter1]);

        assert_eq!(*b"a", &*iter.next().unwrap()?.key);
        assert_eq!(*b"f", &*iter.next_back().unwrap()?.key);
        assert_eq!(*b"b", &*iter.next().unwrap()?.key);
        assert_eq!(*b"e", &*iter.next_back().unwrap()?.key);
        assert_eq!(*b"c", &*iter.next().unwrap()?.key);
        assert_eq!(*b"d", &*iter.next_back().unwrap()?.key);

        iter_closed!(iter);

        Ok(())
    }

    #[test]
    fn merge_non_overlapping() -> crate::Result<()> {
        let iter0 = (0u64..5).map(|x| Value::new(x.to_be_bytes(), *b"old", 0, ValueType::Value));
        let iter1 = (5u64..10).map(|x| Value::new(x.to_be_bytes(), *b"new", 3, ValueType::Value));
        let iter2 =
            (10u64..15).map(|x| Value::new(x.to_be_bytes(), *b"asd", 1, ValueType::Tombstone));
        let iter3 =
            (15u64..20).map(|x| Value::new(x.to_be_bytes(), *b"qwe", 2, ValueType::Tombstone));

        let iter0 = Box::new(iter0.map(Ok));
        let iter1 = Box::new(iter1.map(Ok));
        let iter2 = Box::new(iter2.map(Ok));
        let iter3 = Box::new(iter3.map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1, iter2, iter3]);

        for (idx, item) in merge_iter.enumerate() {
            let item = item?;
            assert_eq!(item.key, (idx as u64).to_be_bytes().into());
        }

        Ok(())
    }

    #[test]
    fn merge_mixed() -> crate::Result<()> {
        let vec0 = [
            Value::new(1u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            Value::new(2u64.to_be_bytes(), *b"new", 2, ValueType::Value),
            Value::new(3u64.to_be_bytes(), *b"old", 0, ValueType::Value),
        ];

        let vec1 = [
            Value::new(1u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            Value::new(2u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            Value::new(3u64.to_be_bytes(), *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);
        let items = merge_iter.collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(
            items,
            vec![
                Value::new(1u64.to_be_bytes(), *b"new", 1, ValueType::Value),
                Value::new(2u64.to_be_bytes(), *b"new", 2, ValueType::Value),
                Value::new(3u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            ]
        );

        Ok(())
    }
}
