use crate::InternalValue;
use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};

// TODO: refactor error handling because it's horrible

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<InternalValue>> + 'a>;

/// Merges multiple KV iterators
pub struct Merger<'a> {
    iterators: Vec<DoubleEndedPeekable<BoxedIterator<'a>>>,
}

impl<'a> Merger<'a> {
    pub fn new(iterators: Vec<BoxedIterator<'a>>) -> Self {
        let iterators = iterators
            .into_iter()
            .map(DoubleEndedPeekableExt::double_ended_peekable)
            .collect::<Vec<_>>();

        Self { iterators }
    }

    pub fn peek(&mut self) -> Option<crate::Result<(usize, &InternalValue)>> {
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

        let mut min: Option<(usize, &InternalValue)> = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, min_val)) = min {
                            if val.key < min_val.key {
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

        min.map(Ok)
    }

    pub fn peek_back(&mut self) -> Option<crate::Result<(usize, &InternalValue)>> {
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

        let mut max: Option<(usize, &InternalValue)> = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek_back()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, max_val)) = max {
                            if val.key > max_val.key {
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

        max.map(Ok)
    }
}

impl<'a> Iterator for Merger<'a> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
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

        let mut min: Option<(usize, &InternalValue)> = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, min_val)) = min {
                            if val.key < min_val.key {
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

            Some(Ok(value))
        } else {
            None
        }
    }
}

impl<'a> DoubleEndedIterator for Merger<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
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

        let mut max: Option<(usize, &InternalValue)> = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek_back()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, max_val)) = max {
                            if val.key > max_val.key {
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

            Some(Ok(value))
        } else {
            None
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::{
//         value::{InternalValue, ValueType},
//         Slice,
//     };
//     use test_log::test;

//     macro_rules! iter_closed {
//         ($iter:expr) => {
//             assert!($iter.next().is_none(), "iterator should be closed (done)");
//             assert!(
//                 $iter.next_back().is_none(),
//                 "iterator should be closed (done)"
//             );
//         };
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_no_evict_simple_forward() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );

//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_no_evict_simple_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_no_evict_complex_forward() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );

//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_simple_forward() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );

//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_simple_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_very_simple_forward() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             iter.next().unwrap()?,
//         );

//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_extremely_simple_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_very_simple_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_mvcc_very_simple_forward() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let iter0 = Box::new(
//             vec0.iter()
//                 // NOTE: "1" because the seqno starts at 0
//                 // When we insert an item, the tree LSN is at 1
//                 // So the snapshot to get all items with seqno = 0 should have seqno = 1
//                 .filter(|x| seqno_filter(x.key.seqno, 1))
//                 .cloned()
//                 .map(Ok),
//         );

//         let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );

//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_mvcc_very_simple_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let iter0 = Box::new(
//             vec0.iter()
//                 // NOTE: "1" because the seqno starts at 0
//                 // When we insert an item, the tree LSN is at 1
//                 // So the snapshot to get all items with seqno = 0 should have seqno = 1
//                 .filter(|x| seqno_filter(x.key.seqno, 1))
//                 .cloned()
//                 .map(Ok),
//         );

//         let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_complex_forward() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
//             iter.next().unwrap()?,
//         );

//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_complex_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"new", 1, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"newest", 2, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"newest", 2, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"newest", 2, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_snapshot_simple_forward() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"", 3, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"", 2, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
//         ];

//         {
//             let iter0 = Box::new(
//                 vec0.iter()
//                     // NOTE: "1" because the seqno starts at 0
//                     // When we insert an item, the tree LSN is at 1
//                     // So the snapshot to get all items with seqno = 0 should have seqno = 1
//                     .filter(|x| seqno_filter(x.key.seqno, 1))
//                     .cloned()
//                     .map(Ok),
//             );

//             let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

//             assert_eq!(
//                 InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
//                 iter.next().unwrap()?,
//             );

//             iter_closed!(iter);
//         }

//         {
//             let iter0 = Box::new(
//                 vec0.iter()
//                     .filter(|x| seqno_filter(x.key.seqno, 2))
//                     .cloned()
//                     .map(Ok),
//             );

//             let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

//             assert_eq!(
//                 InternalValue::from_components(*b"a", *b"", 1, ValueType::Value),
//                 iter.next().unwrap()?,
//             );

//             iter_closed!(iter);
//         }

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_snapshot_simple_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"", 3, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"", 2, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
//         ];

//         {
//             let iter0 = Box::new(
//                 vec0.iter()
//                     // NOTE: "1" because the seqno starts at 0
//                     // When we insert an item, the tree LSN is at 1
//                     // So the snapshot to get all items with seqno = 0 should have seqno = 1
//                     .filter(|x| seqno_filter(x.key.seqno, 1))
//                     .cloned()
//                     .map(Ok),
//             );

//             let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

//             assert_eq!(
//                 InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
//                 iter.next_back().unwrap()?,
//             );

//             assert!(iter.next_back().is_none());
//             iter_closed!(iter);
//         }

//         {
//             let iter0 = Box::new(
//                 vec0.iter()
//                     .filter(|x| seqno_filter(x.key.seqno, 2))
//                     .cloned()
//                     .map(Ok),
//             );

//             let mut iter = MergeIterator::new(vec![iter0]).evict_old_versions(true);

//             assert_eq!(
//                 InternalValue::from_components(*b"a", *b"", 1, ValueType::Value),
//                 iter.next_back().unwrap()?,
//             );

//             assert!(iter.next_back().is_none());
//             iter_closed!(iter);
//         }

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_no_evict_tombstone_forward() -> crate::Result<()> {
//         let vec0 = [InternalValue::from_components(
//             *b"a",
//             *b"old",
//             0,
//             ValueType::Value,
//         )];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             1,
//             ValueType::Tombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"", 1, ValueType::Tombstone),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );

//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_no_evict_tombstone_reverse() -> crate::Result<()> {
//         let vec0 = [InternalValue::from_components(
//             *b"a",
//             *b"old",
//             0,
//             ValueType::Value,
//         )];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             1,
//             ValueType::Tombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(false);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"", 1, ValueType::Tombstone),
//             iter.next_back().unwrap()?,
//         );

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_tombstone_forward() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"old", 2, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             3,
//             ValueType::Tombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"", 3, ValueType::Tombstone),
//             iter.next().unwrap()?,
//         );
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_tombstone_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"old", 2, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             3,
//             ValueType::Tombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"", 3, ValueType::Tombstone),
//             iter.next_back().unwrap()?,
//         );
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_value_after_tombstone_forward() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"", 2, ValueType::Tombstone),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             1,
//             ValueType::Tombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"", 2, ValueType::Tombstone),
//             iter.next().unwrap()?,
//         );
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_value_after_tombstone_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"", 2, ValueType::Tombstone),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             1,
//             ValueType::Tombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"", 2, ValueType::Tombstone),
//             iter.next().unwrap()?,
//         );
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_evict_many_keys_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"", 1, ValueType::Tombstone),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"", 1, ValueType::Tombstone),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"", 1, ValueType::Tombstone),
//             InternalValue::from_components(*b"c", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"d", *b"", 1, ValueType::Tombstone),
//             InternalValue::from_components(*b"d", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"e", *b"", 1, ValueType::Tombstone),
//             InternalValue::from_components(*b"e", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"", 1, ValueType::Tombstone),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"", 1, ValueType::Tombstone),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"c", *b"", 1, ValueType::Tombstone),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"d", *b"", 1, ValueType::Tombstone),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"e", *b"", 1, ValueType::Tombstone),
//             iter.next().unwrap()?,
//         );

//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_snapshot_tombstone_too_new_forward() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"", 1, ValueType::Tombstone),
//             InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"", 1, ValueType::Tombstone),
//             InternalValue::from_components(*b"b", *b"", 0, ValueType::Value),
//         ];

//         let iter0 = Box::new(
//             vec0.iter()
//                 // NOTE: "1" because the seqno starts at 0
//                 // When we insert an item, the tree LSN is at 1
//                 // So the snapshot to get all items with seqno = 0 should have seqno = 1
//                 .filter(|x| seqno_filter(x.key.seqno, 1))
//                 .cloned()
//                 .map(Ok),
//         );

//         let mut iter = MergeIterator::new(vec![iter0]);

//         assert_eq!(*b"a", &*iter.next().unwrap()?.key.user_key);
//         assert_eq!(*b"b", &*iter.next().unwrap()?.key.user_key);

//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_snapshot_tombstone_too_new_reverse() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"", 1, ValueType::Tombstone),
//             InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"", 1, ValueType::Tombstone),
//             InternalValue::from_components(*b"b", *b"", 0, ValueType::Value),
//         ];

//         let iter0 = Box::new(
//             vec0.iter()
//                 // NOTE: "1" because the seqno starts at 0
//                 // When we insert an item, the tree LSN is at 1
//                 // So the snapshot to get all items with seqno = 0 should have seqno = 1
//                 .filter(|x| seqno_filter(x.key.seqno, 1))
//                 .cloned()
//                 .map(Ok),
//         );

//         let mut iter = MergeIterator::new(vec![iter0]);

//         assert_eq!(*b"b", &*iter.next_back().unwrap()?.key.user_key);
//         assert_eq!(*b"a", &*iter.next_back().unwrap()?.key.user_key);

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_ping_pong() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"", 0, ValueType::Value),
//             InternalValue::from_components(*b"c", *b"", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(*b"d", *b"", 0, ValueType::Value),
//             InternalValue::from_components(*b"e", *b"", 0, ValueType::Value),
//             InternalValue::from_components(*b"f", *b"", 0, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]);

//         assert_eq!(*b"a", &*iter.next().unwrap()?.key.user_key);
//         assert_eq!(*b"f", &*iter.next_back().unwrap()?.key.user_key);
//         assert_eq!(*b"b", &*iter.next().unwrap()?.key.user_key);
//         assert_eq!(*b"e", &*iter.next_back().unwrap()?.key.user_key);
//         assert_eq!(*b"c", &*iter.next().unwrap()?.key.user_key);
//         assert_eq!(*b"d", &*iter.next_back().unwrap()?.key.user_key);

//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     fn merge_non_overlapping() -> crate::Result<()> {
//         let iter0 = (0u64..5)
//             .map(|x| InternalValue::from_components(x.to_be_bytes(), *b"old", 0, ValueType::Value));
//         let iter1 = (5u64..10)
//             .map(|x| InternalValue::from_components(x.to_be_bytes(), *b"new", 3, ValueType::Value));
//         let iter2 = (10u64..15).map(|x| {
//             InternalValue::from_components(x.to_be_bytes(), *b"asd", 1, ValueType::Tombstone)
//         });
//         let iter3 = (15u64..20).map(|x| {
//             InternalValue::from_components(x.to_be_bytes(), *b"qwe", 2, ValueType::Tombstone)
//         });

//         let iter0 = Box::new(iter0.map(Ok));
//         let iter1 = Box::new(iter1.map(Ok));
//         let iter2 = Box::new(iter2.map(Ok));
//         let iter3 = Box::new(iter3.map(Ok));

//         let merge_iter = MergeIterator::new(vec![iter0, iter1, iter2, iter3]);

//         for (idx, item) in merge_iter.enumerate() {
//             let item = item?;
//             assert_eq!(item.key.user_key, Slice::from((idx as u64).to_be_bytes()));
//         }

//         Ok(())
//     }

//     #[test]
//     fn merge_mixed() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(1u64.to_be_bytes(), *b"old", 0, ValueType::Value),
//             InternalValue::from_components(2u64.to_be_bytes(), *b"new", 2, ValueType::Value),
//             InternalValue::from_components(3u64.to_be_bytes(), *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(1u64.to_be_bytes(), *b"new", 1, ValueType::Value),
//             InternalValue::from_components(2u64.to_be_bytes(), *b"old", 0, ValueType::Value),
//             InternalValue::from_components(3u64.to_be_bytes(), *b"new", 1, ValueType::Value),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let merge_iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);
//         let items = merge_iter.collect::<crate::Result<Vec<_>>>()?;

//         assert_eq!(
//             items,
//             vec![
//                 InternalValue::from_components(1u64.to_be_bytes(), *b"new", 1, ValueType::Value),
//                 InternalValue::from_components(2u64.to_be_bytes(), *b"new", 2, ValueType::Value),
//                 InternalValue::from_components(3u64.to_be_bytes(), *b"new", 1, ValueType::Value),
//             ]
//         );

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_weak_tombstone() {
//         let vec0 = [InternalValue::from_components(
//             *b"a",
//             *b"old",
//             0,
//             ValueType::Value,
//         )];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             1,
//             ValueType::WeakTombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         iter_closed!(iter);
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_weak_tombstone_2() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             2,
//             ValueType::WeakTombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_weak_tombstone_complex() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(*b"a", *b"", 3, ValueType::Tombstone),
//             InternalValue::from_components(*b"a", *b"", 2, ValueType::WeakTombstone),
//             InternalValue::from_components(*b"b", *b"", 2, ValueType::WeakTombstone),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"", 3, ValueType::Tombstone),
//             iter.next().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             iter.next().unwrap()?,
//         );
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_weak_tombstone_reverse() {
//         let vec0 = [InternalValue::from_components(
//             *b"a",
//             *b"old",
//             0,
//             ValueType::Value,
//         )];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             1,
//             ValueType::WeakTombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_weak_tombstone_reverse_2() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             2,
//             ValueType::WeakTombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_weak_tombstone_reverse_3() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"newnew", 3, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [InternalValue::from_components(
//             *b"a",
//             *b"",
//             2,
//             ValueType::WeakTombstone,
//         )];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"newnew", 3, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );

//         assert!(iter.next_back().is_none());
//         iter_closed!(iter);

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn merge_weak_tombstone_reverse_complex() -> crate::Result<()> {
//         let vec0 = [
//             InternalValue::from_components(*b"a", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"a", *b"old", 0, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"new", 1, ValueType::Value),
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//         ];

//         let vec1 = [
//             InternalValue::from_components(*b"a", *b"", 3, ValueType::Tombstone),
//             InternalValue::from_components(*b"a", *b"", 2, ValueType::WeakTombstone),
//             InternalValue::from_components(*b"b", *b"", 2, ValueType::WeakTombstone),
//         ];

//         let iter0 = Box::new(vec0.iter().cloned().map(Ok));
//         let iter1 = Box::new(vec1.iter().cloned().map(Ok));

//         let mut iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);

//         assert_eq!(
//             InternalValue::from_components(*b"b", *b"old", 0, ValueType::Value),
//             iter.next_back().unwrap()?,
//         );
//         assert_eq!(
//             InternalValue::from_components(*b"a", *b"", 3, ValueType::Tombstone),
//             iter.next_back().unwrap()?,
//         );
//         iter_closed!(iter);

//         Ok(())
//     }
// }
