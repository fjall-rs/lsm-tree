// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::InternalValue;
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

type Item = InternalValue;

/// Clips an iterator to a key range
pub struct ClippingIter<'a, K, R, I>
where
    K: AsRef<[u8]>,
    R: RangeBounds<K>,
    I: DoubleEndedIterator<Item = Item>,
{
    _phantom: std::marker::PhantomData<K>,

    inner: I,
    range: &'a R,

    has_entered_lo: bool,
    has_entered_hi: bool,
}

impl<'a, K, R, I> ClippingIter<'a, K, R, I>
where
    K: AsRef<[u8]>,
    R: RangeBounds<K>,
    I: DoubleEndedIterator<Item = Item>,
{
    pub fn new(iter: I, range: &'a R) -> Self {
        Self {
            _phantom: PhantomData,

            inner: iter,
            range,

            has_entered_lo: false,
            has_entered_hi: false,
        }
    }
}

impl<K, R, I> Iterator for ClippingIter<'_, K, R, I>
where
    K: AsRef<[u8]>,
    R: RangeBounds<K>,
    I: DoubleEndedIterator<Item = Item>,
{
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.inner.next()?;

            // NOTE: PERF: As soon as we enter ->[lo..]
            // we don't need to do key comparisons anymore which are
            // more expensive than a simple flag check, especially for long keys
            if !self.has_entered_lo {
                match self.range.start_bound() {
                    Bound::Included(start) => {
                        if item.key.user_key < start.as_ref() {
                            // Before min key
                            continue;
                        }
                        self.has_entered_lo = true;
                    }
                    Bound::Excluded(start) => {
                        if item.key.user_key <= start.as_ref() {
                            // Before or equal min key
                            continue;
                        }
                        self.has_entered_lo = true;
                    }
                    Bound::Unbounded => {}
                }
            }

            match self.range.end_bound() {
                Bound::Included(start) => {
                    if item.key.user_key > start.as_ref() {
                        // After max key
                        return None;
                    }
                }
                Bound::Excluded(start) => {
                    if item.key.user_key >= start.as_ref() {
                        // Reached max key
                        return None;
                    }
                }
                Bound::Unbounded => {}
            }

            return Some(item);
        }
    }
}

impl<K, R, I> DoubleEndedIterator for ClippingIter<'_, K, R, I>
where
    K: AsRef<[u8]>,
    R: RangeBounds<K>,
    I: DoubleEndedIterator<Item = Item>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.inner.next_back()?;

            match self.range.start_bound() {
                Bound::Included(start) => {
                    if item.key.user_key < start.as_ref() {
                        // Reached min key
                        return None;
                    }
                }
                Bound::Excluded(start) => {
                    if item.key.user_key <= start.as_ref() {
                        // Before min key
                        return None;
                    }
                }
                Bound::Unbounded => {}
            }

            // NOTE: PERF: As soon as we enter [..hi]<-
            // we don't need to do key comparisons anymore which are
            // more expensive than a simple flag check, especially for long keys
            if !self.has_entered_hi {
                match self.range.end_bound() {
                    Bound::Included(end) => {
                        if item.key.user_key > end.as_ref() {
                            // After max key
                            continue;
                        }
                        self.has_entered_hi = true;
                    }
                    Bound::Excluded(end) => {
                        if item.key.user_key >= end.as_ref() {
                            // After or equal max key
                            continue;
                        }
                        self.has_entered_hi = true;
                    }
                    Bound::Unbounded => {}
                }
            }

            return Some(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn v3_clipping_iter_forwards() {
        let items = [
            InternalValue::from_components(b"a", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"b", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"c", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"d", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"e", b"", 0, crate::ValueType::Value),
        ];
        let range = "c"..="d";

        let mut iter = ClippingIter::new(items.into_iter(), &range);
        assert_eq!(
            Some(b"c" as &[u8]),
            iter.next().map(|x| x.key.user_key).as_deref(),
        );
        assert_eq!(
            Some(b"d" as &[u8]),
            iter.next().map(|x| x.key.user_key).as_deref(),
        );
        assert!(iter.next().is_none());
    }

    #[test]
    fn v3_clipping_iter_rev() {
        let items = [
            InternalValue::from_components(b"a", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"b", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"c", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"d", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"e", b"", 0, crate::ValueType::Value),
        ];
        let range = "c"..="d";

        let mut iter = ClippingIter::new(items.into_iter(), &range);
        assert_eq!(
            Some(b"d" as &[u8]),
            iter.next_back().map(|x| x.key.user_key).as_deref(),
        );
        assert_eq!(
            Some(b"c" as &[u8]),
            iter.next_back().map(|x| x.key.user_key).as_deref(),
        );
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn v3_clipping_iter_ping_pong() {
        let items = [
            InternalValue::from_components(b"a", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"b", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"c", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"d", b"", 0, crate::ValueType::Value),
            InternalValue::from_components(b"e", b"", 0, crate::ValueType::Value),
        ];
        let range = "b"..="d";

        let mut iter = ClippingIter::new(items.into_iter(), &range);
        assert_eq!(
            Some(b"b" as &[u8]),
            iter.next().map(|x| x.key.user_key).as_deref(),
        );
        assert_eq!(
            Some(b"d" as &[u8]),
            iter.next_back().map(|x| x.key.user_key).as_deref(),
        );
        assert_eq!(
            Some(b"c" as &[u8]),
            iter.next().map(|x| x.key.user_key).as_deref(),
        );
        assert!(iter.next_back().is_none());
        assert!(iter.next().is_none());
    }
}
