// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::value_block::ValueBlock;
use crate::{value::InternalValue, UserKey};
use std::sync::Arc;

pub struct ValueBlockConsumer {
    pub(crate) inner: Arc<ValueBlock>,
    lo: usize,
    hi: usize,
}

impl ValueBlockConsumer {
    #[must_use]
    pub fn new(inner: Arc<ValueBlock>) -> Self {
        Self::with_bounds(inner, &None, &None)
    }

    #[must_use]
    pub fn with_bounds(
        inner: Arc<ValueBlock>,
        start_key: &Option<UserKey>,
        end_key: &Option<UserKey>,
    ) -> Self {
        let mut lo = start_key.as_ref().map_or(0, |key| {
            inner.items.partition_point(|x| &*x.key.user_key < *key)
        });

        let hi = end_key.as_ref().map_or_else(
            || inner.items.len() - 1,
            |key| {
                let idx = inner.items.partition_point(|x| &*x.key.user_key <= *key);

                if idx == 0 {
                    let first = inner
                        .items
                        .first()
                        .expect("value block should not be empty");

                    if &*first.key.user_key > *key {
                        lo = 1;
                    }
                }

                idx.saturating_sub(1)
            },
        );

        Self { inner, lo, hi }
    }
}

impl Iterator for ValueBlockConsumer {
    type Item = InternalValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.lo > self.hi {
            None
        } else {
            let item = self.inner.items.get(self.lo)?;
            self.lo += 1;

            Some(item.clone())
        }
    }
}

impl DoubleEndedIterator for ValueBlockConsumer {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.hi < self.lo {
            None
        } else {
            let item = self.inner.items.get(self.hi)?;

            if self.hi == 0 {
                // Prevent underflow
                self.lo += 1;
            } else {
                self.hi -= 1;
            }

            Some(item.clone())
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{
        segment::block::{checksum::Checksum, header::Header},
        Slice,
    };
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

    fn block(items: Vec<InternalValue>) -> ValueBlock {
        ValueBlock {
            header: Header {
                compression: crate::segment::meta::CompressionType::None,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                previous_block_offset: 0,
                uncompressed_length: 0,
            },
            items: items.into_boxed_slice(),
        }
    }

    #[test]
    fn block_consumer_simple() {
        let block = block(vec![
            InternalValue::from_components(*b"a", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"b", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"c", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"d", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter = ValueBlockConsumer::new(block.into());
        assert_eq!(*b"a", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(*b"b", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(*b"c", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(*b"d", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(*b"e", &*iter.next().expect("should exist").key.user_key);
        iter_closed!(iter);
    }

    #[test]
    fn block_consumer_simple_rev() {
        let block = block(vec![
            InternalValue::from_components(*b"a", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"b", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"c", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"d", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter = ValueBlockConsumer::new(block.into());
        assert_eq!(
            *b"e",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(
            *b"d",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(
            *b"c",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(
            *b"b",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(
            *b"a",
            &*iter.next_back().expect("should exist").key.user_key
        );
        iter_closed!(iter);
    }

    #[test]
    fn block_consumer_simple_ping_pong() {
        let block = block(vec![
            InternalValue::from_components(*b"a", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"b", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"c", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"d", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter = ValueBlockConsumer::new(block.clone().into());
        assert_eq!(*b"a", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(
            *b"e",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(*b"b", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(
            *b"d",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(*b"c", &*iter.next().expect("should exist").key.user_key);
        iter_closed!(iter);

        let mut iter = ValueBlockConsumer::new(block.into());
        assert_eq!(
            *b"e",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(*b"a", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(
            *b"d",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(*b"b", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(
            *b"c",
            &*iter.next_back().expect("should exist").key.user_key
        );
        iter_closed!(iter);
    }

    #[test]
    fn block_consumer_start_key() {
        let block = block(vec![
            InternalValue::from_components(*b"a", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"b", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"c", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"d", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.clone().into(), &Some(Slice::from(*b"c")), &None);
        assert_eq!(*b"c", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(*b"d", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(*b"e", &*iter.next().expect("should exist").key.user_key);
        iter_closed!(iter);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.into(), &Some(Slice::from(*b"c")), &None);
        assert_eq!(
            *b"e",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(
            *b"d",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(
            *b"c",
            &*iter.next_back().expect("should exist").key.user_key
        );
        iter_closed!(iter);
    }

    #[test]
    fn block_consumer_end_key() {
        let block = block(vec![
            InternalValue::from_components(*b"a", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"b", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"c", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"d", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.clone().into(), &None, &Some(Slice::from(*b"c")));
        assert_eq!(*b"a", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(*b"b", &*iter.next().expect("should exist").key.user_key);
        assert_eq!(*b"c", &*iter.next().expect("should exist").key.user_key);
        iter_closed!(iter);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.into(), &None, &Some(Slice::from(*b"c")));
        assert_eq!(
            *b"c",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(
            *b"b",
            &*iter.next_back().expect("should exist").key.user_key
        );
        assert_eq!(
            *b"a",
            &*iter.next_back().expect("should exist").key.user_key
        );
        iter_closed!(iter);
    }

    #[test]
    fn block_consumer_no_range_end() {
        let block = block(vec![
            InternalValue::from_components(*b"b", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"c", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"d", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.clone().into(), &None, &Some(Slice::from(*b"a")));
        iter_closed!(iter);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.into(), &None, &Some(Slice::from(*b"a"))).rev();
        iter_closed!(iter);
    }

    #[test]
    fn block_consumer_no_range_start() {
        let block = block(vec![
            InternalValue::from_components(*b"a", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"b", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"c", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"d", vec![], 0, crate::ValueType::Value),
            InternalValue::from_components(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.clone().into(), &Some(Slice::from(*b"f")), &None);
        iter_closed!(iter);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.into(), &Some(Slice::from(*b"f")), &None).rev();
        iter_closed!(iter);
    }
}
