use super::value_block::ValueBlock;
use crate::{UserKey, Value};
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
        let mut lo = start_key
            .as_ref()
            .map_or(0, |key| inner.items.partition_point(|x| &*x.key < key));

        let hi = end_key.as_ref().map_or_else(
            || inner.items.len() - 1,
            |key| {
                let idx = inner.items.partition_point(|x| &*x.key <= key);

                if idx == 0 {
                    let first = inner
                        .items
                        .first()
                        .expect("value block should not be empty");

                    if &*first.key > key {
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
    type Item = Value;

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
mod tests {
    use super::*;
    use crate::segment::block::header::Header;
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

    fn block(items: Vec<Value>) -> ValueBlock {
        ValueBlock {
            header: Header {
                compression: crate::segment::meta::CompressionType::Lz4,
                crc: 0,
                data_length: 0,
                previous_block_offset: 0,
            },
            items: items.into_boxed_slice(),
        }
    }

    #[test]
    fn new_block_consumer_simple() {
        let block = block(vec![
            Value::new(*b"a", vec![], 0, crate::ValueType::Value),
            Value::new(*b"b", vec![], 0, crate::ValueType::Value),
            Value::new(*b"c", vec![], 0, crate::ValueType::Value),
            Value::new(*b"d", vec![], 0, crate::ValueType::Value),
            Value::new(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter = ValueBlockConsumer::new(block.into());
        assert_eq!(*b"a", &*iter.next().expect("should exist").key);
        assert_eq!(*b"b", &*iter.next().expect("should exist").key);
        assert_eq!(*b"c", &*iter.next().expect("should exist").key);
        assert_eq!(*b"d", &*iter.next().expect("should exist").key);
        assert_eq!(*b"e", &*iter.next().expect("should exist").key);
        iter_closed!(iter);
    }

    #[test]
    fn new_block_consumer_simple_rev() {
        let block = block(vec![
            Value::new(*b"a", vec![], 0, crate::ValueType::Value),
            Value::new(*b"b", vec![], 0, crate::ValueType::Value),
            Value::new(*b"c", vec![], 0, crate::ValueType::Value),
            Value::new(*b"d", vec![], 0, crate::ValueType::Value),
            Value::new(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter = ValueBlockConsumer::new(block.into());
        assert_eq!(*b"e", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"d", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"c", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"b", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"a", &*iter.next_back().expect("should exist").key);
        iter_closed!(iter);
    }

    #[test]
    fn new_block_consumer_simple_ping_pong() {
        let block = block(vec![
            Value::new(*b"a", vec![], 0, crate::ValueType::Value),
            Value::new(*b"b", vec![], 0, crate::ValueType::Value),
            Value::new(*b"c", vec![], 0, crate::ValueType::Value),
            Value::new(*b"d", vec![], 0, crate::ValueType::Value),
            Value::new(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter = ValueBlockConsumer::new(block.clone().into());
        assert_eq!(*b"a", &*iter.next().expect("should exist").key);
        assert_eq!(*b"e", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"b", &*iter.next().expect("should exist").key);
        assert_eq!(*b"d", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"c", &*iter.next().expect("should exist").key);
        iter_closed!(iter);

        let mut iter = ValueBlockConsumer::new(block.into());
        assert_eq!(*b"e", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"a", &*iter.next().expect("should exist").key);
        assert_eq!(*b"d", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"b", &*iter.next().expect("should exist").key);
        assert_eq!(*b"c", &*iter.next_back().expect("should exist").key);
        iter_closed!(iter);
    }

    #[test]
    fn new_block_consumer_start_key() {
        let block = block(vec![
            Value::new(*b"a", vec![], 0, crate::ValueType::Value),
            Value::new(*b"b", vec![], 0, crate::ValueType::Value),
            Value::new(*b"c", vec![], 0, crate::ValueType::Value),
            Value::new(*b"d", vec![], 0, crate::ValueType::Value),
            Value::new(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.clone().into(), &Some(Arc::from(*b"c")), &None);
        assert_eq!(*b"c", &*iter.next().expect("should exist").key);
        assert_eq!(*b"d", &*iter.next().expect("should exist").key);
        assert_eq!(*b"e", &*iter.next().expect("should exist").key);
        iter_closed!(iter);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.into(), &Some(Arc::from(*b"c")), &None);
        assert_eq!(*b"e", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"d", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"c", &*iter.next_back().expect("should exist").key);
        iter_closed!(iter);
    }

    #[test]
    fn new_block_consumer_end_key() {
        let block = block(vec![
            Value::new(*b"a", vec![], 0, crate::ValueType::Value),
            Value::new(*b"b", vec![], 0, crate::ValueType::Value),
            Value::new(*b"c", vec![], 0, crate::ValueType::Value),
            Value::new(*b"d", vec![], 0, crate::ValueType::Value),
            Value::new(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.clone().into(), &None, &Some(Arc::from(*b"c")));
        assert_eq!(*b"a", &*iter.next().expect("should exist").key);
        assert_eq!(*b"b", &*iter.next().expect("should exist").key);
        assert_eq!(*b"c", &*iter.next().expect("should exist").key);
        iter_closed!(iter);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.into(), &None, &Some(Arc::from(*b"c")));
        assert_eq!(*b"c", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"b", &*iter.next_back().expect("should exist").key);
        assert_eq!(*b"a", &*iter.next_back().expect("should exist").key);
        iter_closed!(iter);
    }

    #[test]
    fn new_block_consumer_no_range_end() {
        let block = block(vec![
            Value::new(*b"b", vec![], 0, crate::ValueType::Value),
            Value::new(*b"c", vec![], 0, crate::ValueType::Value),
            Value::new(*b"d", vec![], 0, crate::ValueType::Value),
            Value::new(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.clone().into(), &None, &Some(Arc::from(*b"a")));
        iter_closed!(iter);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.into(), &None, &Some(Arc::from(*b"a"))).rev();
        iter_closed!(iter);
    }

    #[test]
    fn new_block_consumer_no_range_start() {
        let block = block(vec![
            Value::new(*b"a", vec![], 0, crate::ValueType::Value),
            Value::new(*b"b", vec![], 0, crate::ValueType::Value),
            Value::new(*b"c", vec![], 0, crate::ValueType::Value),
            Value::new(*b"d", vec![], 0, crate::ValueType::Value),
            Value::new(*b"e", vec![], 0, crate::ValueType::Value),
        ]);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.clone().into(), &Some(Arc::from(*b"f")), &None);
        iter_closed!(iter);

        let mut iter =
            ValueBlockConsumer::with_bounds(block.into(), &Some(Arc::from(*b"f")), &None).rev();
        iter_closed!(iter);
    }
}
