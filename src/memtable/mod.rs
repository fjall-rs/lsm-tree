use crate::key::InternalKey;
use crate::mvcc_stream::MvccStream;
use crate::segment::block::ItemSize;
use crate::value::{InternalValue, SeqNo, UserValue, ValueType};
use crossbeam_skiplist::SkipMap;
use std::ops::RangeBounds;
use std::sync::atomic::AtomicU32;

struct DoubleEndedWrapper<I>(I);

impl<I> Iterator for DoubleEndedWrapper<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<I> DoubleEndedIterator for DoubleEndedWrapper<I>
where
    I: Iterator,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        None
    }
}

/// The memtable serves as an intermediary storage for new items
#[derive(Default)]
pub struct MemTable {
    #[doc(hidden)]
    items: SkipMap<InternalKey, UserValue>,

    /// Approximate active memtable size
    ///
    /// If this grows too large, a flush is triggered
    pub(crate) approximate_size: AtomicU32,
}

impl MemTable {
    /// Creates an iterator over all items.
    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = InternalValue> + '_ {
        self.items.iter().map(|entry| InternalValue {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }

    /// Creates an iterator over a range of items.
    pub(crate) fn range<'a, R: RangeBounds<InternalKey> + 'a>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = InternalValue> + '_ {
        self.items.range(range).map(|entry| InternalValue {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }

    /// Returns the item by key if it exists.
    ///
    /// The item with the highest seqno will be returned, if `seqno` is None.
    #[doc(hidden)]
    pub fn get<K: AsRef<[u8]>>(&self, key: K, seqno: Option<SeqNo>) -> Option<InternalValue> {
        let prefix = key.as_ref();

        // NOTE: This range start deserves some explanation...
        // InternalKeys are multi-sorted by 2 categories: user_key and Reverse(seqno). (tombstone doesn't really matter)
        // We search for the lowest entry that is greater or equal the user's prefix key
        // and has the highest seqno (because the seqno is stored in reverse order)
        //
        // Example: We search for "abc"
        //
        // key -> seqno
        //
        // a   -> 7
        // abc -> 5 <<< This is the lowest key (highest seqno) that matches the range
        // abc -> 4
        // abc -> 3
        // abcdef -> 6
        // abcdef -> 5
        //
        let lower_bound = InternalKey::new(prefix, SeqNo::MAX, ValueType::Value);

        let iter = self
            .items
            .range(lower_bound..)
            .take_while(|entry| {
                let key = entry.key();
                &*key.user_key == prefix
            })
            .filter_map(move |entry| {
                let key = entry.key();

                // Check for seqno if needed
                if let Some(seqno) = seqno {
                    if key.seqno < seqno {
                        Some(InternalValue {
                            key: entry.key().clone(),
                            value: entry.value().clone(),
                        })
                    } else {
                        None
                    }
                } else {
                    Some(InternalValue {
                        key: entry.key().clone(),
                        value: entry.value().clone(),
                    })
                }
            })
            .map(Ok);

        // NOTE: Wrap it in a stupid adapter to make it "double ended" again...
        // but we never call next_back anyways
        let iter = DoubleEndedWrapper(iter);

        // NOTE: We need to unwrap the return value again... memtables are not fallible, so it cannot panic
        #[allow(clippy::expect_used)]
        MvccStream::new(iter)
            .next()
            .map(|x| x.expect("cannot fail"))
    }

    /// Gets approximate size of memtable in bytes.
    pub fn size(&self) -> u32 {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Counts the amount of items in the memtable.
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if the memtable is empty.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Inserts an item into the memtable
    #[doc(hidden)]
    pub fn insert(&self, item: InternalValue) -> (u32, u32) {
        // NOTE: Value length is u32 max
        #[allow(clippy::cast_possible_truncation)]
        let item_size = item.size() as u32;

        let size_before = self
            .approximate_size
            .fetch_add(item_size, std::sync::atomic::Ordering::AcqRel);

        let key = InternalKey::new(item.key.user_key, item.key.seqno, item.key.value_type);
        self.items.insert(key, item.value);

        (item_size, size_before + item_size)
    }

    /// Returns the highest sequence number in the memtable.
    pub fn get_highest_seqno(&self) -> Option<SeqNo> {
        self.items
            .iter()
            .map(|x| {
                let key = x.key();
                key.seqno
            })
            .max()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::ValueType;
    use test_log::test;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn memtable_mvcc_point_read() {
        let memtable = MemTable::default();

        memtable.insert(InternalValue::from_components(
            *b"hello-key-999991",
            *b"hello-value-999991",
            0,
            ValueType::Value,
        ));

        let item = memtable.get("hello-key-99999", None);
        assert_eq!(None, item);

        let item = memtable.get("hello-key-999991", None);
        assert_eq!(*b"hello-value-999991", &*item.unwrap().value);

        memtable.insert(InternalValue::from_components(
            *b"hello-key-999991",
            *b"hello-value-999991-2",
            1,
            ValueType::Value,
        ));

        let item = memtable.get("hello-key-99999", None);
        assert_eq!(None, item);

        let item = memtable.get("hello-key-999991", None);
        assert_eq!((*b"hello-value-999991-2"), &*item.unwrap().value);

        let item = memtable.get("hello-key-99999", Some(1));
        assert_eq!(None, item);

        let item = memtable.get("hello-key-999991", Some(1));
        assert_eq!((*b"hello-value-999991"), &*item.unwrap().value);

        let item = memtable.get("hello-key-99999", Some(2));
        assert_eq!(None, item);

        let item = memtable.get("hello-key-999991", Some(2));
        assert_eq!((*b"hello-value-999991-2"), &*item.unwrap().value);
    }

    #[test]
    fn memtable_get() {
        let memtable = MemTable::default();

        let value =
            InternalValue::from_components(b"abc".to_vec(), b"abc".to_vec(), 0, ValueType::Value);

        memtable.insert(value.clone());

        assert_eq!(Some(value), memtable.get("abc", None));
    }

    #[test]
    fn memtable_get_highest_seqno() {
        let memtable = MemTable::default();

        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            1,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            2,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            3,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            4,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                4,
                ValueType::Value,
            )),
            memtable.get("abc", None)
        );
    }

    #[test]
    fn memtable_get_prefix() {
        let memtable = MemTable::default();

        memtable.insert(InternalValue::from_components(
            b"abc0".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            255,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                255,
                ValueType::Value,
            )),
            memtable.get("abc", None)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc0".to_vec(),
                b"abc".to_vec(),
                0,
                ValueType::Value,
            )),
            memtable.get("abc0", None)
        );
    }

    #[test]
    fn memtable_get_old_version() {
        let memtable = MemTable::default();

        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            99,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            255,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                255,
                ValueType::Value,
            )),
            memtable.get("abc", None)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                99,
                ValueType::Value,
            )),
            memtable.get("abc", Some(100))
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                0,
                ValueType::Value,
            )),
            memtable.get("abc", Some(50))
        );
    }
}
