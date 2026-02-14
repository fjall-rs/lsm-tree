// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{InternalValue, SeqNo, UserKey, UserValue, ValueType};
use std::iter::Peekable;

type Item = crate::Result<InternalValue>;

/// A callback that receives all expired KVs
///
/// Used for counting blobs that are not referenced anymore because of
/// vHandles that are being dropped through compaction.
pub trait DroppedKvCallback {
    fn on_dropped(&mut self, kv: &InternalValue);
}

/// Verdict returned by [`StreamFilter`]
#[derive(Debug)]
pub enum StreamFilterVerdict {
    /// Keep the item as is.
    Keep,
    /// Replace the item.
    Replace((ValueType, UserValue)),
    /// Drop the item without leaving a tombstone.
    Drop,
}

/// A callback for modifying KVs in the stream.
pub trait StreamFilter {
    /// Handle an item, possibly modifying it.
    fn filter_item(&mut self, item: &InternalValue) -> crate::Result<StreamFilterVerdict>;
}

/// A [`StreamFilter`] that does not modify anything.
pub struct NoFilter;

impl StreamFilter for NoFilter {
    fn filter_item(&mut self, _item: &InternalValue) -> crate::Result<StreamFilterVerdict> {
        Ok(StreamFilterVerdict::Keep)
    }
}

/// Consumes a stream of KVs and emits a new stream according to GC and tombstone rules
///
/// This iterator is used during flushing & compaction.
pub struct CompactionStream<'a, I: Iterator<Item = Item>, F: StreamFilter = NoFilter> {
    /// KV stream
    inner: Peekable<I>,

    /// MVCC watermark to get rid of old versions
    gc_seqno_threshold: SeqNo,

    /// Event emitter that receives all expired KVs
    dropped_callback: Option<&'a mut dyn DroppedKvCallback>,

    /// Stream filter
    filter: F,

    evict_tombstones: bool,

    zero_seqnos: bool,
}

impl<I: Iterator<Item = Item>> CompactionStream<'_, I, NoFilter> {
    /// Initializes a new merge iterator
    #[must_use]
    pub fn new(iter: I, gc_seqno_threshold: SeqNo) -> Self {
        let iter = iter.peekable();

        Self {
            inner: iter,
            gc_seqno_threshold,
            dropped_callback: None,
            filter: NoFilter,
            evict_tombstones: false,
            zero_seqnos: false,
        }
    }
}

impl<'a, I: Iterator<Item = Item>, F: StreamFilter + 'a> CompactionStream<'a, I, F> {
    /// Installs a filter into this stream
    pub fn with_filter<NF: StreamFilter>(self, filter: NF) -> CompactionStream<'a, I, NF> {
        CompactionStream {
            inner: self.inner,
            gc_seqno_threshold: self.gc_seqno_threshold,
            dropped_callback: self.dropped_callback,
            filter,
            evict_tombstones: self.evict_tombstones,
            zero_seqnos: self.zero_seqnos,
        }
    }

    pub fn evict_tombstones(mut self, b: bool) -> Self {
        self.evict_tombstones = b;
        self
    }

    /// Installs a callback that receives all expired KVs.
    pub fn with_drop_callback(mut self, cb: &'a mut dyn DroppedKvCallback) -> Self {
        self.dropped_callback = Some(cb);
        self
    }

    /// NOTE: Convert sequence number to zero if it is below the snapshot watermark.
    ///
    /// This can save a lot of space, because "0" only takes 1 byte, and sequence numbers are monotonically increasing.
    pub fn zero_seqnos(mut self, b: bool) -> Self {
        self.zero_seqnos = b;
        self
    }

    /// Drains the remaining versions of the given key.
    fn drain_key(&mut self, key: &UserKey) -> crate::Result<()> {
        loop {
            let Some(next) = self.inner.next_if(|kv| {
                if let Ok(kv) = kv {
                    let expired = kv.key.user_key == key;

                    if expired {
                        if let Some(watcher) = &mut self.dropped_callback {
                            watcher.on_dropped(kv);
                        }
                    }

                    expired
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

impl<'a, I: Iterator<Item = Item>, F: StreamFilter + 'a> Iterator for CompactionStream<'a, I, F> {
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut head = fail_iter!(self.inner.next()?);

            if !head.is_tombstone() {
                match fail_iter!(self.filter.filter_item(&head)) {
                    StreamFilterVerdict::Keep => { /* do nothing */ }
                    StreamFilterVerdict::Replace((new_type, new_value)) => {
                        // if we are replacing this item's value, call the dropped callback for the previous item
                        if let Some(watcher) = &mut self.dropped_callback {
                            watcher.on_dropped(&head);
                        }
                        head.value = new_value;
                        head.key.value_type = new_type;
                    }
                    StreamFilterVerdict::Drop => {
                        if let Some(watcher) = &mut self.dropped_callback {
                            watcher.on_dropped(&head);
                        }
                        // ignore
                        continue;
                    }
                }
            }

            if let Some(peeked) = self.inner.peek() {
                let Ok(peeked) = peeked else {
                    #[expect(
                        clippy::expect_used,
                        reason = "we just asserted, the peeked value is an error"
                    )]
                    return Some(Err(self
                        .inner
                        .next()
                        .expect("value should exist")
                        .expect_err("should be error")));
                };

                if peeked.key.user_key > head.key.user_key {
                    if head.is_tombstone() && self.evict_tombstones {
                        continue;
                    }

                    // NOTE: Only item of this key and thus latest version, so return it no matter what
                    // ...
                } else if peeked.key.seqno < self.gc_seqno_threshold {
                    if head.key.value_type == ValueType::Tombstone && self.evict_tombstones {
                        fail_iter!(self.drain_key(&head.key.user_key));
                        continue;
                    }

                    // NOTE: If next item is an actual value, and current value is weak tombstone,
                    // drop the tombstone
                    let drop_weak_tombstone = peeked.key.value_type == ValueType::Value
                        && head.key.value_type == ValueType::WeakTombstone;

                    // NOTE: Next item is expired,
                    // so the tail of this user key is entirely expired, so drain it all
                    fail_iter!(self.drain_key(&head.key.user_key));

                    if drop_weak_tombstone {
                        continue;
                    }
                }
            } else if head.is_tombstone() && self.evict_tombstones {
                continue;
            }

            if self.zero_seqnos && head.key.seqno < self.gc_seqno_threshold {
                head.key.seqno = 0;
            }

            return Some(Ok(head));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{value::InternalValue, ValueType};
    use test_log::test;

    macro_rules! stream {
        ($($key:expr, $sub_key:expr, $value_type:expr),* $(,)?) => {{
            let mut values = Vec::new();
            let mut counters = std::collections::HashMap::new();

            $(
                #[expect(clippy::string_lit_as_bytes)]
                let key = $key.as_bytes();

                #[expect(clippy::string_lit_as_bytes)]
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

    #[derive(Default)]
    struct TrackCallback {
        items: Vec<InternalValue>,
    }

    impl DroppedKvCallback for TrackCallback {
        fn on_dropped(&mut self, kv: &InternalValue) {
            self.items.push(kv.clone());
        }
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_expired_callback_1() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",
          "a", "", "T",
          "a", "", "T",
        ];

        let mut my_watcher = TrackCallback::default();

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_drop_callback(&mut my_watcher);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        assert_eq!(
            [
                InternalValue::from_components("a", "", 998, ValueType::Value),
                InternalValue::from_components("a", "", 997, ValueType::Value),
            ],
            &*my_watcher.items,
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_seqno_zeroing_1() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "3", "V",
          "a", "2", "V",
          "a", "1", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).zero_seqnos(true);

        assert_eq!(
            InternalValue::from_components(*b"a", *b"3", 0, ValueType::Value),
            iter.next().unwrap()?,
        );
        iter_closed!(iter);

        Ok(())
    }

    #[test]
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
    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_tombstone_no_gc() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
          "a", "", "T",
          "b", "", "T",
          "c", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000_000);

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

    #[test]
    #[expect(clippy::unwrap_used)]
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

    #[test]
    #[expect(clippy::unwrap_used)]
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

    #[test]
    #[expect(clippy::unwrap_used)]
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

    #[test]
    #[expect(clippy::unwrap_used)]
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

    #[test]
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

    #[test]
    #[expect(clippy::unwrap_used)]
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

    #[test]
    #[expect(clippy::unwrap_used)]
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

    #[test]
    #[expect(clippy::unwrap_used)]
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

    #[test]
    fn compaction_stream_filter_1() {
        struct Filter(&'static [u8]);
        impl StreamFilter for Filter {
            fn filter_item(&mut self, value: &InternalValue) -> crate::Result<StreamFilterVerdict> {
                if value.key.user_key == b"b" {
                    Ok(StreamFilterVerdict::Drop)
                } else if value.value < self.0 {
                    Ok(StreamFilterVerdict::Replace((
                        ValueType::Tombstone,
                        UserValue::empty(),
                    )))
                } else {
                    Ok(StreamFilterVerdict::Keep)
                }
            }
        }

        #[rustfmt::skip]
        let vec = stream![
            "a", "9", "V",
            "a", "8", "V",
            "a", "7", "V",
            // subsequent values will be filtered out
            "a", "6", "V",
            "a", "5", "V",
            // subsequent values below gc threshold after filter
            "a", "4", "V",

            // this value will be dropped without leaving a tombstone
            "b", "b", "V",
        ];

        let mut drop_cb = TrackCallback { items: vec![] };
        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 995)
            .with_filter(Filter(b"7"))
            .with_drop_callback(&mut drop_cb);

        let out: Vec<_> = iter.map(Result::unwrap).collect();

        #[rustfmt::skip]
        assert_eq!(out, stream![
            "a", "9", "V",
            "a", "8", "V",
            "a", "7", "V",
            "a", "", "T",
            "a", "", "T",
        ]);

        let fc = InternalValue::from_components;

        #[rustfmt::skip]
        assert_eq!(drop_cb.items, [
            fc(b"a", b"6", 996, ValueType::Value),
            fc(b"a", b"5", 995, ValueType::Value),
            fc(b"a", b"4", 994, ValueType::Value),
            fc(b"b", b"b", 999, ValueType::Value),
        ]);
    }

    pub mod custom_mvcc {
        use super::*;
        use byteorder::{ReadBytesExt, BE};
        use test_log::test;

        fn kv(key: &[u8], seqno: SeqNo, value: &[u8], tomb: bool) -> InternalValue {
            InternalValue::from_components(
                {
                    let mut v: Vec<u8> = vec![];
                    v.extend(key);
                    v.extend((!seqno).to_be_bytes()); // IMPORTANT: Invert the seqno for correct descending sort
                    v.push(if tomb { 1 } else { 0 });
                    v
                },
                value,
                2_353, // does not matter for us
                ValueType::Value,
            )
        }

        struct Filter {
            /// The previous user key
            ///
            /// Note that the user key is NOT the full KV key
            /// because we embed MVCC information into the key (user_key#seqno#type).
            prev_user_key: Option<UserKey>,

            /// MVCC watermark we can safely delete if an item < watermark
            /// is covered by a newer version.
            mvcc_watermark: SeqNo,
        }

        impl StreamFilter for Filter {
            fn filter_item(&mut self, value: &InternalValue) -> crate::Result<StreamFilterVerdict> {
                let l = value.key.user_key.len();

                match &self.prev_user_key {
                    Some(prev) => {
                        let user_key = &value.key.user_key[..l - 9];

                        if prev == &user_key {
                            // We found another, older version of the previous key
                            let mut seqno = &value.key.user_key[l - 9..l - 1];
                            // IMPORTANT: Invert the seqno back to normal value
                            let seqno = !seqno.read_u64::<BE>().unwrap();

                            if seqno < self.mvcc_watermark {
                                return Ok(StreamFilterVerdict::Drop);
                            }
                        } else {
                            let user_key = &value.key.user_key.slice(..l - 9);
                            self.prev_user_key = Some(user_key.clone());
                        }
                    }
                    None => {
                        let user_key = &value.key.user_key.slice(..l - 9);
                        self.prev_user_key = Some(user_key.clone());
                    }
                }

                Ok(StreamFilterVerdict::Keep)
            }
        }

        #[test]
        fn compaction_filter_custom_mvcc() {
            let vec = vec![
                kv(b"abc", 4, b"c", false),
                kv(b"abc", 3, b"b", false),
                kv(b"abc", 2, b"a", false),
            ];

            let mut drop_cb = TrackCallback { items: vec![] };
            let iter = vec.iter().cloned().map(Ok);
            let iter = CompactionStream::new(iter, 995)
                .with_filter(Filter {
                    mvcc_watermark: 5,
                    prev_user_key: None,
                })
                .with_drop_callback(&mut drop_cb);

            let out: Vec<_> = iter.map(Result::unwrap).collect();

            #[rustfmt::skip]
            assert_eq!(out, vec![
                kv(b"abc", 4, b"c", false),
            ]);
        }

        #[test]
        fn compaction_filter_custom_mvcc_multi_keys() {
            let vec = vec![
                kv(b"a", 4, b"c", false),
                kv(b"a", 3, b"b", false),
                kv(b"a", 2, b"a", false),
                //
                kv(b"b", 4, b"c", false),
                kv(b"b", 3, b"b", false),
                kv(b"b", 2, b"a", false),
                //
                kv(b"c", 1, b"c", false),
                //
                kv(b"d", 0, b"c", false),
            ];

            let mut drop_cb = TrackCallback { items: vec![] };
            let iter = vec.iter().cloned().map(Ok);
            let iter = CompactionStream::new(iter, 995)
                .with_filter(Filter {
                    mvcc_watermark: 3,
                    prev_user_key: None,
                })
                .with_drop_callback(&mut drop_cb);

            let out: Vec<_> = iter.map(Result::unwrap).collect();

            #[rustfmt::skip]
            assert_eq!(out, vec![
                kv(b"a", 4, b"c", false),
                kv(b"a", 3, b"b", false),
                //
                kv(b"b", 4, b"c", false),
                kv(b"b", 3, b"b", false),
                //
                kv(b"c", 1, b"c", false),
                //
                kv(b"d", 0, b"c", false),
            ]);
        }
    }
}
