// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod compression;
mod gc;
pub mod index;
pub mod value;

use crate::{
    coding::{Decode, Encode},
    compaction::stream::CompactionStream,
    file::BLOBS_FOLDER,
    r#abstract::{AbstractTree, RangeItem},
    tree::inner::MemtableId,
    value::InternalValue,
    Config, KvPair, Memtable, SegmentId, SeqNo, Slice, Snapshot, UserKey, UserValue, ValueType,
};
use compression::MyCompressor;
use gc::{reader::GcReader, writer::GcWriter};
use index::IndexTree;
use std::{
    io::Cursor,
    ops::RangeBounds,
    sync::{Arc, RwLockWriteGuard},
};
use value::MaybeInlineValue;
use value_log::ValueLog;

fn resolve_value_handle(vlog: &ValueLog<MyCompressor>, item: RangeItem) -> RangeItem {
    match item {
        Ok((key, value)) => {
            let mut cursor = Cursor::new(value);
            let item = MaybeInlineValue::decode_from(&mut cursor)?;

            match item {
                MaybeInlineValue::Inline(bytes) => Ok((key, bytes)),
                MaybeInlineValue::Indirect { vhandle, .. } => match vlog.get(&vhandle) {
                    Ok(Some(bytes)) => Ok((key, bytes)),
                    Err(e) => Err(e.into()),
                    _ => panic!("value handle did not match any blob - this is a bug"),
                },
            }
        }
        Err(e) => Err(e),
    }
}

/// A key-value-separated log-structured merge tree
///
/// This tree is a composite structure, consisting of an
/// index tree (LSM-tree) and a log-structured value log
/// to reduce write amplification.
///
/// See <https://docs.rs/value-log> for more information.
#[derive(Clone)]
pub struct BlobTree {
    /// Index tree that holds value handles or small inline values
    #[doc(hidden)]
    pub index: IndexTree,

    /// Log-structured value-log that stores large values
    #[doc(hidden)]
    pub blobs: ValueLog<MyCompressor>,
}

impl BlobTree {
    pub(crate) fn open(config: Config) -> crate::Result<Self> {
        let path = &config.path;

        let vlog_path = path.join(BLOBS_FOLDER);
        let vlog_cfg = value_log::Config::<MyCompressor>::default()
            .blob_cache(config.blob_cache.clone())
            .segment_size_bytes(config.blob_file_target_size)
            .compression(MyCompressor(config.blob_compression));

        let index: IndexTree = config.open()?.into();

        Ok(Self {
            index,
            blobs: ValueLog::open(vlog_path, vlog_cfg)?,
        })
    }

    /// Scans the index tree, collecting statistics about
    /// value log fragmentation
    #[doc(hidden)]
    pub fn gc_scan_stats(&self, seqno: SeqNo) -> crate::Result<crate::GcReport> {
        use std::io::{Error as IoError, ErrorKind as IoErrorKind};
        use MaybeInlineValue::{Indirect, Inline};

        // IMPORTANT: Lock + snapshot memtable to avoid read skew + preventing tampering with memtable
        let _memtable_lock = self.index.read_lock_active_memtable();
        let snapshot = self.index.snapshot(seqno);

        self.blobs
            .scan_for_stats(snapshot.iter().filter_map(|kv| {
                let Ok((_, v)) = kv else {
                    return Some(Err(IoError::new(
                        IoErrorKind::Other,
                        "Failed to load KV pair from index tree",
                    )));
                };

                let mut cursor = Cursor::new(v);
                let value = match MaybeInlineValue::decode_from(&mut cursor) {
                    Ok(v) => v,
                    Err(e) => return Some(Err(IoError::new(IoErrorKind::Other, e.to_string()))),
                };

                match value {
                    Indirect { vhandle, size } => Some(Ok((vhandle, size))),
                    Inline(_) => None,
                }
            }))
            .map_err(Into::into)
    }

    pub fn apply_gc_strategy(
        &self,
        strategy: &impl value_log::GcStrategy<MyCompressor>,
        seqno: SeqNo,
    ) -> crate::Result<u64> {
        // IMPORTANT: Write lock memtable to avoid read skew
        let memtable_lock = self.index.lock_active_memtable();

        self.blobs.apply_gc_strategy(
            strategy,
            &GcReader::new(&self.index, &memtable_lock),
            GcWriter::new(seqno, &memtable_lock),
        )?;

        // NOTE: We still have the memtable lock, can't use gc_drop_stale because recursive locking
        self.blobs.drop_stale_segments().map_err(Into::into)
    }

    /// Drops all stale blob segment files
    #[doc(hidden)]
    pub fn gc_drop_stale(&self) -> crate::Result<u64> {
        // IMPORTANT: Write lock memtable to avoid read skew
        let _lock = self.index.lock_active_memtable();

        self.blobs.drop_stale_segments().map_err(Into::into)
    }

    #[doc(hidden)]
    pub fn flush_active_memtable(
        &self,
        eviction_seqno: SeqNo,
    ) -> crate::Result<Option<Arc<crate::Segment>>> {
        let Some((segment_id, yanked_memtable)) = self.index.rotate_memtable() else {
            return Ok(None);
        };

        let Some(segment) = self.flush_memtable(segment_id, &yanked_memtable, eviction_seqno)?
        else {
            return Ok(None);
        };
        self.register_segments(&[segment.clone()])?;

        Ok(Some(segment))
    }
}

impl AbstractTree for BlobTree {
    #[cfg(feature = "bloom")]
    fn bloom_filter_size(&self) -> usize {
        self.index.bloom_filter_size()
    }

    fn sealed_memtable_count(&self) -> usize {
        self.index.sealed_memtable_count()
    }

    fn is_first_level_disjoint(&self) -> bool {
        self.index.is_first_level_disjoint()
    }

    /* fn import<P: AsRef<Path>>(&self, path: P) -> crate::Result<()> {
        import_tree(path, self)
    } */

    #[doc(hidden)]
    fn verify(&self) -> crate::Result<usize> {
        let index_tree_sum = self.index.verify()?;
        let vlog_sum = self.blobs.verify()?;
        Ok(index_tree_sum + vlog_sum)
    }

    fn keys_with_seqno(
        &self,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static> {
        self.index.keys_with_seqno(seqno, index)
    }

    fn values_with_seqno(
        &self,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static> {
        Box::new(
            self.iter_with_seqno(seqno, index)
                .map(|x| x.map(|(_, v)| v)),
        )
    }

    fn keys(&self) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static> {
        self.index.keys()
    }

    fn values(&self) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static> {
        Box::new(self.iter().map(|x| x.map(|(_, v)| v)))
    }

    fn flush_memtable(
        &self,
        segment_id: SegmentId,
        memtable: &Arc<Memtable>,
        eviction_seqno: SeqNo,
    ) -> crate::Result<Option<Arc<crate::Segment>>> {
        use crate::{
            file::SEGMENTS_FOLDER,
            segment::writer::{Options, Writer as SegmentWriter},
        };
        use value::MaybeInlineValue;

        let lsm_segment_folder = self.index.config.path.join(SEGMENTS_FOLDER);

        log::debug!("flushing memtable & performing key-value separation");
        log::debug!("=> to LSM segments in {:?}", lsm_segment_folder);
        log::debug!("=> to blob segment at {:?}", self.blobs.path);

        let mut segment_writer = SegmentWriter::new(Options {
            segment_id,
            data_block_size: self.index.config.data_block_size,
            index_block_size: self.index.config.index_block_size,
            folder: lsm_segment_folder,
        })?
        .use_compression(self.index.config.compression);

        #[cfg(feature = "bloom")]
        {
            segment_writer = segment_writer.use_bloom_policy(
                crate::segment::writer::BloomConstructionPolicy::FpRate(0.0001),
            );
        }

        let mut blob_writer = self.blobs.get_writer()?;

        let iter = memtable.iter().map(Ok);
        let compaction_filter = CompactionStream::new(iter, eviction_seqno);

        for item in compaction_filter {
            let item = item?;

            if item.is_tombstone() {
                // NOTE: Still need to add tombstone to index tree
                // But no blob to blob writer
                segment_writer.write(InternalValue::new(item.key, vec![]))?;
                continue;
            }

            let mut cursor = Cursor::new(item.value);

            let value = MaybeInlineValue::decode_from(&mut cursor)?;
            let value = match value {
                MaybeInlineValue::Inline(value) => value,
                indirection @ MaybeInlineValue::Indirect { .. } => {
                    // NOTE: This is a previous indirection, just write it to index tree
                    // without writing the blob again

                    let mut serialized_indirection = vec![];
                    indirection.encode_into(&mut serialized_indirection)?;

                    segment_writer
                        .write(InternalValue::new(item.key.clone(), serialized_indirection))?;

                    continue;
                }
            };

            // NOTE: Values are 32-bit max
            #[allow(clippy::cast_possible_truncation)]
            let value_size = value.len() as u32;

            if value_size >= self.index.config.blob_file_separation_threshold {
                let vhandle = blob_writer.get_next_value_handle();

                let indirection = MaybeInlineValue::Indirect {
                    vhandle,
                    size: value_size,
                };
                let mut serialized_indirection = vec![];
                indirection.encode_into(&mut serialized_indirection)?;

                segment_writer
                    .write(InternalValue::new(item.key.clone(), serialized_indirection))?;

                blob_writer.write(&item.key.user_key, value)?;
            } else {
                let direct = MaybeInlineValue::Inline(value);
                let serialized_direct = direct.encode_into_vec()?;
                segment_writer.write(InternalValue::new(item.key, serialized_direct))?;
            }
        }

        log::trace!("Register blob writer into value log");
        self.blobs.register_writer(blob_writer)?;

        log::trace!("Creating segment");
        self.index.consume_writer(segment_id, segment_writer)
    }

    fn register_segments(&self, segments: &[Arc<crate::Segment>]) -> crate::Result<()> {
        self.index.register_segments(segments)
    }

    fn lock_active_memtable(&self) -> std::sync::RwLockWriteGuard<'_, Memtable> {
        self.index.lock_active_memtable()
    }

    fn set_active_memtable(&self, memtable: Memtable) {
        self.index.set_active_memtable(memtable);
    }

    fn add_sealed_memtable(&self, id: MemtableId, memtable: Arc<Memtable>) {
        self.index.add_sealed_memtable(id, memtable);
    }

    fn compact(
        &self,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<()> {
        self.index.compact(strategy, seqno_threshold)
    }

    fn get_next_segment_id(&self) -> SegmentId {
        self.index.get_next_segment_id()
    }

    fn tree_config(&self) -> &Config {
        &self.index.config
    }

    fn get_highest_seqno(&self) -> Option<SeqNo> {
        self.index.get_highest_seqno()
    }

    fn active_memtable_size(&self) -> u32 {
        self.index.active_memtable_size()
    }

    fn tree_type(&self) -> crate::TreeType {
        crate::TreeType::Blob
    }

    fn rotate_memtable(&self) -> Option<(crate::tree::inner::MemtableId, Arc<crate::Memtable>)> {
        self.index.rotate_memtable()
    }

    fn segment_count(&self) -> usize {
        self.index.segment_count()
    }

    fn first_level_segment_count(&self) -> usize {
        self.index.first_level_segment_count()
    }

    fn approximate_len(&self) -> usize {
        self.index.approximate_len()
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster key reads
    fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.index.contains_key(key)
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster key reads
    fn contains_key_with_seqno<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<bool> {
        self.index.contains_key_with_seqno(key, seqno)
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster scans
    fn len(&self) -> crate::Result<usize> {
        self.index.len()
    }

    #[must_use]
    fn disk_space(&self) -> u64 {
        self.index.disk_space() + self.blobs.manifest.disk_space_used()
    }

    fn get_highest_memtable_seqno(&self) -> Option<SeqNo> {
        self.index.get_highest_memtable_seqno()
    }

    fn get_highest_persisted_seqno(&self) -> Option<SeqNo> {
        self.index.get_highest_persisted_seqno()
    }

    fn snapshot(&self, seqno: SeqNo) -> Snapshot {
        use crate::AnyTree::Blob;

        Snapshot::new(Blob(self.clone()), seqno)
    }

    fn iter_with_seqno(
        &self,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static> {
        self.range_with_seqno::<UserKey, _>(.., seqno, index)
    }

    fn range_with_seqno<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static> {
        let vlog = self.blobs.clone();
        Box::new(
            self.index
                .0
                .create_range(&range, Some(seqno), index)
                .map(move |item| resolve_value_handle(&vlog, item)),
        )
    }

    fn prefix_with_seqno<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static> {
        let vlog = self.blobs.clone();
        Box::new(
            self.index
                .0
                .create_prefix(prefix, Some(seqno), index)
                .map(move |item| resolve_value_handle(&vlog, item)),
        )
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static> {
        let vlog = self.blobs.clone();
        Box::new(
            self.index
                .0
                .create_range(&range, None, None)
                .map(move |item| resolve_value_handle(&vlog, item)),
        )
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static> {
        let vlog = self.blobs.clone();
        Box::new(
            self.index
                .0
                .create_prefix(prefix, None, None)
                .map(move |item| resolve_value_handle(&vlog, item)),
        )
    }

    fn raw_insert_with_lock<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        lock: &RwLockWriteGuard<'_, Memtable>,
        key: K,
        value: V,
        seqno: SeqNo,
        r#type: ValueType,
    ) -> (u32, u32) {
        use value::MaybeInlineValue;

        // NOTE: Initially, we always write an inline value
        // On memtable flush, depending on the values' sizes, they will be separated
        // into inline or indirect values
        let item = MaybeInlineValue::Inline(value.as_ref().into());

        let value = item.encode_into_vec().expect("should serialize");

        let value = InternalValue::from_components(key.as_ref(), value, seqno, r#type);
        lock.insert(value)
    }

    fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V, seqno: SeqNo) -> (u32, u32) {
        use value::MaybeInlineValue;

        // NOTE: Initially, we always write an inline value
        // On memtable flush, depending on the values' sizes, they will be separated
        // into inline or indirect values
        let item = MaybeInlineValue::Inline(value.as_ref().into());

        let value = item.encode_into_vec().expect("should serialize");

        self.index.insert(key, value, seqno)
    }

    fn get_with_seqno<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: SeqNo,
    ) -> crate::Result<Option<crate::UserValue>> {
        use value::MaybeInlineValue::{Indirect, Inline};

        let Some(value) = self.index.get_internal_with_seqno(key.as_ref(), seqno)? else {
            return Ok(None);
        };

        match value {
            Inline(bytes) => Ok(Some(bytes)),
            Indirect { vhandle, .. } => {
                // Resolve indirection using value log
                Ok(self.blobs.get(&vhandle)?.map(Slice::from))
            }
        }
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<Slice>> {
        use value::MaybeInlineValue::{Indirect, Inline};

        let Some(value) = self.index.get_internal(key.as_ref())? else {
            return Ok(None);
        };

        match value {
            Inline(bytes) => Ok(Some(bytes)),
            Indirect { vhandle, .. } => {
                // Resolve indirection using value log
                Ok(self.blobs.get(&vhandle)?.map(Slice::from))
            }
        }
    }

    fn remove<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> (u32, u32) {
        self.index.remove(key, seqno)
    }

    fn remove_weak<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> (u32, u32) {
        self.index.remove_weak(key, seqno)
    }
}
