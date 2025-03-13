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
    iter_guard::{IterGuard, IterGuardImpl},
    r#abstract::{AbstractTree, RangeItem},
    tree::inner::MemtableId,
    value::InternalValue,
    Config, KvPair, Memtable, Segment, SegmentId, SeqNo, Snapshot, UserKey, UserValue,
};
use compression::MyCompressor;
use gc::{reader::GcReader, writer::GcWriter};
use index::IndexTree;
use std::{
    io::Cursor,
    ops::{RangeBounds, RangeFull},
    sync::{atomic::AtomicUsize, Arc},
};
use value::MaybeInlineValue;
use value_log::ValueLog;

fn resolve_value_handle(vlog: &ValueLog<MyCompressor>, item: RangeItem) -> RangeItem {
    use MaybeInlineValue::{Indirect, Inline};

    match item {
        Ok((key, value)) => {
            let mut cursor = Cursor::new(value);

            match MaybeInlineValue::decode_from(&mut cursor)? {
                Inline(bytes) => Ok((key, bytes)),
                Indirect { vhandle, .. } => {
                    // Resolve indirection using value log
                    match vlog.get(&vhandle) {
                        Ok(Some(bytes)) => Ok((key, bytes)),
                        Err(e) => Err(e.into()),
                        _ => {
                            panic!("value handle ({:?} => {vhandle:?}) did not match any blob - this is a bug", String::from_utf8_lossy(&key))
                        }
                    }
                }
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

    // TODO: maybe replace this with a nonce system
    #[doc(hidden)]
    pub pending_segments: Arc<AtomicUsize>,
}

pub struct Guard<'a>(
    &'a ValueLog<MyCompressor>,
    crate::Result<(UserKey, UserValue)>,
);

impl IterGuard for Guard<'_> {
    fn key(self) -> crate::Result<UserKey> {
        self.1.map(|(k, _)| k)
    }

    fn size(self) -> crate::Result<u32> {
        use MaybeInlineValue::{Indirect, Inline};

        let value = self.1?.1;
        let mut cursor = Cursor::new(value);

        Ok(match MaybeInlineValue::decode_from(&mut cursor)? {
            Inline(bytes) => bytes.len() as u32,

            // NOTE: No need to resolve vHandle, because the size is already stored
            Indirect { size, .. } => size,
        })
    }

    fn into_inner(self) -> crate::Result<(UserKey, UserValue)> {
        resolve_value_handle(self.0, self.1)
    }
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
            pending_segments: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Scans the index tree, collecting statistics about
    /// value log fragmentation
    #[doc(hidden)]
    pub fn gc_scan_stats(
        &self,
        seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> crate::Result<crate::gc::Report> {
        use std::io::{Error as IoError, ErrorKind as IoErrorKind};
        use MaybeInlineValue::{Indirect, Inline};

        while self
            .pending_segments
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
        {
            // IMPORTANT: Busy wait until all segments in-flight are committed
            // to the tree
        }

        // IMPORTANT: Lock + snapshot memtable to avoid read skew + preventing tampering with memtable
        let _memtable_lock = self.index.read_lock_active_memtable();

        while self
            .pending_segments
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
        {
            // IMPORTANT: Busy wait again until all segments in-flight are committed
            // to the tree
        }

        let iter = self
            .index
            .create_internal_range::<&[u8], RangeFull>(&.., Some(seqno), None);

        // Stores the max seqno of every blob file
        let mut seqno_map = crate::HashMap::<SegmentId, SeqNo>::default();

        let result = self
            .blobs
            .scan_for_stats(iter.filter_map(|kv| {
                let Ok(kv) = kv else {
                    return Some(Err(IoError::new(
                        IoErrorKind::Other,
                        "Failed to load KV pair from index tree",
                    )));
                };

                let mut cursor = Cursor::new(kv.value);
                let value = match MaybeInlineValue::decode_from(&mut cursor) {
                    Ok(v) => v,
                    Err(e) => return Some(Err(IoError::new(IoErrorKind::Other, e.to_string()))),
                };

                match value {
                    Indirect { vhandle, size } => {
                        seqno_map
                            .entry(vhandle.segment_id)
                            .and_modify(|x| *x = (*x).max(kv.key.seqno))
                            .or_insert(kv.key.seqno);

                        Some(Ok((vhandle, size)))
                    }
                    Inline(_) => None,
                }
            }))
            .map_err(Into::into);

        let mut lock = self
            .blobs
            .manifest
            .segments
            .write()
            .expect("lock is poisoned");

        // IMPORTANT: We are overwiting the staleness of blob files
        // that contain an item that is still contained in the GC watermark
        // so snapshots cannot accidentally lose data
        //
        // TODO: 3.0.0 this should be dealt with in value-log 2.0 (make it MVCC aware)
        for (blob_file_id, max_seqno) in seqno_map {
            if gc_watermark <= max_seqno {
                if let Some(blob_file) = lock.get_mut(&blob_file_id) {
                    blob_file.gc_stats.set_stale_items(0);
                    blob_file.gc_stats.set_stale_bytes(0);
                }
            }
        }

        result
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
    pub fn flush_active_memtable(&self, eviction_seqno: SeqNo) -> crate::Result<Option<Segment>> {
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
    fn guarded_prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: Option<SeqNo>,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn Iterator<Item = IterGuardImpl> + '_> {
        Box::new(
            self.index
                .0
                .create_prefix(&prefix, seqno, index)
                .map(move |kv| IterGuardImpl::Blob(Guard(&self.blobs, kv))),
        )
    }

    fn guarded_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: Option<SeqNo>,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn Iterator<Item = IterGuardImpl> + '_> {
        Box::new(
            self.index
                .0
                .create_range(&range, seqno, index)
                .map(move |kv| IterGuardImpl::Blob(Guard(&self.blobs, kv))),
        )
    }

    fn blob_file_count(&self) -> usize {
        self.blobs.segment_count()
    }

    // NOTE: We skip reading from the value log
    // because the vHandles already store the value size
    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: Option<SeqNo>) -> crate::Result<Option<u32>> {
        let vhandle = self.index.get_vhandle(key.as_ref(), seqno)?;

        Ok(vhandle.map(|x| match x {
            MaybeInlineValue::Inline(v) => v.len() as u32,

            // NOTE: We skip reading from the value log
            // because the indirections already store the value size
            MaybeInlineValue::Indirect { size, .. } => size,
        }))
    }

    fn bloom_filter_size(&self) -> usize {
        self.index.bloom_filter_size()
    }

    fn sealed_memtable_count(&self) -> usize {
        self.index.sealed_memtable_count()
    }

    fn is_first_level_disjoint(&self) -> bool {
        self.index.is_first_level_disjoint()
    }

    #[doc(hidden)]
    fn verify(&self) -> crate::Result<usize> {
        let index_tree_sum = self.index.verify()?;
        let vlog_sum = self.blobs.verify()?;
        Ok(index_tree_sum + vlog_sum)
    }

    fn keys(
        &self,
        seqno: Option<SeqNo>,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static> {
        self.index.keys(seqno, index)
    }

    fn values(
        &self,
        seqno: Option<SeqNo>,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static> {
        Box::new(self.iter(seqno, index).map(|x| x.map(|(_, v)| v)))
    }

    fn flush_memtable(
        &self,
        segment_id: SegmentId,
        memtable: &Arc<Memtable>,
        eviction_seqno: SeqNo,
    ) -> crate::Result<Option<Segment>> {
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

        segment_writer = segment_writer.use_bloom_policy(
            crate::segment::writer::BloomConstructionPolicy::FpRate(0.0001),
        );

        let mut blob_writer = self.blobs.get_writer()?;

        let iter = memtable.iter().map(Ok);
        let compaction_filter = CompactionStream::new(iter, eviction_seqno);

        for item in compaction_filter {
            let item = item?;

            if item.is_tombstone() {
                // NOTE: Still need to add tombstone to index tree
                // But no blob to blob writer

                // TODO: Slice::empty
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
                let serialized_direct = direct.encode_into_vec();
                segment_writer.write(InternalValue::new(item.key, serialized_direct))?;
            }
        }

        let _memtable_lock = self.lock_active_memtable();

        log::trace!("Register blob writer into value log");
        self.blobs.register_writer(blob_writer)?;

        log::trace!("Creating LSM-tree segment {segment_id}");
        let segment = self.index.consume_writer(segment_id, segment_writer)?;

        // TODO: this can probably solved in a nicer way
        if segment.is_some() {
            // IMPORTANT: Increment the pending count
            // so there cannot be a GC scan now, until the segment is registered
            self.pending_segments
                .fetch_add(1, std::sync::atomic::Ordering::Release);
        }

        Ok(segment)
    }

    fn register_segments(&self, segments: &[Segment]) -> crate::Result<()> {
        self.index.register_segments(segments)?;

        let count = self
            .pending_segments
            .load(std::sync::atomic::Ordering::Acquire);

        assert!(
            count >= segments.len(),
            "pending_segments is less than segments to register - this is a bug"
        );

        self.pending_segments
            .fetch_sub(segments.len(), std::sync::atomic::Ordering::Release);

        Ok(())
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
    fn contains_key<K: AsRef<[u8]>>(&self, key: K, seqno: Option<SeqNo>) -> crate::Result<bool> {
        self.index.contains_key(key, seqno)
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster scans
    fn len(&self, seqno: Option<SeqNo>, index: Option<Arc<Memtable>>) -> crate::Result<usize> {
        self.index.len(seqno, index)
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

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: Option<SeqNo>,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static> {
        let vlog = self.blobs.clone();
        Box::new(
            self.index
                .0
                .create_range(&range, seqno, index)
                .map(move |item| resolve_value_handle(&vlog, item)),
        )
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: Option<SeqNo>,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static> {
        let vlog = self.blobs.clone();
        Box::new(
            self.index
                .0
                .create_prefix(prefix, seqno, index)
                .map(move |item| resolve_value_handle(&vlog, item)),
        )
    }

    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u32, u32) {
        use value::MaybeInlineValue;

        // NOTE: Initially, we always write an inline value
        // On memtable flush, depending on the values' sizes, they will be separated
        // into inline or indirect values
        let item = MaybeInlineValue::Inline(value.into());

        let value = item.encode_into_vec();

        self.index.insert(key, value, seqno)
    }

    fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<crate::UserValue>> {
        use value::MaybeInlineValue::{Indirect, Inline};

        let key = key.as_ref();

        let Some(value) = self.index.get_vhandle(key, seqno)? else {
            return Ok(None);
        };

        match value {
            Inline(bytes) => Ok(Some(bytes)),
            Indirect { vhandle, .. } => {
                // Resolve indirection using value log
                match self.blobs.get(&vhandle)? {
                    Some(bytes) => Ok(Some(bytes)),
                    None => {
                        panic!("value handle ({key:?} => {vhandle:?}) did not match any blob - this is a bug")
                    }
                }
            }
        }
    }

    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u32, u32) {
        self.index.remove(key, seqno)
    }

    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u32, u32) {
        self.index.remove_weak(key, seqno)
    }
}
