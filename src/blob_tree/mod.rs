mod compression;
mod gc;
pub mod index;
pub mod value;

use self::value::MaybeInlineValue;
use crate::{
    file::BLOBS_FOLDER,
    r#abstract::{AbstractTree, RangeItem},
    serde::{Deserializable, Serializable},
    tree::inner::MemtableId,
    value::InternalValue,
    Config, KvPair, MemTable, SegmentId, SeqNo, Slice, Snapshot, UserKey, UserValue, ValueType,
};
use compression::get_vlog_compressor;
use gc::{reader::GcReader, writer::GcWriter};
use index::IndexTree;
use std::{
    io::Cursor,
    ops::RangeBounds,
    sync::{Arc, RwLockWriteGuard},
};
use value_log::ValueLog;

fn resolve_value_handle(vlog: &ValueLog, item: RangeItem) -> RangeItem {
    match item {
        Ok((key, value)) => {
            let mut cursor = Cursor::new(value);
            let item = MaybeInlineValue::deserialize(&mut cursor)?;

            match item {
                MaybeInlineValue::Inline(bytes) => Ok((key, bytes)),
                MaybeInlineValue::Indirect { handle, .. } => match vlog.get(&handle) {
                    Ok(Some(bytes)) => Ok((key, Slice::from(bytes))),
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
    pub blobs: ValueLog,
}

impl BlobTree {
    pub fn open(config: Config) -> crate::Result<Self> {
        let path = &config.path;

        let vlog_path = path.join(BLOBS_FOLDER);
        let vlog_cfg = value_log::Config::default()
            .blob_cache(config.blob_cache.clone())
            .segment_size_bytes(config.blob_file_target_size)
            .use_compression(get_vlog_compressor(config.inner.compression));

        let index: IndexTree = config.open()?.into();

        Ok(Self {
            index,
            blobs: ValueLog::open(vlog_path, vlog_cfg)?,
        })
    }

    /// Scans the index tree, collecting statistics about
    /// value log fragmentation
    #[doc(hidden)]
    pub fn gc_scan_stats(&self, seqno: SeqNo) -> crate::Result<()> {
        use std::io::{Error as IoError, ErrorKind as IoErrorKind};
        use MaybeInlineValue::{Indirect, Inline};

        // IMPORTANT: Lock + snapshot memtable to avoid read skew + preventing tampering with memtable
        let _memtable_lock = self.index.read_lock_active_memtable();
        let snapshot = self.index.snapshot(seqno);

        self.blobs.scan_for_stats(snapshot.iter().filter_map(|kv| {
            let Ok((_, v)) = kv else {
                return Some(Err(IoError::new(
                    IoErrorKind::Other,
                    "Failed to load KV pair from index tree",
                )));
            };

            let mut cursor = Cursor::new(v);
            let value = match MaybeInlineValue::deserialize(&mut cursor) {
                Ok(v) => v,
                Err(e) => return Some(Err(IoError::new(IoErrorKind::Other, e.to_string()))),
            };

            match value {
                Indirect { handle, size } => Some(Ok((handle, size))),
                Inline(_) => None,
            }
        }))?;

        Ok(())
    }

    pub fn gc_with_space_amp_target(
        &self,
        space_amp_target: f32,
        seqno: SeqNo,
    ) -> crate::Result<()> {
        let ids = self
            .blobs
            .select_segments_for_space_amp_reduction(space_amp_target);

        // IMPORTANT: Write lock memtable to avoid read skew
        let memtable_lock = self.index.lock_active_memtable();

        self.blobs.rollover(
            &ids,
            &GcReader::new(&self.index, &memtable_lock),
            GcWriter::new(seqno, &memtable_lock),
        )?;

        // NOTE: We still have the memtable lock, can't use gc_drop_stale because recursive locking
        self.blobs.drop_stale_segments()?;

        Ok(())
    }

    /// Rewrites blob files that have reached a stale threshold
    pub fn gc_with_staleness_threshold(
        &self,
        stale_threshold: f32,
        seqno: SeqNo,
    ) -> crate::Result<()> {
        // First, find the segment IDs that are stale
        let ids = self
            .blobs
            .find_segments_with_stale_threshold(stale_threshold);

        // IMPORTANT: Write lock memtable to avoid read skew
        let memtable_lock = self.index.lock_active_memtable();

        self.blobs.rollover(
            &ids,
            &GcReader::new(&self.index, &memtable_lock),
            GcWriter::new(seqno, &memtable_lock),
        )?;

        // NOTE: We still have the memtable lock, can't use gc_drop_stale because recursive locking
        self.blobs.drop_stale_segments()?;

        Ok(())
    }

    /// Drops all stale blob segment files
    #[doc(hidden)]
    pub fn gc_drop_stale(&self) -> crate::Result<()> {
        // IMPORTANT: Write lock memtable to avoid read skew
        let _lock = self.index.lock_active_memtable();
        self.blobs.drop_stale_segments()?;
        Ok(())
    }

    pub fn flush_active_memtable(&self) -> crate::Result<Option<Arc<crate::Segment>>> {
        let Some((segment_id, yanked_memtable)) = self.index.rotate_memtable() else {
            return Ok(None);
        };

        let segment = self.flush_memtable(segment_id, &yanked_memtable)?;
        self.register_segments(&[segment.clone()])?;

        Ok(Some(segment))
    }
}

impl AbstractTree for BlobTree {
    fn verify(&self) -> crate::Result<usize> {
        let index_tree_sum = self.index.verify()?;
        let vlog_sum = self.blobs.verify()?;

        Ok(index_tree_sum + vlog_sum)
    }

    fn keys_with_seqno(
        &self,
        seqno: SeqNo,
        index: Option<Arc<MemTable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserKey>>> {
        self.index.keys_with_seqno(seqno, index)
    }

    fn values_with_seqno(
        &self,
        seqno: SeqNo,
        index: Option<Arc<MemTable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserValue>>> {
        Box::new(
            self.iter_with_seqno(seqno, index)
                .map(|x| x.map(|(_, v)| v)),
        )
    }

    fn keys(&self) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserKey>>> {
        self.index.keys()
    }

    fn values(&self) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserKey>>> {
        Box::new(self.iter().map(|x| x.map(|(_, v)| v)))
    }

    fn flush_memtable(
        &self,
        segment_id: SegmentId,
        memtable: &Arc<MemTable>,
    ) -> crate::Result<Arc<crate::Segment>> {
        use crate::{
            file::SEGMENTS_FOLDER,
            segment::writer::{Options, Writer as SegmentWriter},
        };
        use value::MaybeInlineValue;

        let lsm_segment_folder = self.index.config.path.join(SEGMENTS_FOLDER);

        log::debug!("flushing memtable & performing key-value separation");
        log::debug!("=> to LSM segments in {:?}", lsm_segment_folder);
        log::debug!("=> to blob segment {:?}", self.blobs.path);

        let mut segment_writer = SegmentWriter::new(Options {
            segment_id,
            block_size: self.index.config.inner.block_size,
            evict_tombstones: false,
            folder: lsm_segment_folder,
        })?
        .use_compression(self.index.config.inner.compression);

        #[cfg(feature = "bloom")]
        {
            segment_writer = segment_writer.use_bloom_policy(
                crate::segment::writer::BloomConstructionPolicy::FpRate(0.0001),
            );
        }

        let mut blob_writer = self.blobs.get_writer()?;

        // TODO: bug that drops latest blob file for some reason?? see html benchmark w/ delete + gc

        for item in memtable.iter() {
            if item.is_tombstone() {
                // NOTE: Still need to add tombstone to index tree
                // But no blob to blob writer
                segment_writer.write(InternalValue::new(item.key, vec![]))?;
                continue;
            }

            let mut cursor = Cursor::new(item.value);

            let value = MaybeInlineValue::deserialize(&mut cursor)?;
            let MaybeInlineValue::Inline(value) = value else {
                panic!("values are initially always inlined");
            };

            // NOTE: Values are 32-bit max
            #[allow(clippy::cast_possible_truncation)]
            let value_size = value.len() as u32;

            if value_size > self.index.config.blob_file_separation_threshold {
                let handle = blob_writer.get_next_value_handle();

                let indirection = MaybeInlineValue::Indirect {
                    handle,
                    size: value_size,
                };
                let mut serialized_indirection = vec![];
                indirection.serialize(&mut serialized_indirection)?;

                segment_writer
                    .write(InternalValue::new(item.key.clone(), serialized_indirection))?;

                blob_writer.write(&item.key.user_key, value)?;
            } else {
                let direct = MaybeInlineValue::Inline(value);

                let mut serialized_direct = vec![];
                direct.serialize(&mut serialized_direct)?;

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

    fn lock_active_memtable(&self) -> std::sync::RwLockWriteGuard<'_, MemTable> {
        self.index.lock_active_memtable()
    }

    fn set_active_memtable(&self, memtable: MemTable) {
        self.index.set_active_memtable(memtable);
    }

    fn add_sealed_memtable(&self, id: MemtableId, memtable: Arc<MemTable>) {
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

    fn get_lsn(&self) -> Option<SeqNo> {
        self.index.get_lsn()
    }

    fn active_memtable_size(&self) -> u32 {
        self.index.active_memtable_size()
    }

    fn tree_type(&self) -> crate::TreeType {
        crate::TreeType::Blob
    }

    fn rotate_memtable(&self) -> Option<(crate::tree::inner::MemtableId, Arc<crate::MemTable>)> {
        self.index.rotate_memtable()
    }

    fn segment_count(&self) -> usize {
        self.index.segment_count()
    }

    fn first_level_segment_count(&self) -> usize {
        self.index.first_level_segment_count()
    }

    fn approximate_len(&self) -> u64 {
        self.index.approximate_len()
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster key reads
    fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.index.contains_key(key)
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

    fn get_memtable_lsn(&self) -> Option<SeqNo> {
        self.index.get_memtable_lsn()
    }

    fn get_segment_lsn(&self) -> Option<SeqNo> {
        self.index.get_segment_lsn()
    }

    fn snapshot(&self, seqno: SeqNo) -> Snapshot {
        use crate::AnyTree::Blob;

        Snapshot::new(Blob(self.clone()), seqno)
    }

    fn iter_with_seqno(
        &self,
        seqno: SeqNo,
        index: Option<Arc<MemTable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>>> {
        self.range_with_seqno::<UserKey, _>(.., seqno, index)
    }

    fn range_with_seqno<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<Arc<MemTable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>>> {
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
        index: Option<Arc<MemTable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>>> {
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
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>>> {
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
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>>> {
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
        lock: &RwLockWriteGuard<'_, MemTable>,
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

        let mut value = vec![];
        item.serialize(&mut value).expect("should serialize");

        let value = InternalValue::from_components(key.as_ref(), value, seqno, r#type);
        lock.insert(value)
    }

    fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V, seqno: SeqNo) -> (u32, u32) {
        use value::MaybeInlineValue;

        // NOTE: Initially, we always write an inline value
        // On memtable flush, depending on the values' sizes, they will be separated
        // into inline or indirect values
        let item = MaybeInlineValue::Inline(value.as_ref().into());

        let mut value = vec![];
        item.serialize(&mut value).expect("should serialize");

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
            Indirect { handle, .. } => {
                // Resolve indirection using value log
                Ok(self.blobs.get(&handle)?.map(Slice::from))
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
            Indirect { handle, .. } => {
                // Resolve indirection using value log
                Ok(self.blobs.get(&handle)?.map(Slice::from))
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
