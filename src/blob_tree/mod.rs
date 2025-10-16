// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod gc;
pub mod handle;

#[doc(hidden)]
pub use gc::{FragmentationEntry, FragmentationMap};

use crate::{
    coding::{Decode, Encode},
    compaction::stream::CompactionStream,
    file::{fsync_directory, BLOBS_FOLDER},
    iter_guard::{IterGuard, IterGuardImpl},
    r#abstract::{AbstractTree, RangeItem},
    segment::Segment,
    tree::inner::MemtableId,
    value::InternalValue,
    version::Version,
    vlog::{Accessor, BlobFile, BlobFileWriter, ValueHandle},
    Config, Memtable, SegmentId, SeqNo, SequenceNumberCounter, UserKey, UserValue,
};
use handle::BlobIndirection;
use std::{io::Cursor, ops::RangeBounds, path::PathBuf, sync::Arc};

pub struct Guard<'a> {
    blob_tree: &'a BlobTree,
    version: Version,
    kv: crate::Result<InternalValue>,
}

impl IterGuard for Guard<'_> {
    fn key(self) -> crate::Result<UserKey> {
        self.kv.map(|kv| kv.key.user_key)
    }

    fn size(self) -> crate::Result<u32> {
        let kv = self.kv?;

        if kv.key.value_type.is_indirection() {
            let mut cursor = Cursor::new(kv.value);
            Ok(BlobIndirection::decode_from(&mut cursor)?.size)
        } else {
            // NOTE: We know that values are u32 max length
            #[allow(clippy::cast_possible_truncation)]
            Ok(kv.value.len() as u32)
        }
    }

    fn into_inner(self) -> crate::Result<(UserKey, UserValue)> {
        resolve_value_handle(self.blob_tree, &self.version, self.kv?)
    }
}

fn resolve_value_handle(tree: &BlobTree, version: &Version, item: InternalValue) -> RangeItem {
    if item.key.value_type.is_indirection() {
        let mut cursor = Cursor::new(item.value);
        let vptr = BlobIndirection::decode_from(&mut cursor)?;

        // Resolve indirection using value log
        match Accessor::new(&version.value_log).get(
            tree.id(),
            &tree.blobs_folder,
            &item.key.user_key,
            &vptr.vhandle,
            &tree.index.config.cache,
            &tree.index.config.descriptor_table,
        ) {
            Ok(Some(v)) => {
                let k = item.key.user_key;
                Ok((k, v))
            }
            Ok(None) => {
                panic!(
                    "value handle ({:?} => {:?}) did not match any blob - this is a bug; version={}",
                    item.key.user_key, vptr.vhandle,
                    version.id(),
                );
            }
            Err(e) => Err(e),
        }
    } else {
        let k = item.key.user_key;
        let v = item.value;
        Ok((k, v))
    }
}

/// A key-value-separated log-structured merge tree
///
/// This tree is a composite structure, consisting of an
/// index tree (LSM-tree) and a log-structured value log
/// to reduce write amplification.
#[derive(Clone)]
pub struct BlobTree {
    /// Index tree that holds value handles or small inline values
    #[doc(hidden)]
    pub index: crate::Tree,

    blobs_folder: PathBuf,
}

impl BlobTree {
    pub(crate) fn open(config: Config) -> crate::Result<Self> {
        let index = crate::Tree::open(config)?;

        let blobs_folder = index.config.path.join(BLOBS_FOLDER);
        std::fs::create_dir_all(&blobs_folder)?;
        fsync_directory(&blobs_folder)?;

        let blob_file_id_to_continue_with = index
            .current_version()
            .value_log
            .values()
            .map(BlobFile::id)
            .max()
            .map(|x| x + 1)
            .unwrap_or_default();

        index
            .0
            .blob_file_id_generator
            .set(blob_file_id_to_continue_with);

        Ok(Self {
            index,
            blobs_folder,
        })
    }
}

impl AbstractTree for BlobTree {
    fn next_table_id(&self) -> SegmentId {
        self.index.next_table_id()
    }

    fn id(&self) -> crate::TreeId {
        self.index.id()
    }

    fn get_internal_entry(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        self.index.get_internal_entry(key, seqno)
    }

    fn current_version(&self) -> Version {
        self.index.current_version()
    }

    fn flush_active_memtable(&self, eviction_seqno: SeqNo) -> crate::Result<Option<Segment>> {
        let Some((segment_id, yanked_memtable)) = self.index.rotate_memtable() else {
            return Ok(None);
        };

        let Some((segment, blob_file)) =
            self.flush_memtable(segment_id, &yanked_memtable, eviction_seqno)?
        else {
            return Ok(None);
        };
        self.register_segments(
            std::slice::from_ref(&segment),
            blob_file.as_ref().map(std::slice::from_ref),
            None,
            eviction_seqno,
        )?;

        Ok(Some(segment))
    }

    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics> {
        self.index.metrics()
    }

    fn version_free_list_len(&self) -> usize {
        self.index.version_free_list_len()
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl<'_>> + '_> {
        use crate::range::prefix_to_range;

        let range = prefix_to_range(prefix.as_ref());

        let version = self.current_version();

        Box::new(
            self.index
                .create_internal_range(&range, seqno, index)
                .map(move |kv| {
                    IterGuardImpl::Blob(Guard {
                        blob_tree: self,
                        version: version.clone(), // TODO: PERF: ugly Arc clone
                        kv,
                    })
                }),
        )
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl<'_>> + '_> {
        let version = self.current_version();

        // TODO: PERF: ugly Arc clone
        Box::new(
            self.index
                .create_internal_range(&range, seqno, index)
                .map(move |kv| {
                    IterGuardImpl::Blob(Guard {
                        blob_tree: self,
                        version: version.clone(), // TODO: PERF: ugly Arc clone
                        kv,
                    })
                }),
        )
    }

    fn tombstone_count(&self) -> u64 {
        self.index.tombstone_count()
    }

    fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<()> {
        self.index.drop_range(range)
    }

    fn ingest(
        &self,
        iter: impl Iterator<Item = (UserKey, UserValue)>,
        seqno_generator: &SequenceNumberCounter,
        visible_seqno: &SequenceNumberCounter,
    ) -> crate::Result<()> {
        use crate::tree::ingest::Ingestion;
        use std::time::Instant;

        // TODO: take curr seqno for ingest, HOWEVER
        // TODO: we need to take the next seqno AFTER locking the memtable

        todo!();

        // // NOTE: Lock active memtable so nothing else can be going on while we are bulk loading
        // let lock = self.lock_active_memtable();
        // assert!(
        //     lock.is_empty(),
        //     "can only perform bulk_ingest on empty trees",
        // );

        // let mut segment_writer = Ingestion::new(&self.index)?.with_seqno(seqno);
        // let mut blob_writer = self.blobs.get_writer()?;

        // let start = Instant::now();
        // let mut count = 0;
        // let mut last_key = None;

        // for (key, value) in iter {
        //     if let Some(last_key) = &last_key {
        //         assert!(
        //             key > last_key,
        //             "next key in bulk ingest was not greater than last key",
        //         );
        //     }
        //     last_key = Some(key.clone());

        //     // NOTE: Values are 32-bit max
        //     #[allow(clippy::cast_possible_truncation)]
        //     let value_size = value.len() as u32;

        //     if value_size >= self.index.config.blob_file_separation_threshold {
        //         let vhandle = blob_writer.get_next_value_handle();

        //         let indirection = MaybeInlineValue::Indirect {
        //             vhandle,
        //             size: value_size,
        //         };
        //         // TODO: use Slice::with_size
        //         let mut serialized_indirection = vec![];
        //         indirection.encode_into(&mut serialized_indirection)?;

        //         segment_writer.write(key.clone(), serialized_indirection.into())?;

        //         blob_writer.write(&key, value)?;
        //     } else {
        //         // TODO: use Slice::with_size
        //         let direct = MaybeInlineValue::Inline(value);
        //         let serialized_direct = direct.encode_into_vec();
        //         segment_writer.write(key, serialized_direct.into())?;
        //     }

        //     count += 1;
        // }

        // // TODO: add to manifest + unit test
        // // self.blobs.register_writer(blob_writer)?;
        // // segment_writer.finish()?;

        // TODO: increaes visible seqno

        // log::info!("Ingested {count} items in {:?}", start.elapsed());

        Ok(())
    }

    fn major_compact(&self, target_size: u64, seqno_threshold: SeqNo) -> crate::Result<()> {
        self.index.major_compact(target_size, seqno_threshold)
    }

    fn clear_active_memtable(&self) {
        self.index.clear_active_memtable();
    }

    fn l0_run_count(&self) -> usize {
        self.index.l0_run_count()
    }

    fn blob_file_count(&self) -> usize {
        self.current_version().blob_file_count()
    }

    // NOTE: We skip reading from the value log
    // because the vHandles already store the value size
    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<u32>> {
        let Some(item) = self.index.get_internal_entry(key.as_ref(), seqno)? else {
            return Ok(None);
        };

        Ok(Some(if item.key.value_type.is_indirection() {
            let mut cursor = Cursor::new(item.value);
            let vptr = BlobIndirection::decode_from(&mut cursor)?;
            vptr.size
        } else {
            // NOTE: Values are u32 length max
            #[allow(clippy::cast_possible_truncation)]
            {
                item.value.len() as u32
            }
        }))
    }

    fn stale_blob_bytes(&self) -> u64 {
        self.current_version().gc_stats().stale_bytes()
    }

    fn filter_size(&self) -> usize {
        self.index.filter_size()
    }

    fn pinned_filter_size(&self) -> usize {
        self.index.pinned_filter_size()
    }

    fn pinned_block_index_size(&self) -> usize {
        self.index.pinned_block_index_size()
    }

    fn sealed_memtable_count(&self) -> usize {
        self.index.sealed_memtable_count()
    }

    fn flush_memtable(
        &self,
        segment_id: SegmentId,
        memtable: &Arc<Memtable>,
        eviction_seqno: SeqNo,
    ) -> crate::Result<Option<(Segment, Option<BlobFile>)>> {
        use crate::{file::SEGMENTS_FOLDER, segment::Writer as SegmentWriter};

        let lsm_segment_folder = self.index.config.path.join(SEGMENTS_FOLDER);

        log::debug!("Flushing memtable & performing key-value separation");
        log::debug!("=> to LSM table in {}", lsm_segment_folder.display());
        log::debug!("=> to blob file at {}", self.blobs_folder.display());

        let mut segment_writer =
            SegmentWriter::new(lsm_segment_folder.join(segment_id.to_string()), segment_id)?
                // TODO: apply other policies
                .use_data_block_compression(self.index.config.data_block_compression_policy.get(0))
                .use_bloom_policy({
                    use crate::config::FilterPolicyEntry::{Bloom, None};
                    use crate::segment::filter::BloomConstructionPolicy;

                    match self.index.config.filter_policy.get(0) {
                        Bloom(policy) => policy,
                        None => BloomConstructionPolicy::BitsPerKey(0.0),
                    }
                });

        let mut blob_writer = BlobFileWriter::new(
            self.index.0.blob_file_id_generator.clone(),
            u64::MAX, // TODO: actually use target size? but be sure to link to table correctly
            self.index.config.path.join(BLOBS_FOLDER),
        )?
        .use_compression(
            self.index
                .config
                .kv_separation_opts
                .as_ref()
                .expect("blob options should exist")
                .compression,
        );

        let iter = memtable.iter().map(Ok);
        let compaction_stream = CompactionStream::new(iter, eviction_seqno);

        let mut blob_bytes_referenced = 0;
        let mut blobs_referenced_count = 0;

        let separation_threshold = self
            .index
            .config
            .kv_separation_opts
            .as_ref()
            .expect("kv separation options should exist")
            .separation_threshold;

        for item in compaction_stream {
            let item = item?;

            if item.is_tombstone() {
                // NOTE: Still need to add tombstone to index tree
                // But no blob to blob writer
                segment_writer.write(InternalValue::new(item.key, UserValue::empty()))?;
                continue;
            }

            let value = item.value;

            // NOTE: Values are 32-bit max
            #[allow(clippy::cast_possible_truncation)]
            let value_size = value.len() as u32;

            if value_size >= separation_threshold {
                let offset = blob_writer.offset();
                let blob_file_id = blob_writer.blob_file_id();
                let on_disk_size = blob_writer.write(&item.key.user_key, item.key.seqno, &value)?;

                let indirection = BlobIndirection {
                    vhandle: ValueHandle {
                        blob_file_id,
                        offset,
                        on_disk_size,
                    },
                    size: value_size,
                };

                segment_writer.write({
                    let mut vptr =
                        InternalValue::new(item.key.clone(), indirection.encode_into_vec());
                    vptr.key.value_type = crate::ValueType::Indirection;
                    vptr
                })?;

                blob_bytes_referenced += u64::from(value_size);
                blobs_referenced_count += 1;
            } else {
                segment_writer.write(InternalValue::new(item.key, value))?;
            }
        }

        log::trace!("Creating blob file");
        let blob_files = blob_writer.finish()?;
        assert!(blob_files.len() <= 1);
        let blob_file = blob_files.into_iter().next();

        log::trace!("Creating LSM-tree segment {segment_id}");

        if blob_bytes_referenced > 0 {
            if let Some(blob_file) = &blob_file {
                segment_writer.link_blob_file(
                    blob_file.id(),
                    blob_bytes_referenced,
                    blobs_referenced_count,
                );
            }
        }

        let segment = self.index.consume_writer(segment_writer)?;

        Ok(segment.map(|segment| (segment, blob_file)))
    }

    fn register_segments(
        &self,
        segments: &[Segment],
        blob_files: Option<&[BlobFile]>,
        frag_map: Option<FragmentationMap>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<()> {
        self.index
            .register_segments(segments, blob_files, frag_map, seqno_threshold)
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

    fn active_memtable_size(&self) -> u64 {
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

    fn level_segment_count(&self, idx: usize) -> Option<usize> {
        self.index.level_segment_count(idx)
    }

    fn approximate_len(&self) -> usize {
        self.index.approximate_len()
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster key reads
    fn is_empty(&self, seqno: SeqNo, index: Option<Arc<Memtable>>) -> crate::Result<bool> {
        self.index.is_empty(seqno, index)
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster key reads
    fn contains_key<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<bool> {
        self.index.contains_key(key, seqno)
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster scans
    fn len(&self, seqno: SeqNo, index: Option<Arc<Memtable>>) -> crate::Result<usize> {
        self.index.len(seqno, index)
    }

    fn disk_space(&self) -> u64 {
        let version = self.current_version();
        let vlog = crate::vlog::Accessor::new(&version.value_log);
        self.index.disk_space() + vlog.disk_space()
    }

    fn get_highest_memtable_seqno(&self) -> Option<SeqNo> {
        self.index.get_highest_memtable_seqno()
    }

    fn get_highest_persisted_seqno(&self) -> Option<SeqNo> {
        self.index.get_highest_persisted_seqno()
    }

    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u64, u64) {
        self.index.insert(key, value.into(), seqno)
    }

    fn get<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<crate::UserValue>> {
        let key = key.as_ref();

        // TODO: refactor memtable, sealed memtables, manifest lock to be a single lock (SuperVersion kind of)
        // TODO: then, try to reduce the lock access to 1, because we are accessing it twice (index.get, and then vhandle resolving...)

        let Some(item) = self.index.get_internal_entry(key, seqno)? else {
            return Ok(None);
        };

        let version = self.current_version();
        let (_, v) = resolve_value_handle(self, &version, item)?;

        Ok(Some(v))
    }

    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        self.index.remove(key, seqno)
    }

    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        self.index.remove_weak(key, seqno)
    }
}
