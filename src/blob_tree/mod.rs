// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod gc;
pub mod handle;
pub mod ingest;

#[doc(hidden)]
pub use gc::{FragmentationEntry, FragmentationMap};

use crate::{
    coding::Decode,
    file::{fsync_directory, BLOBS_FOLDER},
    iter_guard::{IterGuard, IterGuardImpl},
    r#abstract::{AbstractTree, RangeItem},
    table::Table,
    tree::inner::MemtableId,
    value::InternalValue,
    version::Version,
    vlog::{Accessor, BlobFile, BlobFileWriter, ValueHandle},
    Cache, Config, Memtable, SeqNo, TableId, TreeId, UserKey, UserValue,
};
use handle::BlobIndirection;
use std::{
    io::Cursor,
    ops::RangeBounds,
    path::PathBuf,
    sync::{Arc, MutexGuard},
};

/// Iterator value guard
pub struct Guard {
    tree: crate::BlobTree,
    version: Version,
    kv: crate::Result<InternalValue>,
}

impl IterGuard for Guard {
    fn into_inner_if(
        self,
        pred: impl Fn(&UserKey) -> bool,
    ) -> crate::Result<(UserKey, Option<UserValue>)> {
        let kv = self.kv?;

        if pred(&kv.key.user_key) {
            resolve_value_handle(
                self.tree.id(),
                self.tree.blobs_folder.as_path(),
                &self.tree.index.config.cache,
                &self.version,
                kv,
            )
            .map(|(k, v)| (k, Some(v)))
        } else {
            Ok((kv.key.user_key, None))
        }
    }

    fn key(self) -> crate::Result<UserKey> {
        self.kv.map(|kv| kv.key.user_key)
    }

    fn size(self) -> crate::Result<u32> {
        let kv = self.kv?;

        if kv.key.value_type.is_indirection() {
            let mut cursor = Cursor::new(kv.value);
            Ok(BlobIndirection::decode_from(&mut cursor)?.size)
        } else {
            #[expect(clippy::cast_possible_truncation, reason = "values are u32 max length")]
            Ok(kv.value.len() as u32)
        }
    }

    fn into_inner(self) -> crate::Result<(UserKey, UserValue)> {
        resolve_value_handle(
            self.tree.id(),
            self.tree.blobs_folder.as_path(),
            &self.tree.index.config.cache,
            &self.version,
            self.kv?,
        )
    }
}

fn resolve_value_handle(
    tree_id: TreeId,
    blobs_folder: &std::path::Path,
    cache: &Arc<Cache>,
    version: &Version,
    item: InternalValue,
) -> RangeItem {
    if item.key.value_type.is_indirection() {
        let mut cursor = Cursor::new(item.value);
        let vptr = BlobIndirection::decode_from(&mut cursor)?;

        // Resolve indirection using value log
        match Accessor::new(&version.blob_files).get(
            tree_id,
            blobs_folder,
            &item.key.user_key,
            &vptr.vhandle,
            cache,
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

    blobs_folder: Arc<PathBuf>,
}

impl BlobTree {
    pub(crate) fn open(config: Config) -> crate::Result<Self> {
        let index = crate::Tree::open(config)?;

        let blobs_folder = index.config.path.join(BLOBS_FOLDER);
        std::fs::create_dir_all(&blobs_folder)?;
        fsync_directory(&blobs_folder)?;

        let blob_file_id_to_continue_with = index
            .current_version()
            .blob_files
            .list_ids()
            .max()
            .map(|x| x + 1)
            .unwrap_or_default();

        index
            .0
            .blob_file_id_counter
            .set(blob_file_id_to_continue_with);

        Ok(Self {
            index,
            blobs_folder: Arc::new(blobs_folder),
        })
    }
}

impl AbstractTree for BlobTree {
    fn table_file_cache_size(&self) -> usize {
        self.index.table_file_cache_size()
    }

    fn get_version_history_lock(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, crate::version::SuperVersions> {
        self.index.get_version_history_lock()
    }

    fn next_table_id(&self) -> TableId {
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
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        use crate::range::prefix_to_range;

        let super_version = self.index.get_version_for_snapshot(seqno);
        let tree = self.clone();

        let range = prefix_to_range(prefix.as_ref());

        Box::new(
            crate::Tree::create_internal_range(super_version.clone(), &range, seqno, index).map(
                move |kv| {
                    IterGuardImpl::Blob(Guard {
                        tree: tree.clone(),
                        version: super_version.version.clone(),
                        kv,
                    })
                },
            ),
        )
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        let super_version = self.index.get_version_for_snapshot(seqno);
        let tree = self.clone();

        Box::new(
            crate::Tree::create_internal_range(super_version.clone(), &range, seqno, index).map(
                move |kv| {
                    IterGuardImpl::Blob(Guard {
                        tree: tree.clone(),
                        version: super_version.version.clone(),
                        kv,
                    })
                },
            ),
        )
    }

    fn tombstone_count(&self) -> u64 {
        self.index.tombstone_count()
    }

    fn weak_tombstone_count(&self) -> u64 {
        self.index.weak_tombstone_count()
    }

    fn weak_tombstone_reclaimable_count(&self) -> u64 {
        self.index.weak_tombstone_reclaimable_count()
    }

    fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<()> {
        self.index.drop_range(range)
    }

    fn clear(&self) -> crate::Result<()> {
        self.index.clear()
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
            #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
            {
                item.value.len() as u32
            }
        }))
    }

    fn stale_blob_bytes(&self) -> u64 {
        self.current_version().gc_stats().stale_bytes()
    }

    fn filter_size(&self) -> u64 {
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

    fn get_flush_lock(&self) -> MutexGuard<'_, ()> {
        self.index.get_flush_lock()
    }

    fn flush_to_tables(
        &self,
        stream: impl Iterator<Item = crate::Result<InternalValue>>,
    ) -> crate::Result<Option<(Vec<Table>, Option<Vec<BlobFile>>)>> {
        use crate::{coding::Encode, file::TABLES_FOLDER, table::multi_writer::MultiWriter};

        let start = std::time::Instant::now();

        let table_folder = self.index.config.path.join(TABLES_FOLDER);

        let data_block_size = self.index.config.data_block_size_policy.get(0);

        let data_block_restart_interval =
            self.index.config.data_block_restart_interval_policy.get(0);
        let index_block_restart_interval =
            self.index.config.index_block_restart_interval_policy.get(0);

        let data_block_compression = self.index.config.data_block_compression_policy.get(0);
        let index_block_compression = self.index.config.index_block_compression_policy.get(0);

        let data_block_hash_ratio = self.index.config.data_block_hash_ratio_policy.get(0);

        let index_partitioning = self.index.config.index_block_partitioning_policy.get(0);
        let filter_partitioning = self.index.config.filter_block_partitioning_policy.get(0);

        log::debug!("Flushing memtable(s) and performing key-value separation, data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, data_block_compression={data_block_compression}, index_block_compression={index_block_compression}");
        log::debug!("=> to table(s) in {}", table_folder.display());
        log::debug!("=> to blob file(s) at {}", self.blobs_folder.display());

        let mut table_writer = MultiWriter::new(
            table_folder.clone(),
            self.index.table_id_counter.clone(),
            64 * 1_024 * 1_024,
            0,
        )?
        .use_data_block_restart_interval(data_block_restart_interval)
        .use_index_block_restart_interval(index_block_restart_interval)
        .use_data_block_compression(data_block_compression)
        .use_index_block_compression(index_block_compression)
        .use_data_block_size(data_block_size)
        .use_data_block_hash_ratio(data_block_hash_ratio)
        .use_bloom_policy({
            use crate::config::FilterPolicyEntry::{Bloom, None};
            use crate::table::filter::BloomConstructionPolicy;

            match self.index.config.filter_policy.get(0) {
                Bloom(policy) => policy,
                None => BloomConstructionPolicy::BitsPerKey(0.0),
            }
        });

        if index_partitioning {
            table_writer = table_writer.use_partitioned_index();
        }
        if filter_partitioning {
            table_writer = table_writer.use_partitioned_filter();
        }

        #[expect(
            clippy::expect_used,
            reason = "cannot create blob tree without defining kv separation options"
        )]
        let kv_opts = self
            .index
            .config
            .kv_separation_opts
            .as_ref()
            .expect("kv separation options should exist");

        let mut blob_writer = BlobFileWriter::new(
            self.index.0.blob_file_id_counter.clone(),
            self.index.config.path.join(BLOBS_FOLDER),
            self.id(),
            self.index.config.descriptor_table.clone(),
        )?
        .use_target_size(kv_opts.file_target_size)
        .use_compression(kv_opts.compression);

        let separation_threshold = kv_opts.separation_threshold;

        for item in stream {
            let item = item?;

            if item.is_tombstone() {
                // NOTE: Still need to add tombstone to index tree
                // But no blob to blob writer
                table_writer.write(InternalValue::new(item.key, UserValue::empty()))?;
                continue;
            }

            let value = item.value;

            #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
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

                table_writer.write({
                    let mut vptr =
                        InternalValue::new(item.key.clone(), indirection.encode_into_vec());
                    vptr.key.value_type = crate::ValueType::Indirection;
                    vptr
                })?;

                table_writer.register_blob(indirection);
            } else {
                table_writer.write(InternalValue::new(item.key, value))?;
            }
        }

        let blob_files = blob_writer.finish()?;

        let result = table_writer.finish()?;

        log::debug!("Flushed memtable(s) in {:?}", start.elapsed());

        let pin_filter = self.index.config.filter_block_pinning_policy.get(0);
        let pin_index = self.index.config.index_block_pinning_policy.get(0);

        // Load tables
        let tables = result
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                Table::recover(
                    table_folder.join(table_id.to_string()),
                    checksum,
                    0,
                    self.index.id,
                    self.index.config.cache.clone(),
                    self.index.config.descriptor_table.clone(),
                    pin_filter,
                    pin_index,
                    #[cfg(feature = "metrics")]
                    self.index.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Some((tables, Some(blob_files))))
    }

    fn register_tables(
        &self,
        tables: &[Table],
        blob_files: Option<&[BlobFile]>,
        frag_map: Option<FragmentationMap>,
        sealed_memtables_to_delete: &[MemtableId],
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        self.index.register_tables(
            tables,
            blob_files,
            frag_map,
            sealed_memtables_to_delete,
            gc_watermark,
        )
    }

    fn compact(
        &self,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<()> {
        self.index.compact(strategy, seqno_threshold)
    }

    fn get_next_table_id(&self) -> TableId {
        self.index.get_next_table_id()
    }

    fn tree_config(&self) -> &Config {
        &self.index.config
    }

    fn get_highest_seqno(&self) -> Option<SeqNo> {
        self.index.get_highest_seqno()
    }

    fn active_memtable(&self) -> Arc<Memtable> {
        self.index.active_memtable()
    }

    fn tree_type(&self) -> crate::TreeType {
        crate::TreeType::Blob
    }

    fn rotate_memtable(&self) -> Option<Arc<Memtable>> {
        self.index.rotate_memtable()
    }

    fn table_count(&self) -> usize {
        self.index.table_count()
    }

    fn level_table_count(&self, idx: usize) -> Option<usize> {
        self.index.level_table_count(idx)
    }

    fn approximate_len(&self) -> usize {
        self.index.approximate_len()
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster key reads
    fn is_empty(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<bool> {
        self.index.is_empty(seqno, index)
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster key reads
    fn contains_key<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<bool> {
        self.index.contains_key(key, seqno)
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster scans
    fn len(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<usize> {
        self.index.len(seqno, index)
    }

    fn disk_space(&self) -> u64 {
        let version = self.current_version();
        self.index.disk_space() + version.blob_files.on_disk_size()
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

        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let super_version = self
            .index
            .version_history
            .read()
            .expect("lock is poisoned")
            .get_version_for_snapshot(seqno);

        let Some(item) = crate::Tree::get_internal_entry_from_version(&super_version, key, seqno)?
        else {
            return Ok(None);
        };

        let (_, v) = resolve_value_handle(
            self.id(),
            self.blobs_folder.as_path(),
            &self.index.config.cache,
            &super_version.version,
            item,
        )?;

        Ok(Some(v))
    }

    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        self.index.remove(key, seqno)
    }

    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        self.index.remove_weak(key, seqno)
    }
}
