// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{filter::BloomConstructionPolicy, writer::Writer};
use crate::{
    blob_tree::handle::BlobIndirection, encryption::EncryptionProvider, fs::Fs,
    prefix::PrefixExtractor, range_tombstone::RangeTombstone, table::writer::LinkedFile,
    value::InternalValue, vlog::BlobFileId, Checksum, CompressionType, HashMap,
    SequenceNumberCounter, TableId, UserKey,
};
use std::{path::PathBuf, sync::Arc};

/// Like `Writer` but will rotate to a new table, once a table grows larger than `target_size`
///
/// This results in a sorted "run" of tables
pub struct MultiWriter {
    fs: Arc<dyn Fs>,

    pub(crate) base_path: PathBuf,

    data_block_hash_ratio: f32,

    data_block_size: u32,

    data_block_restart_interval: u8,
    index_block_restart_interval: u8,

    use_partitioned_index: bool,
    use_partitioned_filter: bool,

    /// Target size of tables in bytes
    ///
    /// If a table reaches the target size, a new one is started,
    /// resulting in a sorted "run" of tables
    pub target_size: u64,

    results: Vec<(TableId, Checksum)>,

    table_id_generator: SequenceNumberCounter,

    pub writer: Writer,

    pub data_block_compression: CompressionType,
    pub index_block_compression: CompressionType,

    bloom_policy: BloomConstructionPolicy,

    current_key: Option<UserKey>,

    linked_blobs: HashMap<BlobFileId, LinkedFile>,

    /// Range tombstones to distribute across output tables.
    /// During compaction these are clipped to each table's key range;
    /// during flush they are written unmodified (they must cover keys in older SSTs).
    range_tombstones: Vec<RangeTombstone>,

    /// When true, range tombstones are clipped to each output table's KV key range
    /// via `intersect_opt`. This is correct for compaction (input tables are consumed)
    /// but wrong for flush (RTs must cover keys in older SSTs outside the memtable's range).
    clip_range_tombstones: bool,

    /// Level the tables are written to
    initial_level: u8,

    prefix_extractor: Option<Arc<dyn PrefixExtractor>>,

    encryption: Option<Arc<dyn EncryptionProvider>>,

    #[cfg(feature = "zstd")]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
}

impl MultiWriter {
    /// Sets up a new `MultiWriter` at the given tables folder
    pub fn new(
        base_path: PathBuf,
        table_id_generator: SequenceNumberCounter,
        target_size: u64,
        initial_level: u8,
        fs: Arc<dyn Fs>,
    ) -> crate::Result<Self> {
        let current_table_id = table_id_generator.next();

        let path = base_path.join(current_table_id.to_string());
        let writer = Writer::new(path, current_table_id, initial_level, fs.clone())?;

        Ok(Self {
            fs,
            initial_level,

            base_path,

            data_block_hash_ratio: 0.0,

            data_block_size: 4_096,

            data_block_restart_interval: 16,
            index_block_restart_interval: 1,

            target_size,
            results: Vec::new(),
            table_id_generator,
            writer,

            data_block_compression: CompressionType::None,
            index_block_compression: CompressionType::None,

            use_partitioned_index: false,
            use_partitioned_filter: false,

            bloom_policy: BloomConstructionPolicy::default(),

            current_key: None,

            linked_blobs: HashMap::default(),
            range_tombstones: Vec::new(),
            clip_range_tombstones: false,

            prefix_extractor: None,

            encryption: None,

            #[cfg(feature = "zstd")]
            zstd_dictionary: None,
        })
    }

    /// Enables RT clipping: each tombstone is intersected with the output
    /// table's KV key range. Use this for compaction where input tables are
    /// consumed; do NOT use for flush where RTs must cover older SSTs.
    #[must_use]
    pub fn use_clip_range_tombstones(mut self) -> Self {
        self.clip_range_tombstones = true;
        self
    }

    /// Sets range tombstones to be distributed across output tables.
    pub fn set_range_tombstones(&mut self, tombstones: Vec<RangeTombstone>) {
        self.range_tombstones = tombstones;
    }

    /// Writes range tombstones to the given writer, respecting the clip mode.
    ///
    /// - **clip=true** (compaction): intersect each RT with the table's
    ///   "responsibility range".  For intermediate tables (rotation)
    ///   `clip_upper` is the first key of the *next* output table, so the
    ///   range extends past the table's last KV key and covers the gap.
    ///   For the final table `clip_upper` is `None` and we fall back to
    ///   `upper_bound_exclusive(last_key)`.
    /// - **clip=false** (flush): write all overlapping RTs unmodified so they
    ///   cover keys in older SSTs outside this memtable's key range.
    fn write_rts_to_writer(
        tombstones: &[RangeTombstone],
        clip: bool,
        writer: &mut Writer,
        clip_upper: Option<&UserKey>,
    ) {
        if let (Some(first_key), Some(last_key)) =
            (writer.meta.first_key.clone(), writer.meta.last_key.clone())
        {
            if clip {
                // Compaction mode: clip RTs to this table's responsibility range.
                //
                // For intermediate tables (rotation) `clip_upper` is the first key
                // of the next output table — the range [first_key, next_key) covers
                // the gap between tables so RTs spanning it are preserved.
                //
                // For the final table `clip_upper` is None and we derive the
                // exclusive upper bound from the table's last KV key.
                let derived_upper;
                let max_exclusive: Option<&[u8]> = if let Some(upper) = clip_upper {
                    Some(upper.as_ref())
                } else {
                    derived_upper =
                        crate::range_tombstone::upper_bound_exclusive(last_key.as_ref());
                    derived_upper.as_deref()
                };

                if let Some(max_exclusive) = max_exclusive {
                    for rt in tombstones {
                        if let Some(clipped) = rt.intersect_opt(first_key.as_ref(), max_exclusive) {
                            // Widen last_key so point reads for keys in the
                            // gap will consult this table for RT suppression.
                            //
                            // Only widen during rotation (clip_upper is Some)
                            // where we know the exact boundary.  For the final
                            // table (clip_upper is None) widening with the
                            // derived exclusive bound could overlap a non-
                            // compacted adjacent table at the same level.
                            //
                            // Even during rotation, clipped.end must be
                            // strictly less than clip_upper (the next table's
                            // first key) — equality would make key_ranges
                            // overlap, breaking Run::get_for_key_cmp.
                            //
                            // Only last_key needs widening: intersect_opt
                            // already clamps clipped.start >= first_key.
                            if let Some(existing) = &mut writer.meta.last_key {
                                let safe = clip_upper
                                    .is_some_and(|upper| clipped.end.as_ref() < upper.as_ref());
                                if safe && clipped.end.as_ref() > existing.as_ref() {
                                    *existing = clipped.end.clone();
                                }
                            }

                            writer.write_range_tombstone(clipped);
                        }
                    }
                } else {
                    // `last_key` is the lexicographically maximal encodable user
                    // key, so there is no strict successor. In that case clip
                    // only on the lower bound and keep the persisted key_range
                    // unchanged; widening it during compaction would break the
                    // disjoint-run invariant that point reads rely on.
                    for rt in tombstones {
                        let clipped_start = if rt.start.as_ref() > first_key.as_ref() {
                            rt.start.as_ref()
                        } else {
                            first_key.as_ref()
                        };

                        if clipped_start < rt.end.as_ref() {
                            writer.write_range_tombstone(RangeTombstone::new(
                                UserKey::from(clipped_start),
                                rt.end.clone(),
                                rt.seqno,
                            ));
                        }
                    }
                }
            } else {
                // Flush mode: write ALL RTs without clipping so they cover keys
                // in older SSTs outside this memtable's key range. No overlap
                // filter — an RT disjoint from this table's KV range (e.g.,
                // delete_range on keys only in older SSTs) must still be persisted.
                //
                // Conservatively widen key_range to include RT coverage so leveled
                // compaction overlap selection can discover these RTs. Using rt.end
                // (exclusive) as an inclusive upper bound over-approximates the
                // actual KV max but does not lose entries.
                for rt in tombstones {
                    match &mut writer.meta.first_key {
                        Some(existing) => {
                            if rt.start.as_ref() < existing.as_ref() {
                                *existing = rt.start.clone();
                            }
                        }
                        None => {
                            writer.meta.first_key = Some(rt.start.clone());
                        }
                    }
                    match &mut writer.meta.last_key {
                        Some(existing) => {
                            if rt.end.as_ref() > existing.as_ref() {
                                *existing = rt.end.clone();
                            }
                        }
                        None => {
                            writer.meta.last_key = Some(rt.end.clone());
                        }
                    }
                    writer.write_range_tombstone(rt.clone());
                }
            }
        } else {
            // RT-only table (no KV items yet) — write all tombstones unclipped.
            for rt in tombstones {
                writer.write_range_tombstone(rt.clone());
            }
        }
    }

    pub fn register_blob(&mut self, indirection: BlobIndirection) {
        self.linked_blobs
            .entry(indirection.vhandle.blob_file_id)
            .and_modify(|entry| {
                entry.bytes += u64::from(indirection.size);
                entry.on_disk_bytes += u64::from(indirection.vhandle.on_disk_size);
                entry.len += 1;
            })
            .or_insert_with(|| LinkedFile {
                blob_file_id: indirection.vhandle.blob_file_id,
                bytes: u64::from(indirection.size),
                on_disk_bytes: u64::from(indirection.vhandle.on_disk_size),
                len: 1,
            });
    }

    #[must_use]
    pub fn use_partitioned_index(mut self) -> Self {
        self.use_partitioned_index = true;
        self.writer = self.writer.use_partitioned_index();
        self
    }

    #[must_use]
    pub fn use_partitioned_filter(mut self) -> Self {
        self.use_partitioned_filter = true;
        self.writer = self.writer.use_partitioned_filter();
        self
    }

    #[must_use]
    pub fn use_data_block_restart_interval(mut self, interval: u8) -> Self {
        self.data_block_restart_interval = interval;
        self.writer = self.writer.use_data_block_restart_interval(interval);
        self
    }

    #[must_use]
    pub fn use_index_block_restart_interval(mut self, interval: u8) -> Self {
        self.index_block_restart_interval = interval;
        self.writer = self.writer.use_index_block_restart_interval(interval);
        self
    }

    #[must_use]
    pub fn use_data_block_hash_ratio(mut self, ratio: f32) -> Self {
        self.data_block_hash_ratio = ratio;
        self.writer = self.writer.use_data_block_hash_ratio(ratio);
        self
    }

    #[must_use]
    pub(crate) fn use_data_block_size(mut self, size: u32) -> Self {
        assert!(
            size <= 4 * 1_024 * 1_024,
            "data block size must be <= 4 MiB",
        );
        self.data_block_size = size;
        self.writer = self.writer.use_data_block_size(size);
        self
    }

    #[must_use]
    pub fn use_data_block_compression(mut self, compression: CompressionType) -> Self {
        self.data_block_compression = compression;
        self.writer = self.writer.use_data_block_compression(compression);
        self
    }

    #[must_use]
    pub fn use_index_block_compression(mut self, compression: CompressionType) -> Self {
        self.index_block_compression = compression;
        self.writer = self.writer.use_index_block_compression(compression);
        self
    }

    #[must_use]
    pub fn use_bloom_policy(mut self, bloom_policy: BloomConstructionPolicy) -> Self {
        self.bloom_policy = bloom_policy;
        self.writer = self.writer.use_bloom_policy(bloom_policy);
        self
    }

    #[must_use]
    pub fn use_prefix_extractor(mut self, extractor: Option<Arc<dyn PrefixExtractor>>) -> Self {
        self.prefix_extractor.clone_from(&extractor);
        self.writer = self.writer.use_prefix_extractor(extractor);
        self
    }

    #[must_use]
    pub fn use_encryption(mut self, encryption: Option<Arc<dyn EncryptionProvider>>) -> Self {
        self.encryption.clone_from(&encryption);
        self.writer = self.writer.use_encryption(encryption);
        self
    }

    #[cfg(feature = "zstd")]
    #[must_use]
    pub fn use_zstd_dictionary(
        mut self,
        dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
    ) -> Self {
        self.zstd_dictionary.clone_from(&dictionary);
        self.writer = self.writer.use_zstd_dictionary(dictionary);
        self
    }

    /// Flushes the current writer, stores its metadata, and sets up a new writer for the next table
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating table writer");

        let new_table_id = self.table_id_generator.next();
        let path = self.base_path.join(new_table_id.to_string());

        let mut new_writer = Writer::new(path, new_table_id, self.initial_level, self.fs.clone())?
            .use_data_block_compression(self.data_block_compression)
            .use_index_block_compression(self.index_block_compression)
            .use_data_block_size(self.data_block_size)
            .use_data_block_restart_interval(self.data_block_restart_interval)
            .use_index_block_restart_interval(self.index_block_restart_interval)
            .use_bloom_policy(self.bloom_policy)
            .use_data_block_hash_ratio(self.data_block_hash_ratio);

        if self.use_partitioned_index {
            new_writer = new_writer.use_partitioned_index();
        }
        if self.use_partitioned_filter {
            new_writer = new_writer.use_partitioned_filter();
        }

        new_writer = new_writer.use_prefix_extractor(self.prefix_extractor.clone());
        new_writer = new_writer.use_encryption(self.encryption.clone());

        #[cfg(feature = "zstd")]
        {
            new_writer = new_writer.use_zstd_dictionary(self.zstd_dictionary.clone());
        }

        let mut old_writer = std::mem::replace(&mut self.writer, new_writer);
        old_writer.spill_block()?;

        // Write range tombstones to the finishing writer.
        // In flush mode (clip=false) tombstones are written unmodified because
        // they must cover keys in older SSTs outside this memtable's key range.
        // In compaction mode (clip=true) tombstones are clipped to the table's
        // responsibility range [first_key, current_key) — current_key is the
        // first key of the NEW table, so this covers the gap between tables.
        if !self.range_tombstones.is_empty() {
            Self::write_rts_to_writer(
                &self.range_tombstones,
                self.clip_range_tombstones,
                &mut old_writer,
                self.current_key.as_ref(),
            );
        }

        for linked in self.linked_blobs.values() {
            old_writer.link_blob_file(
                linked.blob_file_id,
                linked.len,
                linked.bytes,
                linked.on_disk_bytes,
            );
        }
        self.linked_blobs.clear();

        if let Some((table_id, checksum)) = old_writer.finish()? {
            self.results.push((table_id, checksum));
        }

        Ok(())
    }

    /// Writes an item
    pub fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        let is_next_key = self.current_key.as_ref() < Some(&item.key.user_key);

        if is_next_key {
            self.current_key = Some(item.key.user_key.clone());

            if *self.writer.meta.file_pos >= self.target_size {
                self.rotate()?;
            }
        }

        self.writer.write(item)?;

        Ok(())
    }

    /// Finishes the last table, making sure all data is written durably
    ///
    /// Returns the metadata of created tables
    pub fn finish(mut self) -> crate::Result<Vec<(TableId, Checksum)>> {
        self.writer.spill_block()?;

        // Write range tombstones to the last writer. No next table exists,
        // so clip_upper=None falls back to upper_bound_exclusive(last_key).
        if !self.range_tombstones.is_empty() {
            Self::write_rts_to_writer(
                &self.range_tombstones,
                self.clip_range_tombstones,
                &mut self.writer,
                None,
            );
        }

        for linked in self.linked_blobs.values() {
            self.writer.link_blob_file(
                linked.blob_file_id,
                linked.len,
                linked.bytes,
                linked.on_disk_bytes,
            );
        }

        if let Some((table_id, checksum)) = self.writer.finish()? {
            self.results.push((table_id, checksum));
        }

        Ok(self.results)
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
    use crate::{config::CompressionPolicy, AbstractTree, Config, SeqNo, SequenceNumberCounter};
    use test_log::test;

    // NOTE: Tests that versions of the same key stay
    // in the same table even if it needs to be rotated
    //
    // This avoids tables' key ranges overlapping
    //
    // http://github.com/fjall-rs/lsm-tree/commit/f46b6fe26a1e90113dc2dbb0342db160a295e616
    #[test]
    fn table_multi_writer_same_key_norotate() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
        .index_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
        .open()?;

        tree.insert("a", "a1".repeat(4_000), 0);
        tree.insert("a", "a2".repeat(4_000), 1);
        tree.insert("a", "a3".repeat(4_000), 2);
        tree.insert("a", "a4".repeat(4_000), 3);
        tree.insert("a", "a5".repeat(4_000), 4);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.len(SeqNo::MAX, None)?);

        tree.major_compact(1_024, 0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.len(SeqNo::MAX, None)?);

        Ok(())
    }

    // Regression (#32): compaction clip must preserve RT covering gap between
    // output tables.  Before the fix, MultiWriter clipped each RT to
    // [first_key, upper_bound(last_key)) — RTs in the gap were dropped by all
    // tables.  The fix clips to [first_key, next_table_first_key) during
    // rotation, covering the gap, and widens key_range so point reads find it.
    #[test]
    fn clip_preserves_rt_covering_gap_between_output_tables() -> crate::Result<()> {
        use crate::range_tombstone::RangeTombstone;
        use crate::{fs::StdFs, InternalValue, UserKey};
        use std::sync::Arc;

        let folder = tempfile::tempdir()?;
        let base_path = folder.path().join("tables");
        std::fs::create_dir_all(&base_path)?;

        let id_gen = SequenceNumberCounter::default();
        let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

        // Tiny target_size to force rotation between "l" and "q"
        let mut mw = super::MultiWriter::new(base_path.clone(), id_gen, 100, 1, fs)?
            .use_clip_range_tombstones();

        mw.set_range_tombstones(vec![RangeTombstone::new(
            UserKey::from(b"m" as &[u8]),
            UserKey::from(b"p" as &[u8]),
            20,
        )]);

        // Table 1: keys [a, l]  — values large enough to fill a 4 KiB data
        // block and push file_pos past target_size so rotation fires on "q".
        mw.write(InternalValue::from_components(
            UserKey::from(b"a" as &[u8]),
            vec![0u8; 4_000],
            1,
            crate::ValueType::Value,
        ))?;
        mw.write(InternalValue::from_components(
            UserKey::from(b"l" as &[u8]),
            vec![0u8; 4_000],
            2,
            crate::ValueType::Value,
        ))?;
        // Table 2: keys [q, z]  — rotation happens before "q"
        mw.write(InternalValue::from_components(
            UserKey::from(b"q" as &[u8]),
            vec![0u8; 4_000],
            3,
            crate::ValueType::Value,
        ))?;
        mw.write(InternalValue::from_components(
            UserKey::from(b"z" as &[u8]),
            vec![0u8; 4_000],
            4,
            crate::ValueType::Value,
        ))?;

        let results = mw.finish()?;
        assert!(
            results.len() >= 2,
            "expected 2+ output tables to verify gap, got {}",
            results.len(),
        );

        // Recover each output table and count preserved RTs
        let cache = Arc::new(crate::Cache::with_capacity_bytes(64 * 1_024));
        let comparator: crate::SharedComparator = Arc::new(crate::DefaultUserComparator);
        let mut total_rts = 0;

        for (table_id, checksum) in &results {
            let table = crate::Table::recover(
                base_path.join(table_id.to_string()),
                *checksum,
                0,
                0,
                cache.clone(),
                None,
                false,
                false,
                None,
                #[cfg(feature = "zstd")]
                None,
                comparator.clone(),
                #[cfg(feature = "metrics")]
                Arc::new(crate::Metrics::default()),
            )?;
            total_rts += table.range_tombstones().len();
        }

        assert!(
            total_rts > 0,
            "BUG: RT [m,p)@20 was dropped by compaction clip — \
             no output table preserved it (gap between tables)",
        );

        Ok(())
    }

    // Edge case (#32): RT spans past the next table's first key, so
    // clipped.end == clip_upper.  Widening last_key to clip_upper would
    // make adjacent tables' key_ranges overlap and break Run::get_for_key_cmp.
    // Verify the RT is still written but key_range stays disjoint.
    #[test]
    fn clip_rt_spanning_next_table_does_not_overlap_key_ranges() -> crate::Result<()> {
        use crate::{fs::StdFs, InternalValue, UserKey};
        use std::sync::Arc;

        let folder = tempfile::tempdir()?;
        let base_path = folder.path().join("tables");
        std::fs::create_dir_all(&base_path)?;

        let id_gen = SequenceNumberCounter::default();
        let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

        let mut mw = super::MultiWriter::new(base_path.clone(), id_gen, 100, 1, fs)?
            .use_clip_range_tombstones();

        // RT [m, r) — end "r" > next table's first key "q", so after
        // clipping to [first_key, clip_upper="q") the clipped.end == "q".
        mw.set_range_tombstones(vec![crate::range_tombstone::RangeTombstone::new(
            UserKey::from(b"m" as &[u8]),
            UserKey::from(b"r" as &[u8]),
            20,
        )]);

        // Table 1: [a, l]
        mw.write(InternalValue::from_components(
            UserKey::from(b"a" as &[u8]),
            vec![0u8; 4_000],
            1,
            crate::ValueType::Value,
        ))?;
        mw.write(InternalValue::from_components(
            UserKey::from(b"l" as &[u8]),
            vec![0u8; 4_000],
            2,
            crate::ValueType::Value,
        ))?;
        // Table 2: [q, z]
        mw.write(InternalValue::from_components(
            UserKey::from(b"q" as &[u8]),
            vec![0u8; 4_000],
            3,
            crate::ValueType::Value,
        ))?;
        mw.write(InternalValue::from_components(
            UserKey::from(b"z" as &[u8]),
            vec![0u8; 4_000],
            4,
            crate::ValueType::Value,
        ))?;

        let results = mw.finish()?;
        assert!(results.len() >= 2);

        let cache = Arc::new(crate::Cache::with_capacity_bytes(64 * 1_024));
        let comparator: crate::SharedComparator = Arc::new(crate::DefaultUserComparator);

        let mut tables = Vec::new();
        for (table_id, checksum) in &results {
            tables.push(crate::Table::recover(
                base_path.join(table_id.to_string()),
                *checksum,
                0,
                0,
                cache.clone(),
                None,
                false,
                false,
                None,
                #[cfg(feature = "zstd")]
                None,
                comparator.clone(),
                #[cfg(feature = "metrics")]
                Arc::new(crate::Metrics::default()),
            )?);
        }

        // Key ranges must be disjoint: table1.max < table2.min
        let t1_max = tables[0].metadata.key_range.max();
        let t2_min = tables[1].metadata.key_range.min();
        assert!(
            t1_max.as_ref() < t2_min.as_ref(),
            "key_ranges must be disjoint: table1.max={:?} must be < table2.min={:?}",
            t1_max,
            t2_min,
        );

        // RT must still be written to at least one output table
        let total_rts: usize = tables.iter().map(|t| t.range_tombstones().len()).sum();
        assert!(
            total_rts > 0,
            "RT [m,r)@20 must be preserved in at least one output table",
        );

        Ok(())
    }

    // NOTE: Follow-up fix for non-disjoint output
    //
    // https://github.com/fjall-rs/lsm-tree/commit/1609a57c2314420b858d826790ecd1442aa76720
    #[test]
    fn table_multi_writer_same_key_norotate_2() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
        .index_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
        .open()?;

        tree.insert("a", "a1".repeat(4_000), 0);
        tree.insert("a", "a1".repeat(4_000), 1);
        tree.insert("a", "a1".repeat(4_000), 2);
        tree.insert("b", "a1".repeat(4_000), 0);
        tree.insert("c", "a1".repeat(4_000), 0);
        tree.insert("c", "a1".repeat(4_000), 1);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(3, tree.len(SeqNo::MAX, None)?);

        tree.major_compact(1_024, 0)?;
        assert_eq!(3, tree.table_count());
        assert_eq!(3, tree.len(SeqNo::MAX, None)?);

        Ok(())
    }
}
