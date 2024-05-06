pub mod iter;
mod level;

#[cfg(feature = "segment_history")]
mod segment_history;

#[cfg(feature = "segment_history")]
use crate::time::unix_timestamp;

use self::level::Level;
use crate::{
    file::rewrite_atomic,
    segment::{meta::SegmentId, Segment},
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
};

pub type HiddenSet = HashSet<SegmentId>;

/// Represents the levels of a log-structured merge tree.
pub struct LevelManifest {
    path: PathBuf,

    #[doc(hidden)]
    pub levels: Vec<Level>,

    /// Set of segment IDs that are masked
    ///
    /// While consuming segments (because of compaction) they will not appear in the list of segments
    /// as to not cause conflicts between multiple compaction threads (compacting the same segments)
    hidden_set: HiddenSet,

    #[cfg(feature = "segment_history")]
    segment_history_writer: segment_history::Writer,
}

impl LevelManifest {
    pub(crate) fn is_compacting(&self) -> bool {
        !self.hidden_set.is_empty()
    }

    pub(crate) fn create_new<P: AsRef<Path>>(level_count: u8, path: P) -> crate::Result<Self> {
        assert!(level_count > 0, "level_count should be >= 1");

        let levels = (0..level_count)
            .map(|_| Level::default())
            .collect::<Vec<_>>();

        let mut levels = Self {
            path: path.as_ref().to_path_buf(),
            levels,
            hidden_set: HashSet::with_capacity(10),

            #[cfg(feature = "segment_history")]
            segment_history_writer: segment_history::Writer::new()?,
        };
        levels.write_to_disk()?;

        #[cfg(feature = "segment_history")]
        levels.write_segment_history_entry("create_new")?;

        Ok(levels)
    }

    #[cfg(feature = "segment_history")]
    fn write_segment_history_entry(&mut self, event: &str) -> crate::Result<()> {
        let ts = unix_timestamp();

        let line = serde_json::to_string(&serde_json::json!({
            "time_unix": ts.as_secs(),
            "time_ms": ts.as_millis(),
            "event": event,
            "levels": self.levels.iter().map(|level| {
                level.segments
                .iter()
                .map(|segment| serde_json::json!({
                        "id": segment.metadata.id,
                        "metadata": segment.metadata.clone(),
                        "hidden": self.hidden_set.contains(&segment.metadata.id)
                    }))
                    .collect::<Vec<_>>()
            }).collect::<Vec<_>>()
        }))
        .expect("Segment history write failed");

        self.segment_history_writer.write(&line)
    }

    pub(crate) fn load_level_manifest<P: AsRef<Path>>(
        path: P,
    ) -> crate::Result<Vec<Vec<SegmentId>>> {
        let mut level_manifest = Cursor::new(std::fs::read(&path)?);

        let mut levels = vec![];

        let level_count = level_manifest.read_u32::<BigEndian>()?;

        for _ in 0..level_count {
            let mut level = vec![];
            let segment_count = level_manifest.read_u32::<BigEndian>()?;

            for _ in 0..segment_count {
                let id = level_manifest.read_u64::<BigEndian>()?;
                level.push(id);
            }

            levels.push(level);
        }

        Ok(levels)
    }

    pub(crate) fn recover_ids<P: AsRef<Path>>(path: P) -> crate::Result<Vec<SegmentId>> {
        Ok(Self::load_level_manifest(path)?
            .into_iter()
            .flatten()
            .collect())
    }

    fn resolve_levels(
        level_manifest: Vec<Vec<SegmentId>>,
        segments: &HashMap<SegmentId, Arc<Segment>>,
    ) -> Vec<Level> {
        let mut levels = Vec::with_capacity(level_manifest.len());

        for level in level_manifest {
            let mut created_level = Level::default();

            for id in level {
                let segment = segments.get(&id).cloned().expect("should find segment");
                created_level.insert(segment);
            }

            levels.push(created_level);
        }

        levels
    }

    pub(crate) fn recover<P: AsRef<Path>>(
        path: P,
        segments: Vec<Arc<Segment>>,
    ) -> crate::Result<Self> {
        let level_manifest = Self::load_level_manifest(&path)?;

        let segments: HashMap<_, _> = segments
            .into_iter()
            .map(|seg| (seg.metadata.id, seg))
            .collect();

        let levels = Self::resolve_levels(level_manifest, &segments);

        // NOTE: See segment_history feature
        #[allow(unused_mut)]
        let mut levels = Self {
            levels,
            hidden_set: HashSet::with_capacity(10),
            path: path.as_ref().to_path_buf(),

            #[cfg(feature = "segment_history")]
            segment_history_writer: segment_history::Writer::new()?,
        };

        #[cfg(feature = "segment_history")]
        levels.write_segment_history_entry("load_from_disk")?;

        Ok(levels)
    }

    pub(crate) fn write_to_disk(&mut self) -> crate::Result<()> {
        log::trace!("Writing level manifest to {:?}", self.path);

        let mut serialized = vec![];
        serialized.write_u32::<BigEndian>(self.levels.len() as u32)?;

        for level in &self.levels {
            serialized.write_u32::<BigEndian>(level.segments.len() as u32)?;

            for segment in &level.segments {
                serialized.write_u64::<BigEndian>(segment.metadata.id)?;
            }
        }

        // NOTE: Compaction threads don't have concurrent access to the level manifest
        // because it is behind a mutex
        // *However*, the file still needs to be rewritten atomically, because
        // the system could crash at any moment, so
        //
        // a) truncating is not an option, because for a short moment, the file is empty
        // b) just overwriting corrupts the file content
        rewrite_atomic(&self.path, &serialized)?;

        Ok(())
    }

    pub(crate) fn add(&mut self, segment: Arc<Segment>) {
        self.insert_into_level(0, segment);
    }

    /// Sorts all levels from newest to oldest
    ///
    /// This will make segments with highest seqno get checked first,
    /// so if there are two versions of an item, the fresher one is seen first:
    ///
    /// segment a   segment b
    /// [key:asd:2] [key:asd:1]
    ///
    /// point read ----------->
    pub(crate) fn sort_levels(&mut self) {
        for level in &mut self.levels {
            level.sort_by_seqno();
        }
    }

    pub(crate) fn insert_into_level(&mut self, level_no: u8, segment: Arc<Segment>) {
        let last_level_index = self.depth() - 1;
        let index = level_no.clamp(0, last_level_index);

        let level = self
            .levels
            .get_mut(index as usize)
            .expect("level should exist");

        level.insert(segment);

        #[cfg(feature = "segment_history")]
        self.write_segment_history_entry("insert").ok();
    }

    pub(crate) fn remove(&mut self, segment_id: SegmentId) {
        for level in &mut self.levels {
            level.remove(segment_id);
        }

        #[cfg(feature = "segment_history")]
        self.write_segment_history_entry("remove").ok();
    }

    /// Returns `true` if there are no segments
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the amount of levels in the tree
    #[must_use]
    pub fn depth(&self) -> u8 {
        self.levels.len() as u8
    }

    pub fn first_level_segment_count(&self) -> usize {
        self.levels.first().expect("L0 should always exist").len()
    }

    /// Returns the amount of levels in the tree
    #[must_use]
    pub fn last_level_index(&self) -> u8 {
        self.depth() - 1
    }

    /// Returns the amount of segments, summed over all levels
    #[must_use]
    pub fn len(&self) -> usize {
        self.levels.iter().map(|level| level.len()).sum()
    }

    /// Returns the (compressed) size of all segments
    #[must_use]
    pub fn size(&self) -> u64 {
        let segment_iter = iter::LevelManifestIterator::new(self);
        segment_iter.map(|s| s.metadata.file_size).sum()
    }

    pub fn busy_levels(&self) -> HashSet<u8> {
        let mut output = HashSet::with_capacity(self.len());

        for (idx, level) in self.levels.iter().enumerate() {
            for segment_id in level.ids() {
                if self.hidden_set.contains(&segment_id) {
                    output.insert(idx as u8);
                }
            }
        }

        output
    }

    /// Returns a view into the levels, hiding all segments that currently are being compacted
    #[must_use]
    pub fn resolved_view(&self) -> Vec<Level> {
        let mut output = Vec::with_capacity(self.len());

        for raw_level in &self.levels {
            let mut level = raw_level.clone();

            for id in &self.hidden_set {
                level.remove(*id);
            }

            output.push(level);
        }

        output
    }

    #[doc(hidden)]
    pub fn get_all_segments_flattened(&self) -> Vec<Arc<Segment>> {
        let mut output = Vec::with_capacity(self.len());

        for level in &self.levels {
            for segment in level.segments.iter().cloned() {
                output.push(segment);
            }
        }

        output
    }

    pub(crate) fn get_all_segments(&self) -> HashMap<SegmentId, Arc<Segment>> {
        let segment_iter = iter::LevelManifestIterator::new(self);
        let mut output = HashMap::new();

        for segment in segment_iter {
            output.insert(segment.metadata.id, segment);
        }

        output
    }

    pub(crate) fn get_visible_segments(&self) -> HashMap<SegmentId, Arc<Segment>> {
        let segment_iter = iter::LevelManifestIterator::new(self);
        let mut output = HashMap::new();

        for segment in segment_iter {
            if !self.hidden_set.contains(&segment.metadata.id) {
                output.insert(segment.metadata.id, segment);
            }
        }

        output
    }

    pub(crate) fn show_segments(&mut self, keys: &[SegmentId]) {
        for key in keys {
            self.hidden_set.remove(key);
        }

        #[cfg(feature = "segment_history")]
        self.write_segment_history_entry("show").ok();
    }

    pub(crate) fn hide_segments(&mut self, keys: &[SegmentId]) {
        for key in keys {
            self.hidden_set.insert(*key);
        }

        #[cfg(feature = "segment_history")]
        self.write_segment_history_entry("hide").ok();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        key_range::KeyRange,
        levels::level::Level,
        segment::{
            block_index::BlockIndex,
            meta::{Metadata, SegmentId},
            Segment,
        },
    };
    use std::sync::Arc;

    #[cfg(feature = "bloom")]
    use crate::bloom::BloomFilter;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: SegmentId, key_range: KeyRange) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));

        Arc::new(Segment {
            tree_id: 0,
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index: Arc::new(BlockIndex::new((0, id).into(), block_cache.clone())),
            metadata: Metadata {
                // version: crate::version::Version::V0,
                block_count: 0,
                block_size: 0,
                created_at: 0,
                id,
                file_size: 0,
                compression: crate::segment::meta::CompressionType::Lz4,
                item_count: 0,
                key_count: 0,
                key_range,
                tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, 0),
            },
            block_cache,

            #[cfg(feature = "bloom")]
            bloom_filter: BloomFilter::with_fp_rate(1, 0.1),
        })
    }

    #[test]
    fn level_disjoint() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(&folder).open()?;

        let mut x = 0_u64;

        for _ in 0..10 {
            for _ in 0..10 {
                let key = x.to_be_bytes();
                x += 1;
                tree.insert(key, key, 0);
            }
            tree.flush_active_memtable().expect("should flush");
        }

        assert!(
            tree.levels
                .read()
                .expect("lock is poisoned")
                .levels
                .first()
                .expect("should exist")
                .is_disjoint
        );

        Ok(())
    }

    #[test]
    fn level_not_disjoint() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(&folder).open()?;

        for i in 0..10 {
            tree.insert("a", "", i);
            tree.insert("z", "", i);
            tree.flush_active_memtable().expect("should flush");
        }

        assert!(
            !tree
                .levels
                .read()
                .expect("lock is poisoned")
                .levels
                .first()
                .expect("should exist")
                .is_disjoint
        );

        Ok(())
    }

    #[test]
    fn level_overlaps() {
        let seg0 = fixture_segment(
            1,
            KeyRange::new((b"c".to_vec().into(), b"k".to_vec().into())),
        );
        let seg1 = fixture_segment(
            2,
            KeyRange::new((b"l".to_vec().into(), b"z".to_vec().into())),
        );

        let mut level = Level::default();
        level.insert(seg0);
        level.insert(seg1);

        assert_eq!(
            Vec::<SegmentId>::new(),
            level.get_overlapping_segments(&KeyRange::new((
                b"a".to_vec().into(),
                b"b".to_vec().into()
            ))),
        );

        assert_eq!(
            vec![1],
            level.get_overlapping_segments(&KeyRange::new((
                b"d".to_vec().into(),
                b"k".to_vec().into()
            ))),
        );

        assert_eq!(
            vec![1, 2],
            level.get_overlapping_segments(&KeyRange::new((
                b"f".to_vec().into(),
                b"x".to_vec().into()
            ))),
        );
    }
}
