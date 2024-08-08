pub mod iter;
mod level;

use self::level::Level;
use crate::{
    file::rewrite_atomic,
    segment::{meta::SegmentId, Segment},
    serde::Serializable,
    DeserializeError, SerializeError,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use iter::LevelManifestIterator;
use std::{
    collections::{HashMap, HashSet},
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

pub const LEVEL_MANIFEST_HEADER_MAGIC: &[u8] = &[b'L', b'S', b'M', b'T', b'L', b'V', b'L', b'2'];

pub type HiddenSet = HashSet<SegmentId>;

/// Represents the levels of a log-structured merge tree.
pub struct LevelManifest {
    /// Path of level manifest file
    path: PathBuf,

    /// Actual levels containing segments
    #[doc(hidden)]
    pub levels: Vec<Level>,

    /// Set of segment IDs that are masked
    ///
    /// While consuming segments (because of compaction) they will not appear in the list of segments
    /// as to not cause conflicts between multiple compaction threads (compacting the same segments)
    hidden_set: HiddenSet,
}

impl std::fmt::Display for LevelManifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, level) in self.levels.iter().enumerate() {
            write!(f, "{idx}: ")?;

            if level.segments.is_empty() {
                write!(f, "<empty>")?;
            } else if level.segments.len() >= 24 {
                #[allow(clippy::indexing_slicing)]
                for segment in level.segments.iter().take(2) {
                    let id = segment.metadata.id;
                    let is_hidden = self.hidden_set.contains(&id);

                    write!(
                        f,
                        "{}{id}{}",
                        if is_hidden { "(" } else { "[" },
                        if is_hidden { ")" } else { "]" },
                    )?;
                }
                write!(f, " . . . . . ")?;

                #[allow(clippy::indexing_slicing)]
                for segment in level.segments.iter().rev().take(2).rev() {
                    let id = segment.metadata.id;
                    let is_hidden = self.hidden_set.contains(&id);

                    write!(
                        f,
                        "{}{id}{}",
                        if is_hidden { "(" } else { "[" },
                        if is_hidden { ")" } else { "]" },
                    )?;
                }
            } else {
                for segment in &level.segments {
                    let id = segment.metadata.id;
                    let is_hidden = self.hidden_set.contains(&id);

                    write!(
                        f,
                        "{}{id}{}",
                        if is_hidden { "(" } else { "[" },
                        /*       segment.metadata.file_size / 1_024 / 1_024, */
                        if is_hidden { ")" } else { "]" },
                    )?;
                }
            }

            writeln!(
                f,
                " | # = {}, {} MiB",
                level.len(),
                level.size() / 1_024 / 1_024
            )?;
        }

        Ok(())
    }
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

        #[allow(unused_mut)]
        let mut levels = Self {
            path: path.as_ref().to_path_buf(),
            levels,
            hidden_set: HashSet::with_capacity(10),
        };
        Self::write_to_disk(path, &levels.levels)?;

        Ok(levels)
    }

    pub(crate) fn load_level_manifest<P: AsRef<Path>>(
        path: P,
    ) -> crate::Result<Vec<Vec<SegmentId>>> {
        let mut level_manifest = Cursor::new(std::fs::read(&path)?);

        // Check header
        let mut magic = [0u8; LEVEL_MANIFEST_HEADER_MAGIC.len()];
        level_manifest.read_exact(&mut magic)?;

        if magic != LEVEL_MANIFEST_HEADER_MAGIC {
            return Err(crate::Error::Deserialize(DeserializeError::InvalidHeader(
                "LevelManifest",
            )));
        }

        let mut levels = vec![];

        let level_count = level_manifest.read_u8()?;

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

        Ok(Self {
            levels,
            hidden_set: HashSet::with_capacity(10),
            path: path.as_ref().to_path_buf(),
        })
    }

    pub(crate) fn write_to_disk<P: AsRef<Path>>(path: P, levels: &Vec<Level>) -> crate::Result<()> {
        let path = path.as_ref();

        log::trace!("Writing level manifest to {path:?}",);

        let mut serialized = vec![];
        levels.serialize(&mut serialized)?;

        // NOTE: Compaction threads don't have concurrent access to the level manifest
        // because it is behind a mutex
        // *However*, the file still needs to be rewritten atomically, because
        // the system could crash at any moment, so
        //
        // a) truncating is not an option, because for a short moment, the file is empty
        // b) just overwriting corrupts the file content
        rewrite_atomic(path, &serialized)?;

        Ok(())
    }

    /// Modifies the level manifest atomically.
    pub(crate) fn atomic_swap<F: FnOnce(&mut Vec<Level>)>(&mut self, f: F) -> crate::Result<()> {
        // NOTE: Create a copy of the levels we can operate on
        // without mutating the current level manifest
        // If persisting to disk fails, this way the level manifest
        // is unchanged
        let mut working_copy = self.levels.clone();

        f(&mut working_copy);

        Self::write_to_disk(&self.path, &working_copy)?;
        self.levels = working_copy;

        log::trace!("Swapped level manifest to:\n{self}");

        Ok(())
    }

    // NOTE: Used in tests
    #[allow(unused)]
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
            level.sort();
        }
    }

    // NOTE: Used in tests
    #[allow(unused)]
    pub(crate) fn insert_into_level(&mut self, level_no: u8, segment: Arc<Segment>) {
        let last_level_index = self.depth() - 1;
        let index = level_no.clamp(0, last_level_index);

        let level = self
            .levels
            .get_mut(index as usize)
            .expect("level should exist");

        level.insert(segment);
    }

    #[must_use]
    pub fn is_disjoint(&self) -> bool {
        self.levels.iter().all(|x| x.is_disjoint)
    }

    /// Returns `true` if there are no segments
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the amount of levels in the tree
    #[must_use]
    pub fn depth(&self) -> u8 {
        // NOTE: Level count is u8
        #[allow(clippy::cast_possible_truncation)]
        let len = self.levels.len() as u8;

        len
    }

    #[must_use]
    pub fn first_level_segment_count(&self) -> usize {
        self.levels.first().map(Level::len).unwrap_or_default()
    }

    /// Returns the amount of levels in the tree
    #[must_use]
    pub fn last_level_index(&self) -> u8 {
        self.depth() - 1
    }

    /// Returns the amount of segments, summed over all levels
    #[must_use]
    pub fn len(&self) -> usize {
        self.levels.iter().map(Level::len).sum()
    }

    /// Returns the (compressed) size of all segments
    #[must_use]
    pub fn size(&self) -> u64 {
        self.iter().map(|s| s.metadata.file_size).sum()
    }

    #[must_use]
    pub fn busy_levels(&self) -> HashSet<u8> {
        let mut output = HashSet::with_capacity(self.len());

        for (idx, level) in self.levels.iter().enumerate() {
            for segment_id in level.ids() {
                if self.hidden_set.contains(&segment_id) {
                    // NOTE: Level count is u8
                    #[allow(clippy::cast_possible_truncation)]
                    let idx = idx as u8;

                    output.insert(idx);
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

    pub fn iter(&self) -> impl Iterator<Item = &Arc<Segment>> + '_ {
        LevelManifestIterator::new(self)
    }

    pub(crate) fn get_all_segments(&self) -> HashMap<SegmentId, Arc<Segment>> {
        let mut output = HashMap::new();

        for segment in self.iter().cloned() {
            output.insert(segment.metadata.id, segment);
        }

        output
    }

    pub(crate) fn show_segments(&mut self, keys: &[SegmentId]) {
        for key in keys {
            self.hidden_set.remove(key);
        }
    }

    pub(crate) fn hide_segments(&mut self, keys: &[SegmentId]) {
        for key in keys {
            self.hidden_set.insert(*key);
        }
    }
}

impl Serializable for Vec<Level> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // Write header
        writer.write_all(LEVEL_MANIFEST_HEADER_MAGIC)?;

        // NOTE: "Truncation" is OK, because levels are created from a u8
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u8(self.len() as u8)?;

        for level in self {
            // NOTE: "Truncation" is OK, because there are never 4 billion segments in a tree, I hope
            #[allow(clippy::cast_possible_truncation)]
            writer.write_u32::<BigEndian>(level.segments.len() as u32)?;

            for segment in &level.segments {
                writer.write_u64::<BigEndian>(segment.metadata.id)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        key_range::KeyRange,
        levels::{level::Level, LevelManifest},
        segment::{
            block_index::BlockIndex,
            file_offsets::FileOffsets,
            meta::{Metadata, SegmentId},
            Segment,
        },
        serde::Serializable,
        AbstractTree,
    };
    use std::{collections::HashSet, sync::Arc};
    use test_log::test;

    #[cfg(feature = "bloom")]
    use crate::bloom::BloomFilter;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: SegmentId, key_range: KeyRange) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));

        Arc::new(Segment {
            tree_id: 0,
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index: Arc::new(BlockIndex::new((0, id).into(), block_cache.clone())),

            offsets: FileOffsets {
                bloom_ptr: 0,
                rf_ptr: 0,
                index_block_ptr: 0,
                metadata_ptr: 0,
                range_tombstones_ptr: 0,
                tli_ptr: 0,
            },

            metadata: Metadata {
                block_count: 0,
                data_block_size: 4_096,
                index_block_size: 4_096,
                created_at: 0,
                id,
                file_size: 0,
                compression: crate::segment::meta::CompressionType::None,
                table_type: crate::segment::meta::TableType::Block,
                item_count: 0,
                key_count: 0,
                key_range,
                tombstone_count: 0,
                range_tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, 0),
            },
            block_cache,

            #[cfg(feature = "bloom")]
            bloom_filter: BloomFilter::with_fp_rate(1, 0.1),
        })
    }

    #[test]
    fn level_manifest_atomicity() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(folder).open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable()?;
        tree.insert("a", "a", 1);
        tree.flush_active_memtable()?;
        tree.insert("a", "a", 2);
        tree.flush_active_memtable()?;

        assert_eq!(3, tree.approximate_len());

        tree.major_compact(u64::MAX, 3)?;

        assert_eq!(1, tree.segment_count());

        tree.insert("a", "a", 3);
        tree.flush_active_memtable()?;

        let segment_count_before_major_compact = tree.segment_count();

        // NOTE: Purposefully change level manifest to have invalid path
        // to force an I/O error
        tree.levels.write().expect("lock is poisoned").path = "/invaliiid/asd".into();

        assert!(tree.major_compact(u64::MAX, 4).is_err());

        assert!(tree
            .levels
            .read()
            .expect("lock is poisoned")
            .hidden_set
            .is_empty());

        assert_eq!(segment_count_before_major_compact, tree.segment_count());

        Ok(())
    }

    #[test]
    fn level_manifest_raw_empty() -> crate::Result<()> {
        let levels = LevelManifest {
            hidden_set: HashSet::default(),
            levels: Vec::default(),
            path: "a".into(),
        };

        let mut bytes = vec![];
        levels.levels.serialize(&mut bytes)?;

        #[rustfmt::skip]
        let raw = &[
            // Magic
            b'L', b'S', b'M', b'T', b'L', b'V', b'L', b'2',

            // Count
            0,
        ];

        assert_eq!(bytes, raw);

        Ok(())
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
            level
                .overlapping_segments(&KeyRange::new((b"a".to_vec().into(), b"b".to_vec().into())))
                .map(|x| x.metadata.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            vec![1],
            level
                .overlapping_segments(&KeyRange::new((b"d".to_vec().into(), b"k".to_vec().into())))
                .map(|x| x.metadata.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            vec![1, 2],
            level
                .overlapping_segments(&KeyRange::new((b"f".to_vec().into(), b"x".to_vec().into())))
                .map(|x| x.metadata.id)
                .collect::<Vec<_>>(),
        );
    }
}
