// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod iter;
pub(crate) mod level;

use crate::{
    coding::{DecodeError, Encode, EncodeError},
    file::{rewrite_atomic, MAGIC_BYTES},
    key_range::KeyRange,
    segment::{meta::SegmentId, Segment},
    HashMap, HashSet,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use iter::LevelManifestIterator;
use level::Level;
use std::{
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

pub type HiddenSet = HashSet<SegmentId>;

type Levels = Vec<Arc<Level>>;

/// Represents the levels of a log-structured merge tree.
pub struct LevelManifest {
    /// Path of level manifest file
    path: PathBuf,

    /// Actual levels containing segments
    #[doc(hidden)]
    pub levels: Levels,

    /// Set of segment IDs that are masked
    ///
    /// While consuming segments (because of compaction) they will not appear in the list of segments
    /// as to not cause conflicts between multiple compaction threads (compacting the same segments)
    hidden_set: HiddenSet,

    is_disjoint: bool,
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

        let levels = (0..level_count).map(|_| Arc::default()).collect::<Vec<_>>();

        #[allow(unused_mut)]
        let mut manifest = Self {
            path: path.as_ref().to_path_buf(),
            levels,
            hidden_set: HashSet::with_capacity_and_hasher(
                10,
                xxhash_rust::xxh3::Xxh3Builder::new(),
            ),
            is_disjoint: true,
        };
        Self::write_to_disk(path, &manifest.deep_clone())?;

        Ok(manifest)
    }

    fn set_disjoint_flag(&mut self) {
        // TODO: store key range in levels precomputed
        let key_ranges = self
            .levels
            .iter()
            .filter(|x| !x.is_empty())
            .map(|x| KeyRange::aggregate(x.iter().map(|s| &s.metadata.key_range)))
            .collect::<Vec<_>>();

        self.is_disjoint = KeyRange::is_disjoint(&key_ranges.iter().collect::<Vec<_>>());
    }

    pub(crate) fn load_level_manifest<P: AsRef<Path>>(
        path: P,
    ) -> crate::Result<Vec<Vec<SegmentId>>> {
        let mut level_manifest = Cursor::new(std::fs::read(&path)?);

        // Check header
        let mut magic = [0u8; MAGIC_BYTES.len()];
        level_manifest.read_exact(&mut magic)?;

        if magic != MAGIC_BYTES {
            return Err(crate::Error::Decode(DecodeError::InvalidHeader(
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
    ) -> Levels {
        let mut levels = Vec::with_capacity(level_manifest.len());

        for level in level_manifest {
            let mut created_level = Level::default();

            for id in level {
                let segment = segments.get(&id).cloned().expect("should find segment");
                created_level.insert(segment);
            }

            levels.push(Arc::new(created_level));
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

        let mut manifest = Self {
            levels,
            hidden_set: HashSet::with_capacity_and_hasher(
                10,
                xxhash_rust::xxh3::Xxh3Builder::new(),
            ),
            path: path.as_ref().to_path_buf(),
            is_disjoint: false,
        };
        manifest.set_disjoint_flag();

        Ok(manifest)
    }

    pub(crate) fn write_to_disk<P: AsRef<Path>>(path: P, levels: &Vec<Level>) -> crate::Result<()> {
        let path = path.as_ref();

        log::trace!("Writing level manifest to {path:?}",);

        let serialized = levels.encode_into_vec()?;

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

    /// Clones the level to get a mutable copy for atomic swap.
    fn deep_clone(&self) -> Vec<Level> {
        self.levels
            .iter()
            .map(|x| Level {
                segments: x.segments.clone(),
                is_disjoint: x.is_disjoint,
            })
            .collect()
    }

    /// Modifies the level manifest atomically.
    pub(crate) fn atomic_swap<F: FnOnce(&mut Vec<Level>)>(&mut self, f: F) -> crate::Result<()> {
        // NOTE: Copy-on-write...
        //
        // Create a copy of the levels we can operate on
        // without mutating the current level manifest
        // If persisting to disk fails, this way the level manifest
        // is unchanged
        let mut working_copy = self.deep_clone();

        f(&mut working_copy);

        Self::write_to_disk(&self.path, &working_copy)?;
        self.levels = working_copy.into_iter().map(Arc::new).collect();
        self.sort_levels();
        self.set_disjoint_flag();

        log::trace!("Swapped level manifest to:\n{self}");

        Ok(())
    }

    #[allow(unused)]
    #[cfg(test)]
    pub(crate) fn add(&mut self, segment: Arc<Segment>) {
        self.insert_into_level(0, segment);
    }

    pub(crate) fn sort_levels(&mut self) {
        for level in &mut self.levels {
            Arc::get_mut(level)
                .expect("could not get mutable Arc - this is a bug")
                .sort();
        }
    }

    #[allow(unused)]
    #[cfg(test)]
    pub(crate) fn insert_into_level(&mut self, level_no: u8, segment: Arc<Segment>) {
        let last_level_index = self.depth() - 1;
        let index = level_no.clamp(0, last_level_index);

        let level = self
            .levels
            .get_mut(index as usize)
            .expect("level should exist");

        let level = Arc::get_mut(level).expect("only used in tests");

        level.insert(segment);
    }

    #[must_use]
    pub fn is_disjoint(&self) -> bool {
        self.is_disjoint && self.levels.iter().all(|x| x.is_disjoint)
        // TODO: not needed? -----------^
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
        self.levels.first().map(|lvl| lvl.len()).unwrap_or_default()
    }

    /// Returns the amount of levels in the tree
    #[must_use]
    pub fn last_level_index(&self) -> u8 {
        self.depth() - 1
    }

    /// Returns the amount of segments, summed over all levels
    #[must_use]
    pub fn len(&self) -> usize {
        self.levels.iter().map(|lvl| lvl.len()).sum()
    }

    /// Returns the (compressed) size of all segments
    #[must_use]
    pub fn size(&self) -> u64 {
        self.iter().map(|s| s.metadata.file_size).sum()
    }

    #[must_use]
    pub fn busy_levels(&self) -> HashSet<u8> {
        let mut output =
            HashSet::with_capacity_and_hasher(self.len(), xxhash_rust::xxh3::Xxh3Builder::new());

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
            let mut level = raw_level.iter().cloned().collect::<Vec<_>>();
            level.retain(|x| !self.hidden_set.contains(&x.metadata.id));

            output.push(Level {
                segments: level,
                is_disjoint: raw_level.is_disjoint,
            });
        }

        output
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<Segment>> + '_ {
        LevelManifestIterator::new(self)
    }

    pub(crate) fn get_all_segments(&self) -> HashMap<SegmentId, Arc<Segment>> {
        let mut output = HashMap::with_hasher(xxhash_rust::xxh3::Xxh3Builder::new());

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

impl Encode for Vec<Level> {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        // Write header
        writer.write_all(&MAGIC_BYTES)?;

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
    use crate::{coding::Encode, level_manifest::LevelManifest, AbstractTree};
    use std::collections::HashSet;
    use test_log::test;

    #[test]
    fn level_manifest_atomicity() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(folder).open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        tree.insert("a", "a", 1);
        tree.flush_active_memtable(0)?;
        tree.insert("a", "a", 2);
        tree.flush_active_memtable(0)?;

        assert_eq!(3, tree.approximate_len());

        tree.major_compact(u64::MAX, 3)?;

        assert_eq!(1, tree.segment_count());

        tree.insert("a", "a", 3);
        tree.flush_active_memtable(0)?;

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
        let manifest = LevelManifest {
            hidden_set: HashSet::default(),
            levels: Vec::default(),
            path: "a".into(),
            is_disjoint: false,
        };

        let bytes = manifest.deep_clone().encode_into_vec()?;

        #[rustfmt::skip]
        let raw = &[
            // Magic
            b'L', b'S', b'M', 2,

            // Count
            0,
        ];

        assert_eq!(bytes, raw);

        Ok(())
    }
}
