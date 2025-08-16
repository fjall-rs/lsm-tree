// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub(crate) mod hidden_set;
pub(crate) mod level;

use crate::{
    coding::{DecodeError, Encode, EncodeError},
    file::{rewrite_atomic, MAGIC_BYTES},
    segment::{meta::SegmentId, Segment},
    HashMap, HashSet, KeyRange,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use hidden_set::HiddenSet;
use level::Level;
use std::{
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

type Levels = Vec<Arc<Level>>;

/// Represents the levels of a log-structured merge tree
pub struct LevelManifest {
    /// Path of level manifest file.
    path: PathBuf,

    /// Actual levels containing segments.
    #[doc(hidden)]
    pub levels: Levels,

    /// Set of segment IDs that are masked.
    ///
    /// While consuming segments (because of compaction) they will not appear in the list of segments
    /// as to not cause conflicts between multiple compaction threads (compacting the same segments).
    hidden_set: HiddenSet,

    is_disjoint: bool,
}

impl std::fmt::Display for LevelManifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, level) in self.levels.iter().enumerate() {
            write!(
                f,
                "{idx} [{}]: ",
                match (level.is_empty(), level.compute_is_disjoint()) {
                    (true, _) => ".",
                    (false, true) => "D",
                    (false, false) => "_",
                }
            )?;

            if level.segments.is_empty() {
                write!(f, "<empty>")?;
            } else if level.segments.len() >= 30 {
                #[allow(clippy::indexing_slicing)]
                for segment in level.segments.iter().take(2) {
                    let id = segment.id();
                    let is_hidden = self.hidden_set.is_hidden(id);

                    write!(
                        f,
                        "{}{id}{}",
                        if is_hidden { "(" } else { "[" },
                        if is_hidden { ")" } else { "]" },
                    )?;
                }
                write!(f, " . . . ")?;

                #[allow(clippy::indexing_slicing)]
                for segment in level.segments.iter().rev().take(2).rev() {
                    let id = segment.id();
                    let is_hidden = self.hidden_set.is_hidden(id);

                    write!(
                        f,
                        "{}{id}{}",
                        if is_hidden { "(" } else { "[" },
                        if is_hidden { ")" } else { "]" },
                    )?;
                }
            } else {
                for segment in &level.segments {
                    let id = segment.id();
                    let is_hidden = self.hidden_set.is_hidden(id);

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
                level.size() / 1_024 / 1_024,
            )?;
        }

        Ok(())
    }
}

impl LevelManifest {
    pub(crate) fn is_compacting(&self) -> bool {
        !self.hidden_set.is_empty()
    }

    pub(crate) fn create_new<P: Into<PathBuf>>(level_count: u8, path: P) -> crate::Result<Self> {
        assert!(level_count > 0, "level_count should be >= 1");

        let levels = (0..level_count).map(|_| Arc::default()).collect::<Vec<_>>();

        #[allow(unused_mut)]
        let mut manifest = Self {
            path: path.into(),
            levels,
            hidden_set: HiddenSet::default(),
            is_disjoint: true,
        };
        Self::write_to_disk(&manifest.path, &manifest.deep_clone())?;

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

    pub(crate) fn load_level_manifest(path: &Path) -> crate::Result<Vec<Vec<SegmentId>>> {
        let mut level_manifest = Cursor::new(std::fs::read(path)?);

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

    pub(crate) fn recover_ids(
        path: &Path,
    ) -> crate::Result<crate::HashMap<SegmentId, u8 /* Level index */>> {
        let manifest = Self::load_level_manifest(path)?;
        let mut result = crate::HashMap::default();

        for (level_idx, segment_ids) in manifest.into_iter().enumerate() {
            for segment_id in segment_ids {
                result.insert(
                    segment_id,
                    level_idx
                        .try_into()
                        .expect("there are less than 256 levels"),
                );
            }
        }

        Ok(result)
    }

    fn resolve_levels(
        level_manifest: Vec<Vec<SegmentId>>,
        segments: &HashMap<SegmentId, Segment>,
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

    pub(crate) fn recover<P: Into<PathBuf>>(
        path: P,
        segments: Vec<Segment>,
    ) -> crate::Result<Self> {
        let path = path.into();

        let level_manifest = Self::load_level_manifest(&path)?;

        let segments: HashMap<_, _> = segments.into_iter().map(|seg| (seg.id(), seg)).collect();

        let levels = Self::resolve_levels(level_manifest, &segments);

        let mut manifest = Self {
            levels,
            hidden_set: HiddenSet::default(),
            path,
            is_disjoint: false,
        };
        manifest.set_disjoint_flag();

        Ok(manifest)
    }

    pub(crate) fn write_to_disk(path: &Path, levels: &[Level]) -> crate::Result<()> {
        log::trace!("Writing level manifest to {path:?}");

        let serialized = Runs(levels).encode_into_vec();

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
        self.update_metadata();
        self.set_disjoint_flag();

        log::trace!("Swapped level manifest to:\n{self}");

        Ok(())
    }

    #[allow(unused)]
    #[cfg(test)]
    pub(crate) fn add(&mut self, segment: Segment) {
        self.insert_into_level(0, segment);
    }

    pub fn update_metadata(&mut self) {
        for level in &mut self.levels {
            Arc::get_mut(level)
                .expect("could not get mutable Arc - this is a bug")
                .update_metadata();
        }
    }

    #[allow(unused)]
    #[cfg(test)]
    pub(crate) fn insert_into_level(&mut self, level_no: u8, segment: Segment) {
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
            HashSet::with_capacity_and_hasher(self.len(), Default::default());

        for (idx, level) in self.levels.iter().enumerate() {
            if level.ids().any(|id| self.hidden_set.is_hidden(id)) {
                // NOTE: Level count is u8
                #[allow(clippy::cast_possible_truncation)]
                output.insert(idx as u8);
            }
        }

        output
    }

    pub(crate) fn get_segment(&self, id: SegmentId) -> Option<Segment> {
        for level in &self.levels {
            if let Some(segment) = level.segments.iter().find(|x| x.id() == id).cloned() {
                return Some(segment);
            }
        }
        None
    }

    /// Returns a view into the levels, hiding all segments that currently are being compacted
    #[must_use]
    pub fn resolved_view(&self) -> Vec<Level> {
        let mut output = Vec::with_capacity(self.len());

        for raw_level in &self.levels {
            let mut level = raw_level.iter().cloned().collect::<Vec<_>>();
            level.retain(|x| !self.hidden_set.is_hidden(x.id()));

            output.push(Level {
                segments: level,
                is_disjoint: raw_level.is_disjoint,
            });
        }

        output
    }

    pub fn iter(&self) -> impl Iterator<Item = &Segment> + '_ {
        self.levels.iter().flat_map(|x| &x.segments)
    }

    pub(crate) fn should_decline_compaction<T: IntoIterator<Item = SegmentId>>(
        &self,
        ids: T,
    ) -> bool {
        self.hidden_set().is_blocked(ids)
    }

    pub(crate) fn hidden_set(&self) -> &HiddenSet {
        &self.hidden_set
    }

    pub(crate) fn hide_segments<T: IntoIterator<Item = SegmentId>>(&mut self, keys: T) {
        self.hidden_set.hide(keys);
    }

    pub(crate) fn show_segments<T: IntoIterator<Item = SegmentId>>(&mut self, keys: T) {
        self.hidden_set.show(keys);
    }
}

struct Runs<'a>(&'a [Level]);

impl<'a> std::ops::Deref for Runs<'a> {
    type Target = [Level];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a> Encode for Runs<'a> {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        // Write header
        writer.write_all(&MAGIC_BYTES)?;

        // NOTE: "Truncation" is OK, because levels are created from a u8
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u8(self.len() as u8)?;

        for level in self.iter() {
            // NOTE: "Truncation" is OK, because there are never 4 billion segments in a tree, I hope
            #[allow(clippy::cast_possible_truncation)]
            writer.write_u32::<BigEndian>(level.segments.len() as u32)?;

            for segment in &level.segments {
                writer.write_u64::<BigEndian>(segment.id())?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::Runs;
    use crate::{
        coding::Encode,
        level_manifest::{hidden_set::HiddenSet, LevelManifest},
        AbstractTree,
    };
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
            hidden_set: HiddenSet::default(),
            levels: Vec::default(),
            path: "a".into(),
            is_disjoint: false,
        };

        let bytes = Runs(&manifest.deep_clone()).encode_into_vec();

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
