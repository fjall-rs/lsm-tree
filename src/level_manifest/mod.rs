// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub(crate) mod hidden_set;

use crate::{
    coding::{DecodeError, Encode},
    file::{fsync_directory, rewrite_atomic, MAGIC_BYTES},
    segment::Segment,
    version::{Level, Run, Version, VersionId, DEFAULT_LEVEL_COUNT},
    SegmentId, SeqNo,
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use hidden_set::HiddenSet;
use std::{
    collections::VecDeque,
    io::{BufWriter, Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

/// Represents the levels of a log-structured merge tree
pub struct LevelManifest {
    /// Path of tree folder.
    folder: PathBuf,

    /// Current version.
    current: Version,

    /// Set of segment IDs that are masked.
    ///
    /// While consuming segments (because of compaction) they will not appear in the list of segments
    /// as to not cause conflicts between multiple compaction threads (compacting the same segments).
    hidden_set: HiddenSet,

    /// Holds onto versions until they are safe to drop.
    version_free_list: VecDeque<Version>,
}

impl std::fmt::Display for LevelManifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, level) in self.current.iter_levels().enumerate() {
            writeln!(
                f,
                "{idx} [{}], r={}: ",
                match (level.is_empty(), level.is_disjoint()) {
                    (true, _) => ".",
                    (false, true) => "D",
                    (false, false) => "_",
                },
                level.len(),
            )?;

            for run in level.iter() {
                write!(f, "  ")?;

                if run.len() >= 30 {
                    #[allow(clippy::indexing_slicing)]
                    for segment in run.iter().take(2) {
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
                    for segment in run.iter().rev().take(2).rev() {
                        let id = segment.id();
                        let is_hidden = self.hidden_set.is_hidden(id);

                        write!(
                            f,
                            "{}{id}{}",
                            if is_hidden { "(" } else { "[" },
                            if is_hidden { ")" } else { "]" },
                        )?;
                    }

                    writeln!(
                        f,
                        " | # = {}, {} MiB",
                        run.len(),
                        run.iter().map(Segment::file_size).sum::<u64>() / 1_024 / 1_024,
                    )?;
                } else {
                    for segment in run.iter() {
                        let id = segment.id();
                        let is_hidden = self.hidden_set.is_hidden(id);

                        write!(
                            f,
                            "{}{id}{}",
                            if is_hidden { "(" } else { "[" },
                            if is_hidden { ")" } else { "]" },
                        )?;
                    }

                    writeln!(
                        f,
                        " | # = {}, {} MiB",
                        run.len(),
                        run.iter().map(Segment::file_size).sum::<u64>() / 1_024 / 1_024,
                    )?;
                }
            }
        }

        Ok(())
    }
}

impl LevelManifest {
    #[must_use]
    pub fn current_version(&self) -> &Version {
        &self.current
    }

    pub(crate) fn is_compacting(&self) -> bool {
        !self.hidden_set.is_empty()
    }

    pub(crate) fn create_new<P: Into<PathBuf>>(folder: P) -> crate::Result<Self> {
        // assert!(level_count > 0, "level_count should be >= 1");

        #[allow(unused_mut)]
        let mut manifest = Self {
            folder: folder.into(),
            current: Version::new(0),
            hidden_set: HiddenSet::default(),
            version_free_list: VecDeque::default(),
        };

        Self::persist_version(&manifest.folder, &manifest.current)?;

        Ok(manifest)
    }

    // TODO: move into Version::decode
    pub(crate) fn load_version(path: &Path) -> crate::Result<Vec<Vec<Vec<SegmentId>>>> {
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
            let run_count = level_manifest.read_u8()?;

            for _ in 0..run_count {
                let mut run = vec![];
                let segment_count = level_manifest.read_u32::<LittleEndian>()?;

                for _ in 0..segment_count {
                    let id = level_manifest.read_u64::<LittleEndian>()?;
                    run.push(id);
                }

                level.push(run);
            }

            levels.push(level);
        }

        Ok(levels)
    }

    pub(crate) fn recover_ids(
        folder: &Path,
    ) -> crate::Result<crate::HashMap<SegmentId, u8 /* Level index */>> {
        let curr_version = Self::get_current_version(folder)?;
        let version_file_path = folder.join(format!("v{curr_version}"));

        let manifest = Self::load_version(&version_file_path)?;
        let mut result = crate::HashMap::default();

        for (level_idx, segment_ids) in manifest.into_iter().enumerate() {
            for run in segment_ids {
                for segment_id in run {
                    // NOTE: We know there are always less than 256 levels
                    #[allow(clippy::expect_used)]
                    result.insert(
                        segment_id,
                        level_idx
                            .try_into()
                            .expect("there are less than 256 levels"),
                    );
                }
            }
        }

        Ok(result)
    }

    pub fn get_current_version(folder: &Path) -> crate::Result<VersionId> {
        let mut buf = [0; 8];

        {
            let mut file = std::fs::File::open(folder.join("current"))?;
            file.read_exact(&mut buf)?;
        }

        Ok(u64::from_le_bytes(buf))
    }

    pub(crate) fn recover<P: Into<PathBuf>>(
        folder: P,
        segments: &[Segment],
    ) -> crate::Result<Self> {
        let folder = folder.into();

        let curr_version = Self::get_current_version(&folder)?;
        let version_file_path = folder.join(format!("v{curr_version}"));

        let version_file = std::path::Path::new(&version_file_path);

        if !version_file.try_exists()? {
            log::error!("Cannot find version file {}", version_file_path.display());
            return Err(crate::Error::Unrecoverable);
        }

        let raw_version = Self::load_version(&version_file_path)?;

        let version_levels = raw_version
            .iter()
            .map(|level| {
                let level_runs = level
                    .iter()
                    .map(|run| {
                        let run_segments = run
                            .iter()
                            .map(|segment_id| {
                                segments
                                    .iter()
                                    .find(|x| x.id() == *segment_id)
                                    .cloned()
                                    .ok_or(crate::Error::Unrecoverable)
                            })
                            .collect::<crate::Result<Vec<_>>>()?;

                        Ok(Arc::new(Run::new(run_segments)))
                    })
                    .collect::<crate::Result<Vec<_>>>()?;

                Ok(Level::from_runs(level_runs))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Self {
            current: Version::from_levels(curr_version, version_levels),
            folder,
            hidden_set: HiddenSet::default(),
            version_free_list: VecDeque::default(), // TODO: 3. create free list from versions that are N < CURRENT
        })
    }

    fn persist_version(folder: &Path, version: &Version) -> crate::Result<()> {
        log::trace!(
            "Persisting version {} in {}",
            version.id(),
            folder.display(),
        );

        let file = std::fs::File::create_new(folder.join(format!("v{}", version.id())))?;
        let mut writer = BufWriter::new(file);

        version.encode_into(&mut writer)?;

        writer.flush()?;
        writer.get_mut().sync_all()?;
        fsync_directory(folder)?;
        // IMPORTANT: ^ wait for fsync and directory sync to fully finish

        rewrite_atomic(&folder.join("current"), &version.id().to_le_bytes())?;

        Ok(())
    }

    /// Modifies the level manifest atomically.
    ///
    /// The function accepts a transition function that receives the current version
    /// and returns a new version.
    ///
    /// The function takes care of persisting the version changes on disk.
    pub(crate) fn atomic_swap<F: FnOnce(&Version) -> Version>(
        &mut self,
        f: F,
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        // NOTE: Copy-on-write...
        //
        // Create a copy of the levels we can operate on
        // without mutating the current level manifest
        // If persisting to disk fails, this way the level manifest
        // is unchanged
        let next_version = f(&self.current);

        Self::persist_version(&self.folder, &next_version)?;

        let mut old_version = std::mem::replace(&mut self.current, next_version);
        old_version.seqno_watermark = gc_watermark;

        self.version_free_list.push_back(old_version);

        Ok(())
    }

    pub(crate) fn maintenance(&mut self, gc_watermark: SeqNo) -> crate::Result<()> {
        loop {
            let Some(head) = self.version_free_list.front() else {
                break;
            };

            if head.seqno_watermark < gc_watermark {
                let path = self.folder.join(format!("v{}", head.id()));
                std::fs::remove_file(path)?;
                self.version_free_list.pop_front();
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Returns `true` if there are no segments
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of levels in the tree
    #[must_use]
    pub fn level_count(&self) -> u8 {
        // NOTE: Level count is u8
        #[allow(clippy::cast_possible_truncation)]
        {
            self.current.level_count() as u8
        }
    }

    /// Returns the number of levels in the tree.
    #[must_use]
    pub fn last_level_index(&self) -> u8 {
        DEFAULT_LEVEL_COUNT - 1
    }

    /// Returns the number of segments, summed over all levels
    #[must_use]
    pub fn len(&self) -> usize {
        self.current.segment_count()
    }

    /// Returns the (compressed) size of all segments
    #[must_use]
    pub fn size(&self) -> u64 {
        self.iter().map(Segment::file_size).sum()
    }

    #[must_use]
    pub fn level_is_busy(&self, idx: usize) -> bool {
        self.current.level(idx).is_some_and(|level| {
            level
                .iter()
                .flat_map(|run| run.iter())
                .any(|segment| self.hidden_set.is_hidden(segment.id()))
        })
    }

    pub(crate) fn get_segment(&self, id: SegmentId) -> Option<&Segment> {
        self.current.iter_segments().find(|x| x.metadata.id == id)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[Level] {
        &self.current.levels
    }

    pub fn iter(&self) -> impl Iterator<Item = &Segment> {
        self.current.iter_segments()
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::{
        coding::Encode,
        level_manifest::{hidden_set::HiddenSet, LevelManifest},
        version::Version,
        AbstractTree,
    };
    use std::collections::VecDeque;
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
        tree.manifest.write().expect("lock is poisoned").folder = "/invaliiid/asd".into();

        assert!(tree.major_compact(u64::MAX, 4).is_err());

        assert!(tree
            .manifest
            .read()
            .expect("lock is poisoned")
            .hidden_set
            .is_empty());

        assert_eq!(segment_count_before_major_compact, tree.segment_count());

        Ok(())
    }
}
