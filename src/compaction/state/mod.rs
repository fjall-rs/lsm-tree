// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod hidden_set;

use crate::{
    file::{fsync_directory, rewrite_atomic},
    tree::inner::SuperVersion,
    version::Version,
    SeqNo,
};
use hidden_set::HiddenSet;
use std::{
    collections::VecDeque,
    io::BufWriter,
    path::{Path, PathBuf},
};

pub fn persist_version(folder: &Path, version: &Version) -> crate::Result<()> {
    log::trace!(
        "Persisting version {} in {}",
        version.id(),
        folder.display(),
    );

    let path = folder.join(format!("v{}", version.id()));
    let file = std::fs::File::create_new(path)?;
    let writer = BufWriter::new(file);
    let mut writer = sfa::Writer::from_writer(writer);

    version.encode_into(&mut writer)?;

    writer.finish().map_err(|e| match e {
        sfa::Error::Io(e) => crate::Error::from(e),
        _ => unreachable!(),
    })?;

    // IMPORTANT: fsync folder on Unix
    fsync_directory(folder)?;

    rewrite_atomic(&folder.join("current"), &version.id().to_le_bytes())?;

    Ok(())
}

pub struct CompactionState {
    /// Path of tree folder.
    folder: PathBuf,

    /// Set of table IDs that are masked.
    ///
    /// While consuming tables (because of compaction) they will not appear in the list of tables
    /// as to not cause conflicts between multiple compaction threads (compacting the same tables).
    hidden_set: HiddenSet,

    /// Holds onto versions until they are safe to drop.
    version_free_list: VecDeque<Version>,
}

impl CompactionState {
    pub fn new(folder: impl Into<PathBuf>) -> Self {
        Self {
            folder: folder.into(),
            hidden_set: HiddenSet::default(),
            version_free_list: VecDeque::default(),
        }
    }

    pub fn create_new(folder: impl Into<PathBuf>) -> crate::Result<Self> {
        let folder = folder.into();

        persist_version(&folder, &Version::new(0))?;

        Ok(Self::new(folder))
    }

    /// Modifies the level manifest atomically.
    ///
    /// The function accepts a transition function that receives the current version
    /// and returns a new version.
    ///
    /// The function takes care of persisting the version changes on disk.
    pub(crate) fn upgrade_version<F: FnOnce(&Version) -> crate::Result<Version>>(
        &mut self,
        super_version: &mut SuperVersion,
        f: F,
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        // NOTE: Copy-on-write...
        //
        // Create a copy of the levels we can operate on
        // without mutating the current level manifest
        // If persisting to disk fails, this way the level manifest
        // is unchanged
        let next_version = f(&super_version.version)?;

        persist_version(&self.folder, &next_version)?;

        let mut old_version = std::mem::replace(&mut super_version.version, next_version);
        old_version.seqno_watermark = gc_watermark;

        self.push_old_version(old_version);

        Ok(())
    }

    fn push_old_version(&mut self, version: Version) {
        self.version_free_list.push_back(version);
    }

    pub fn version_free_list_len(&self) -> usize {
        self.version_free_list.len()
    }

    pub fn hidden_set(&self) -> &HiddenSet {
        &self.hidden_set
    }

    pub fn hidden_set_mut(&mut self) -> &mut HiddenSet {
        &mut self.hidden_set
    }

    pub(crate) fn maintenance(&mut self, gc_watermark: SeqNo) -> crate::Result<()> {
        log::trace!("Running manifest GC with watermark={gc_watermark}");

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

        log::trace!(
            "Manifest GC done, manifest length now {}",
            self.version_free_list_len(),
        );

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::AbstractTree;
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

        let table_count_before_major_compact = tree.segment_count();

        let crate::AnyTree::Standard(tree) = tree else {
            unreachable!();
        };

        {
            // NOTE: Purposefully change level manifest to have invalid path
            // to force an I/O error
            tree.compaction_state
                .lock()
                .expect("lock is poisoned")
                .folder = "/invaliiid/asd".into();
        }

        assert!(tree.major_compact(u64::MAX, 4).is_err());

        assert!(tree
            .compaction_state
            .lock()
            .expect("lock is poisoned")
            .hidden_set()
            .is_empty());

        assert_eq!(table_count_before_major_compact, tree.segment_count());

        Ok(())
    }
}
