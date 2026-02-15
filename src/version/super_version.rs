// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    fs::FileSystem,
    memtable::Memtable,
    tree::sealed::SealedMemtables,
    version::{persist_version, Version},
    SeqNo, SequenceNumberCounter,
};
use std::{collections::VecDeque, path::Path, sync::Arc};

/// A super version is a point-in-time snapshot of memtables and a [`Version`] (list of disk files)
pub struct SuperVersion<F: FileSystem> {
    /// Active memtable that is being written to
    #[doc(hidden)]
    pub active_memtable: Arc<Memtable>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<SealedMemtables>,

    /// Current tree version
    pub(crate) version: Version<F>,

    pub(crate) seqno: SeqNo,
}

impl<F: FileSystem> Clone for SuperVersion<F> {
    fn clone(&self) -> Self {
        Self {
            active_memtable: self.active_memtable.clone(),
            sealed_memtables: self.sealed_memtables.clone(),
            version: self.version.clone(),
            seqno: self.seqno,
        }
    }
}

pub struct SuperVersions<F: FileSystem>(VecDeque<SuperVersion<F>>);

impl<F: FileSystem> SuperVersions<F> {
    pub fn new(version: Version<F>) -> Self {
        Self(
            vec![SuperVersion {
                active_memtable: Arc::new(Memtable::new(0)),
                sealed_memtables: Arc::default(),
                version,
                seqno: 0,
            }]
            .into(),
        )
    }

    pub fn memtable_size_sum(&self) -> u64 {
        let mut set = crate::HashMap::default();

        for super_version in &self.0 {
            set.entry(super_version.active_memtable.id)
                .and_modify(|bytes| *bytes += super_version.active_memtable.size())
                .or_insert_with(|| super_version.active_memtable.size());

            for sealed in super_version.sealed_memtables.iter() {
                set.entry(sealed.id)
                    .and_modify(|bytes| *bytes += sealed.size())
                    .or_insert_with(|| sealed.size());
            }
        }

        set.into_values().sum()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn free_list_len(&self) -> usize {
        self.len().saturating_sub(1)
    }

    pub fn maintenance<FS: FileSystem>(
        &mut self,
        folder: &Path,
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        if gc_watermark == 0 {
            return Ok(());
        }

        if self.free_list_len() < 1 {
            return Ok(());
        }

        log::trace!("Running manifest GC with watermark={gc_watermark}");

        if let Some(hi_idx) = self.0.iter().rposition(|x| x.seqno < gc_watermark) {
            for _ in 0..hi_idx {
                let Some(head) = self.0.front() else {
                    break;
                };

                log::trace!(
                    "Removing version #{} (seqno={})",
                    head.version.id(),
                    head.seqno,
                );

                let path = folder.join(format!("v{}", head.version.id()));
                if FS::exists(&path)? {
                    FS::remove_file(&path)?;
                }

                self.0.pop_front();
            }
        }

        log::trace!("Manifest GC done, version length now {}", self.0.len());

        Ok(())
    }

    /// Modifies the level manifest atomically.
    ///
    /// The function accepts a transition function that receives the current version
    /// and returns a new version.
    ///
    /// The function takes care of persisting the version changes on disk.
    pub(crate) fn upgrade_version<T: FnOnce(&SuperVersion<F>) -> crate::Result<SuperVersion<F>>>(
        &mut self,
        tree_path: &Path,
        f: T,
        seqno: &SequenceNumberCounter,
        visible_seqno: &SequenceNumberCounter,
    ) -> crate::Result<()> {
        self.upgrade_version_with_seqno(tree_path, f, seqno.next(), visible_seqno)
    }

    /// Like `upgrade_version`, but takes an already-allocated sequence number.
    ///
    /// This is useful when the seqno must be coordinated with other operations
    /// (e.g., bulk ingestion where tables are recovered with the same seqno).
    pub(crate) fn upgrade_version_with_seqno<
        T: FnOnce(&SuperVersion<F>) -> crate::Result<SuperVersion<F>>,
    >(
        &mut self,
        tree_path: &Path,
        f: T,
        seqno: SeqNo,
        visible_seqno: &SequenceNumberCounter,
    ) -> crate::Result<()> {
        let mut next_version = f(&self.latest_version())?;
        next_version.seqno = seqno;
        log::trace!("Next version seqno={}", next_version.seqno);

        persist_version::<F>(tree_path, &next_version.version)?;
        self.append_version(next_version);

        visible_seqno.fetch_max(seqno + 1);

        Ok(())
    }

    pub fn append_version(&mut self, version: SuperVersion<F>) {
        self.0.push_back(version);
    }

    pub fn replace_latest_version(&mut self, version: SuperVersion<F>) {
        if self.0.pop_back().is_some() {
            self.0.push_back(version);
        }
    }

    pub fn latest_version(&self) -> SuperVersion<F> {
        #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
        self.0
            .iter()
            .last()
            .cloned()
            .expect("should always have a SuperVersion")
    }

    pub fn get_version_for_snapshot(&self, seqno: SeqNo) -> SuperVersion<F> {
        if seqno == 0 {
            #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
            return self
                .0
                .front()
                .cloned()
                .expect("should always find a SuperVersion");
        }

        let version = self
            .0
            .iter()
            .rev()
            .find(|version| version.seqno < seqno)
            .cloned();

        if version.is_none() {
            log::error!("Failed to find a SuperVersion for snapshot with seqno={seqno}");
            log::error!("SuperVersions:");

            for version in self.0.iter().rev() {
                log::error!("-> {}, seqno={}", version.version.id(), version.seqno);
            }
        }

        #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
        version.expect("should always find a SuperVersion")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn super_version_gc_above_watermark() -> crate::Result<()> {
        let mut history = SuperVersions(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance::<crate::fs::StdFileSystem>(Path::new("."), 0)?;

        assert_eq!(history.free_list_len(), 2);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_simple() -> crate::Result<()> {
        let mut history = SuperVersions(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance::<crate::fs::StdFileSystem>(Path::new("."), 3)?;

        assert_eq!(history.len(), 1);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_simple_2() -> crate::Result<()> {
        let mut history = SuperVersions(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 8,
                },
            ]
            .into(),
        );

        history.maintenance::<crate::fs::StdFileSystem>(Path::new("."), 3)?;

        assert_eq!(history.len(), 2);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_keep() -> crate::Result<()> {
        let mut history = SuperVersions(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 8,
                },
            ]
            .into(),
        );

        history.maintenance::<crate::fs::StdFileSystem>(Path::new("."), 3)?;

        assert_eq!(history.len(), 2);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_shadowed() -> crate::Result<()> {
        let mut history = SuperVersions(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::<crate::fs::StdFileSystem>::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance::<crate::fs::StdFileSystem>(Path::new("."), 3)?;

        assert_eq!(history.len(), 1);

        Ok(())
    }
}
