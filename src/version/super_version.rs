// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    memtable::Memtable,
    tree::sealed::SealedMemtables,
    version::{persist_version, Version},
    SeqNo, SequenceNumberCounter,
};
use std::{collections::VecDeque, path::Path, sync::Arc};

/// A super version is a point-in-time snapshot of memtables and a [`Version`] (list of disk files)
#[derive(Clone)]
pub struct SuperVersion {
    /// Active memtable that is being written to
    #[doc(hidden)]
    pub active_memtable: Arc<Memtable>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<SealedMemtables>,

    /// Current tree version
    pub(crate) version: Version,

    pub(crate) seqno: SeqNo,
}

pub struct SuperVersions(VecDeque<SuperVersion>);

impl SuperVersions {
    pub fn new(version: Version) -> Self {
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
                set.entry(sealed.id())
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

    pub fn maintenance(&mut self, folder: &Path, gc_watermark: SeqNo) -> crate::Result<()> {
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
                if path.try_exists()? {
                    std::fs::remove_file(path)?;
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
    pub(crate) fn upgrade_version<F: FnOnce(&SuperVersion) -> crate::Result<SuperVersion>>(
        &mut self,
        tree_path: &Path,
        f: F,
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
        F: FnOnce(&SuperVersion) -> crate::Result<SuperVersion>,
    >(
        &mut self,
        tree_path: &Path,
        f: F,
        seqno: SeqNo,
        visible_seqno: &SequenceNumberCounter,
    ) -> crate::Result<()> {
        let mut next_version = f(&self.latest_version())?;
        next_version.seqno = seqno;
        log::trace!("Next version seqno={}", next_version.seqno);

        persist_version(tree_path, &next_version.version)?;
        self.append_version(next_version);

        visible_seqno.fetch_max(seqno + 1);

        Ok(())
    }

    pub fn append_version(&mut self, version: SuperVersion) {
        self.0.push_back(version);
    }

    pub fn replace_latest_version(&mut self, version: SuperVersion) {
        if self.0.pop_back().is_some() {
            self.0.push_back(version);
        }
    }

    pub fn latest_version(&self) -> SuperVersion {
        #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
        self.0
            .iter()
            .last()
            .cloned()
            .expect("should always have a SuperVersion")
    }

    pub fn get_version_for_snapshot(&self, seqno: SeqNo) -> SuperVersion {
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
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 0)?;

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
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

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
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 8,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

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
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 8,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

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
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

        assert_eq!(history.len(), 1);

        Ok(())
    }
}
