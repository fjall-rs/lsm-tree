// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    memtable::Memtable,
    tree::{inner::MemtableId, sealed::SealedMemtables},
    version::{persist_version, Version},
    SeqNo, SequenceNumberCounter,
};
use std::{collections::VecDeque, path::Path, sync::Arc};

/// A super version is a point-in-time snapshot of memtables and a [`Version`] (list of disk files)
#[derive(Clone)]
pub struct SuperVersion {
    /// Active memtable that is being written to
    pub(crate) active_memtable: Arc<Memtable>,

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
                active_memtable: Arc::default(),
                sealed_memtables: Arc::default(),
                version,
                seqno: 0,
            }]
            .into(),
        )
    }

    pub fn free_list_len(&self) -> usize {
        self.0.len().saturating_sub(1)
    }

    pub(crate) fn maintenance(&mut self, folder: &Path, gc_watermark: SeqNo) -> crate::Result<()> {
        log::trace!("Running manifest GC with watermark={gc_watermark}");

        loop {
            if self.free_list_len() == 0 {
                break;
            }

            let Some(head) = self.0.front() else {
                break;
            };

            if head.seqno < gc_watermark {
                let path = folder.join(format!("v{}", head.version.id()));
                if path.try_exists()? {
                    std::fs::remove_file(path)?;
                }
                self.0.pop_front();
            } else {
                break;
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
    ) -> crate::Result<()> {
        // NOTE: Copy-on-write...
        //
        // Create a copy of the levels we can operate on
        // without mutating the current level manifest
        // If persisting to disk fails, this way the level manifest
        // is unchanged
        let mut next_version = f(&self.latest_version())?;
        next_version.seqno = seqno.next();
        log::trace!("Next version seqno={}", next_version.seqno);

        persist_version(tree_path, &next_version.version)?;
        self.append_version(next_version);

        Ok(())
    }

    pub fn append_version(&mut self, version: SuperVersion) {
        self.0.push_back(version);
    }

    pub fn latest_version(&self) -> SuperVersion {
        self.0
            .iter()
            .last()
            .cloned()
            .expect("should always have a SuperVersion")
    }

    pub fn get_version_for_snapshot(&self, seqno: SeqNo) -> SuperVersion {
        if seqno == 0 {
            return self
                .0
                .front()
                .cloned()
                .expect("should always find a SuperVersion");
        }

        self.0
            .iter()
            .rev()
            .find(|version| version.seqno < seqno)
            .cloned()
            .expect("should always find a SuperVersion")
    }

    pub fn append_sealed_memtable(&mut self, id: MemtableId, memtable: Arc<Memtable>) {
        let mut copy = self.latest_version();
        copy.sealed_memtables = Arc::new(copy.sealed_memtables.add(id, memtable));
        self.0.push_back(copy);
    }
}
