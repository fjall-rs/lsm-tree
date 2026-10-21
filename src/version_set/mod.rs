// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[doc(hidden)]
mod mem;

pub use mem::MemoryVersionSetProvider;

use crate::{version::SuperVersion, vlog::BlobFileId, SeqNo, TableId};

/// TODO: WIP
#[derive(Debug)]
pub enum EditRecord {
    AddTables {
        level: usize,
        tables: Vec<TableId>,
    },
    DropTables(Vec<TableId>),
    MoveTables {
        dst_lvl: usize,
        tables: Vec<TableId>,
    },
    DropBlobFiles(Vec<BlobFileId>),
}

#[derive(Debug)]
pub struct VersionEdit(Vec<EditRecord>);

impl VersionEdit {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn empty() -> Self {
        Self(Vec::default())
    }

    pub fn new(recs: Vec<EditRecord>) -> Self {
        Self(recs)
    }

    pub fn iter(&self) -> impl Iterator<Item = &EditRecord> {
        self.0.iter()
    }
}

/// Provides access to a history of super versions
pub trait VersionSetProvider {
    /// Returns the number of versions in the version set.
    fn version_history_len(&self) -> usize;

    /// Gets the latest super version.
    fn latest_version(&self) -> SuperVersion;

    /// Gets a super version visible to the given snapshot.
    fn get_version_for_seqno(&self, seqno: SeqNo) -> SuperVersion;
}
