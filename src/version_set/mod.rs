// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[doc(hidden)]
mod mem;

#[doc(hidden)]
mod io;

pub use io::{IoVersionSetProvider, Persister};
pub use mem::MemoryVersionSetProvider;

use crate::{blob_tree::FragmentationMap, version::SuperVersion, vlog::BlobFileId, SeqNo, TableId};

/// TODO: WIP
#[derive(Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub enum EditRecord {
    /// Some tables were added to a level
    AddTables {
        level: usize,
        tables: Vec<TableId>,
    },

    /// Some blob files were created
    AddBlobFiles {
        blob_files: Vec<BlobFileId>,
    },

    // TODO: add lvl param
    /// Some tables were dropped
    DropTables(Vec<TableId>),

    /// Some tables were moved
    MoveTables {
        dst_lvl: usize,
        tables: Vec<TableId>,
    },

    /// Some blob files were dropped
    DropBlobFiles(Vec<BlobFileId>),

    /// The version seqno was set
    SetVersionSeqno(SeqNo),

    UpdateFragMap(FragmentationMap),

    Custom(Vec<u8>),
}

#[derive(Debug, Eq, PartialEq, Default)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub struct VersionEdit(Vec<EditRecord>);

impl VersionEdit {
    pub fn empty() -> Self {
        Self(Vec::default())
    }

    pub fn new(recs: Vec<EditRecord>) -> Self {
        Self(recs)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
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
