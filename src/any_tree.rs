// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    blob_tree::ingest::BlobIngestion, tree::ingest::Ingestion, BlobTree, SeqNo, Tree, UserKey,
    UserValue,
};
use enum_dispatch::enum_dispatch;

/// May be a standard [`Tree`] or a [`BlobTree`]
#[derive(Clone)]
#[enum_dispatch(AbstractTree)]
pub enum AnyTree {
    /// Standard LSM-tree, see [`Tree`]
    Standard(Tree),

    /// Key-value separated LSM-tree, see [`BlobTree`]
    Blob(BlobTree),
}

/// Unified ingestion builder over `AnyTree`
// Keep zero allocations and direct dispatch; boxing introduces heap indirection and `dyn` adds virtual dispatch.
// Ingestion calls use `&mut self` in tight loops; the active variant is stable and branch prediction makes the match cheap.
// Allowing this lint preserves hot-path performance at the cost of a larger enum size.
#[allow(clippy::large_enum_variant)]
pub enum AnyIngestion<'a> {
    /// Ingestion for a standard LSM-tree
    Standard(Ingestion<'a>),
    /// Ingestion for a [`BlobTree`] with KV separation
    Blob(BlobIngestion<'a>),
}

impl<'a> AnyIngestion<'a> {
    #[must_use]
    /// Sets the sequence number used for subsequent writes
    pub fn with_seqno(self, seqno: SeqNo) -> Self {
        match self {
            AnyIngestion::Standard(i) => AnyIngestion::Standard(i.with_seqno(seqno)),
            AnyIngestion::Blob(b) => AnyIngestion::Blob(b.with_seqno(seqno)),
        }
    }

    /// Writes a key-value pair
    pub fn write(&mut self, key: UserKey, value: UserValue) -> crate::Result<()> {
        match self {
            AnyIngestion::Standard(i) => i.write(key, value),
            AnyIngestion::Blob(b) => b.write(key, value),
        }
    }

    /// Writes a tombstone for a key
    pub fn write_tombstone(&mut self, key: UserKey) -> crate::Result<()> {
        match self {
            AnyIngestion::Standard(i) => i.write_tombstone(key),
            AnyIngestion::Blob(b) => b.write_tombstone(key),
        }
    }

    /// Finalizes ingestion and registers created tables (and blob files if present)
    pub fn finish(self) -> crate::Result<()> {
        match self {
            AnyIngestion::Standard(i) => i.finish(),
            AnyIngestion::Blob(b) => b.finish(),
        }
    }
}

impl AnyTree {
    /// Starts an ingestion for any tree type (standard or blob)
    pub fn ingestion(&self) -> crate::Result<AnyIngestion<'_>> {
        match self {
            AnyTree::Standard(t) => Ok(AnyIngestion::Standard(Ingestion::new(t)?)),
            AnyTree::Blob(b) => Ok(AnyIngestion::Blob(BlobIngestion::new(b)?)),
        }
    }
}
