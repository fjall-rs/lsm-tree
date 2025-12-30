// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    blob_tree::ingest::BlobIngestion, tree::ingest::Ingestion, AnyTree, UserKey, UserValue,
};

/// Unified ingestion builder over `AnyTree`
// Keep zero allocations and direct dispatch; boxing introduces heap indirection and `dyn` adds virtual dispatch.
// Ingestion calls use `&mut self` in tight loops; the active variant is stable and branch prediction makes the match cheap.
// Allowing this lint preserves hot-path performance at the cost of a larger enum size.
#[expect(clippy::large_enum_variant)]
pub enum AnyIngestion<'a> {
    /// Ingestion for a standard LSM-tree
    Standard(Ingestion<'a>),

    /// Ingestion for a [`BlobTree`] with KV separation
    Blob(BlobIngestion<'a>),
}

impl AnyIngestion<'_> {
    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        key: K,
        value: V,
    ) -> crate::Result<()> {
        match self {
            Self::Standard(i) => i.write(key.into(), value.into()),
            Self::Blob(b) => b.write(key.into(), value.into()),
        }
    }

    /// Writes a tombstone for a key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_tombstone<K: Into<UserKey>>(&mut self, key: K) -> crate::Result<()> {
        match self {
            Self::Standard(i) => i.write_tombstone(key.into()),
            Self::Blob(b) => b.write_tombstone(key.into()),
        }
    }

    /// Writes a weak tombstone for a key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Config;
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// #
    /// let mut ingestion = tree.ingestion()?;
    /// ingestion.write("a", "abc")?;
    /// ingestion.write_weak_tombstone("b")?;
    /// ingestion.finish()?;
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_weak_tombstone<K: Into<UserKey>>(&mut self, key: K) -> crate::Result<()> {
        match self {
            Self::Standard(i) => i.write_weak_tombstone(key.into()),
            Self::Blob(b) => b.write_weak_tombstone(key.into()),
        }
    }

    /// Finalizes ingestion and registers created tables (and blob files if present).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn finish(self) -> crate::Result<()> {
        match self {
            Self::Standard(i) => i.finish(),
            Self::Blob(b) => b.finish(),
        }
    }
}

impl AnyTree {
    /// Starts an ingestion for any tree type (standard or blob).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn ingestion(&self) -> crate::Result<AnyIngestion<'_>> {
        match self {
            Self::Standard(t) => Ok(AnyIngestion::Standard(Ingestion::new(t)?)),
            Self::Blob(b) => Ok(AnyIngestion::Blob(BlobIngestion::new(b)?)),
        }
    }
}
