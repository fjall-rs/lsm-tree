// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{vlog::BlobFileId, TableId};
use std::path::PathBuf;

/// Resolves the path of an existing table or blob file from its ID.
///
/// Used when opening the files of a recovered version, e.g. when tables of a
/// migrated tree live under a different file name than their ID.
///
/// New files are always written to `<tree path>/tables/<id>` and `<tree path>/blobs/<id>`,
/// so a resolver must return those paths for them.
pub trait FileResolver: Send + Sync {
    /// Returns the path of the table with the given ID.
    fn resolve_table(&self, id: TableId) -> PathBuf;

    /// Returns the path of the blob file with the given ID.
    fn resolve_blob_file(&self, id: BlobFileId) -> PathBuf;
}
