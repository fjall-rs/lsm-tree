// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Compatibility helpers for loading legacy manifests.

/// Load a v3 manifest into the current in-memory version representation.
pub mod v3 {
    use crate::{
        tree::inner::get_next_tree_id,
        version::{recovery::recover, Version},
        Cache, DescriptorTable, Table,
    };
    use std::{path::Path, sync::Arc};

    pub fn load_manifest<P: AsRef<Path>>(
        folder: P,
        cache: &Arc<Cache>,
        descriptor_table: &Arc<DescriptorTable>,
    ) -> crate::Result<Version> {
        let folder = folder.as_ref();
        let tree_id = get_next_tree_id();

        let recovery = recover(folder)?;

        #[cfg(feature = "metrics")]
        let metrics = Arc::new(crate::Metrics::default());

        let tables = recovery
            .table_ids
            .iter()
            .flat_map(|level| level.iter())
            .flat_map(|run| run.iter())
            .map(|table| {
                Table::recover(
                    folder
                        .join(crate::file::TABLES_FOLDER)
                        .join(table.id.to_string()),
                    table.checksum,
                    table.global_seqno,
                    tree_id,
                    cache.clone(),
                    Some(descriptor_table.clone()),
                    false,
                    false,
                    #[cfg(feature = "metrics")]
                    metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let (blob_files, _) = crate::vlog::recover_blob_files(
            &folder.join(crate::file::BLOBS_FOLDER),
            &recovery.blob_file_ids,
            tree_id,
            Some(descriptor_table),
        )?;

        Version::from_recovery(recovery, &tables, &blob_files)
    }
}
