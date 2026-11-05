use super::{BlobFileList, BlobFileRef, Level, RawVersionData, Run, Version};
use crate::{
    abstract_tree::AbstractTree,
    file_accessor::FileAccessor,
    table::Table,
    vlog::blob_file::{Inner as BlobFileInner, Metadata},
    BlobFile, Config, DescriptorTable,
};
use std::{
    fs::File,
    sync::{atomic::AtomicBool, Arc},
};

/// Rebuilds an in-memory version from raw manifest data and an opened tree's
/// recovery context.
pub fn hydrate<T: AbstractTree>(raw: &RawVersionData, tree: &T) -> crate::Result<Version> {
    let config = tree.tree_config();

    #[cfg(feature = "metrics")]
    let metrics = tree.metrics().clone();

    let mut tables = crate::HashMap::default();

    for (level_idx, level) in raw.table_ids.iter().enumerate() {
        let pin_filter = config.filter_block_pinning_policy.get(level_idx);
        let pin_index = config.index_block_pinning_policy.get(level_idx);

        for run in level {
            for table_ref in run {
                let table = Table::recover(
                    config.table_file_path(table_ref.id()),
                    table_ref.checksum(),
                    table_ref.global_seqno(),
                    tree.id(),
                    config.cache.clone(),
                    config.descriptor_table.clone(),
                    pin_filter,
                    pin_index,
                    #[cfg(feature = "metrics")]
                    metrics.clone(),
                )?;

                if tables.insert(table_ref.id(), table).is_some() {
                    return Err(crate::Error::Unrecoverable);
                }
            }
        }
    }

    let levels = raw
        .table_ids
        .iter()
        .map(|level| {
            let runs = level
                .iter()
                .map(|run| {
                    let tables = run
                        .iter()
                        .map(|table_ref| {
                            tables
                                .get(&table_ref.id())
                                .cloned()
                                .ok_or(crate::Error::Unrecoverable)
                        })
                        .collect::<crate::Result<Vec<_>>>()?;

                    let run = Run::new(tables).ok_or(crate::Error::Unrecoverable)?;
                    Ok(Arc::new(run))
                })
                .collect::<crate::Result<Vec<_>>>()?;

            Ok(Level::from_runs(runs))
        })
        .collect::<crate::Result<Vec<_>>>()?;

    let blob_files = recover_blob_files(
        config,
        &raw.blob_file_ids,
        tree.id(),
        config.descriptor_table.as_ref(),
    )?;

    let blob_files = BlobFileList::new(
        blob_files
            .into_iter()
            .map(|blob_file| (blob_file.id(), blob_file))
            .collect(),
    );

    Ok(Version::from_levels(
        raw.version_id,
        raw.tree_type,
        levels,
        blob_files,
        raw.gc_stats.clone(),
        raw.version_seqno,
        raw.flush_watermark,
    ))
}

fn recover_blob_files(
    config: &Config,
    refs: &[BlobFileRef],
    tree_id: crate::TreeId,
    descriptor_table: Option<&Arc<DescriptorTable>>,
) -> crate::Result<Vec<BlobFile>> {
    let mut blob_files = Vec::with_capacity(refs.len());

    for blob_ref in refs {
        let path = config.blob_file_path(blob_ref.id());
        let file = File::open(&path)?;

        let metadata = {
            let reader = sfa::Reader::new(&path)?;
            let toc = reader.toc();
            let metadata_section = toc.section(b"meta").ok_or(crate::Error::Unrecoverable)?;

            #[expect(
                clippy::cast_possible_truncation,
                reason = "metadata sections are bounded by addressable memory"
            )]
            let metadata_len = metadata_section.len() as usize;

            let metadata_slice =
                crate::file::read_exact(&file, metadata_section.pos(), metadata_len)?;

            Metadata::from_slice(&metadata_slice)?
        };

        let file_accessor = if let Some(descriptor_table) = descriptor_table.cloned() {
            FileAccessor::DescriptorTable(descriptor_table)
        } else {
            FileAccessor::File(Arc::new(file))
        };

        blob_files.push(BlobFile(Arc::new(BlobFileInner {
            id: blob_ref.id(),
            tree_id,
            path,
            meta: metadata,
            is_deleted: AtomicBool::new(false),
            checksum: blob_ref.checksum(),
            file_accessor,
        })));
    }

    Ok(blob_files)
}
