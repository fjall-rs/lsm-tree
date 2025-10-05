// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod blob_file;
mod gc;
mod handle;
mod index;
// mod manifest;
mod accessor;

#[doc(hidden)]
pub mod scanner;

mod value_log;

pub use {
    accessor::Accessor,
    blob_file::multi_writer::MultiWriter as BlobFileWriter,
    gc::report::GcReport,
    gc::{GcStrategy, SpaceAmpStrategy, StaleThresholdStrategy},
    handle::ValueHandle,
    index::{Reader as IndexReader, Writer as IndexWriter},
    value_log::ValueLog,
};

#[doc(hidden)]
pub use blob_file::{reader::Reader as BlobFileReader, BlobFile};

use crate::vlog::blob_file::{trailer::Trailer, GcStats, Inner as BlobFileInner};
use std::{path::Path, sync::Arc};

pub fn recover_blob_files(folder: &Path, ids: &[BlobFileId]) -> crate::Result<Vec<BlobFile>> {
    let cnt = ids.len();

    let progress_mod = match cnt {
        _ if cnt <= 20 => 1,
        _ if cnt <= 100 => 10,
        _ => 100,
    };

    log::debug!("Recovering {cnt} blob files from {:?}", folder.display(),);

    // TODO:
    // Self::remove_unfinished_blob_files(&folder, &ids)?;

    let mut blob_files = Vec::with_capacity(ids.len());

    for (idx, &id) in ids.iter().enumerate() {
        log::trace!("Recovering blob file #{id:?}");

        let path = folder.join(id.to_string());
        let trailer = Trailer::from_file(&path)?;

        blob_files.push(BlobFile(Arc::new(BlobFileInner {
            id,
            path,
            meta: trailer.metadata,
            gc_stats: GcStats::default(),
        })));

        if idx % progress_mod == 0 {
            log::debug!("Recovered {idx}/{cnt} blob files");
        }
    }

    if blob_files.len() < ids.len() {
        return Err(crate::Error::Unrecoverable);
    }

    log::debug!("Successfully recovered {} blob files", blob_files.len());

    Ok(blob_files)
}

/// The unique identifier for a value log blob file.
pub type BlobFileId = u64;
