// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod accessor;
pub mod blob_file;
mod handle;

pub use {
    accessor::Accessor,
    blob_file::{
        merge::MergeScanner as BlobFileMergeScanner, multi_writer::MultiWriter as BlobFileWriter,
        BlobFile, BlobFileScanner,
    },
    handle::ValueHandle,
};

/// The unique identifier for a value log blob file.
pub type BlobFileId = u64;
