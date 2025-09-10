// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod blob_file;
mod compression; // TODO: remove
mod config;
mod gc;
mod handle;
mod index;
mod manifest;

#[doc(hidden)]
pub mod scanner;

mod value_log;

pub use {
    blob_file::multi_writer::MultiWriter as BlobFileWriter,
    compression::Compressor,
    config::Config,
    gc::report::GcReport,
    gc::{GcStrategy, SpaceAmpStrategy, StaleThresholdStrategy},
    handle::ValueHandle,
    index::{Reader as IndexReader, Writer as IndexWriter},
    value_log::{ValueLog, ValueLogId},
};

#[doc(hidden)]
pub use blob_file::{reader::Reader as BlobFileReader, BlobFile};

/// The unique identifier for a value log blob file.
pub type BlobFileId = u64;
