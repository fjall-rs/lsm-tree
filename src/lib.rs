// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).
//!
//! ##### NOTE
//!
//! > This crate only provides a primitive LSM-tree, not a full storage engine.
//! > You probably want to use <https://crates.io/crates/fjall> instead.
//! > For example, it does not ship with a write-ahead log, so writes are not
//! > persisted until manually flushing the memtable.
//!
//! ##### About
//!
//! This crate exports a `Tree` that supports a subset of the `BTreeMap` API.
//!
//! LSM-trees are an alternative to B-trees to persist a sorted list of items (e.g. a database table)
//! on disk and perform fast lookup queries.
//! Instead of updating a disk-based data structure in-place,
//! deltas (inserts and deletes) are added into an in-memory write buffer (`Memtable`).
//! Data is then flushed to disk-resident table files when the write buffer reaches some threshold.
//!
//! Amassing many tables on disk will degrade read performance and waste disk space, so tables
//! can be periodically merged into larger tables in a process called `Compaction`.
//! Different compaction strategies have different advantages and drawbacks, and should be chosen based
//! on the workload characteristics.
//!
//! Because maintaining an efficient structure is deferred to the compaction process, writing to an LSMT
//! is very fast (_O(1)_ complexity).
//!
//! Keys are limited to 65536 bytes, values are limited to 2^32 bytes. As is normal with any kind of storage
//! engine, larger keys and values have a bigger performance impact.

#![doc(html_logo_url = "https://raw.githubusercontent.com/fjall-rs/lsm-tree/main/logo.png")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/fjall-rs/lsm-tree/main/logo.png")]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]
#![warn(clippy::multiple_crate_versions)]
#![allow(clippy::option_if_let_else)]
#![warn(clippy::redundant_feature_names)]
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[doc(hidden)]
pub type HashMap<K, V> = std::collections::HashMap<K, V, rustc_hash::FxBuildHasher>;

pub(crate) type HashSet<K> = std::collections::HashSet<K, rustc_hash::FxBuildHasher>;

macro_rules! fail_iter {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        }
    };
}

macro_rules! unwrap {
    ($x:expr) => {{
        $x.expect("should read")
    }};
}


mod any_tree;

mod r#abstract;

#[doc(hidden)]
pub mod blob_tree;

#[doc(hidden)]
mod cache;

#[doc(hidden)]
pub mod checksum;

#[doc(hidden)]
pub mod coding;

pub mod compaction;
mod compression;

/// Configuration
pub mod config;

#[doc(hidden)]
pub mod descriptor_table;

#[doc(hidden)]
pub mod file_accessor;

mod double_ended_peekable;
mod error;

#[doc(hidden)]
pub mod file;

mod hash;
mod ingestion;
mod iter_guard;
mod key;
mod key_range;
mod manifest;
mod memtable;
mod run_reader;
mod run_scanner;

#[doc(hidden)]
pub mod merge;

#[cfg(feature = "metrics")]
pub(crate) mod metrics;

// mod multi_reader;

#[doc(hidden)]
pub mod mvcc_stream;

mod path;

#[doc(hidden)]
pub mod range;

#[doc(hidden)]
pub mod table;

mod seqno;
mod slice;
mod slice_windows;

#[doc(hidden)]
pub mod stop_signal;

mod format_version;
mod time;
mod tree;

/// Utility functions
pub mod util;

mod value;
mod value_type;
mod version;
mod vlog;

/// User defined key (byte array)
pub type UserKey = Slice;

/// User defined data (byte array)
pub type UserValue = Slice;

/// KV-tuple (key + value)
pub type KvPair = (UserKey, UserValue);

#[doc(hidden)]
pub use {
    blob_tree::{handle::BlobIndirection, Guard as BlobGuard},
    checksum::Checksum,
    iter_guard::IterGuardImpl,
    key_range::KeyRange,
    merge::BoxedIterator,
    slice::Builder,
    table::{GlobalTableId, Table, TableId},
    tree::inner::TreeId,
    tree::Guard as StandardGuard,
    value::InternalValue,
};

pub use {
    any_tree::AnyTree,
    blob_tree::BlobTree,
    cache::Cache,
    compression::CompressionType,
    config::{Config, KvSeparationOptions, TreeType},
    descriptor_table::DescriptorTable,
    error::{Error, Result},
    format_version::FormatVersion,
    ingestion::AnyIngestion,
    iter_guard::IterGuard as Guard,
    memtable::{Memtable, MemtableId},
    r#abstract::AbstractTree,
    seqno::SequenceNumberCounter,
    slice::Slice,
    tree::Tree,
    value::SeqNo,
    value_type::ValueType,
    vlog::BlobFile,
};

#[cfg(feature = "metrics")]
pub use metrics::Metrics;

#[doc(hidden)]
#[must_use]
#[allow(missing_docs, clippy::missing_errors_doc, clippy::unwrap_used)]
pub fn get_tmp_folder() -> tempfile::TempDir {
    if let Ok(p) = std::env::var("LSMT_TMP_FOLDER") {
        tempfile::tempdir_in(p)
    } else {
        tempfile::tempdir()
    }
    .unwrap()
}
