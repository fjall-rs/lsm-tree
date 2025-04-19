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
//! Data is then flushed to disk segments, as the write buffer reaches some threshold.
//!
//! Amassing many segments on disk will degrade read performance and waste disk space usage, so segments
//! can be periodically merged into larger segments in a process called `Compaction`.
//! Different compaction strategies have different advantages and drawbacks, and should be chosen based
//! on the workload characteristics.
//!
//! Because maintaining an efficient structure is deferred to the compaction process, writing to an LSMT
//! is very fast (O(1) complexity).
//!
//! Keys are limited to 65536 bytes, values are limited to 2^32 bytes. As is normal with any kind of storage
//! engine, larger keys and values have a bigger performance impact.
//!
//! # Example usage
//!
//! ```
//! use lsm_tree::{AbstractTree, Config, Tree};
//! #
//! # let folder = tempfile::tempdir()?;
//!
//! // A tree is a single physical keyspace/index/...
//! // and supports a BTreeMap-like API
//! let tree = Config::new(folder).open()?;
//!
//! // Note compared to the BTreeMap API, operations return a Result<T>
//! // So you can handle I/O errors if they occur
//! tree.insert("my_key", "my_value", /* sequence number */ 0);
//!
//! let item = tree.get("my_key", None)?;
//! assert_eq!(Some("my_value".as_bytes().into()), item);
//!
//! // Search by prefix
//! for item in tree.prefix("prefix", None, None) {
//!   // ...
//! }
//!
//! // Search by range
//! for item in tree.range("a"..="z", None, None) {
//!   // ...
//! }
//!
//! // Iterators implement DoubleEndedIterator, so you can search backwards, too!
//! for item in tree.prefix("prefix", None, None).rev() {
//!   // ...
//! }
//!
//! // Flush to secondary storage, clearing the memtable
//! // and persisting all in-memory data.
//! // Note, this flushes synchronously, which may not be desired
//! tree.flush_active_memtable(0)?;
//! assert_eq!(Some("my_value".as_bytes().into()), item);
//!
//! // When some disk segments have amassed, use compaction
//! // to reduce the amount of disk segments
//!
//! // Choose compaction strategy based on workload
//! use lsm_tree::compaction::Leveled;
//! # use std::sync::Arc;
//!
//! let strategy = Leveled::default();
//!
//! let version_gc_threshold = 0;
//! tree.compact(Arc::new(strategy), version_gc_threshold)?;
//!
//! assert_eq!(Some("my_value".as_bytes().into()), item);
//! #
//! # Ok::<(), lsm_tree::Error>(())
//! ```

#![doc(html_logo_url = "https://raw.githubusercontent.com/fjall-rs/lsm-tree/main/logo.png")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/fjall-rs/lsm-tree/main/logo.png")]
#![warn(unsafe_code)]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]
#![warn(clippy::multiple_crate_versions)]
#![allow(clippy::option_if_let_else)]
#![warn(clippy::needless_lifetimes)]

pub(crate) type HashMap<K, V> = std::collections::HashMap<K, V, xxhash_rust::xxh3::Xxh3Builder>;
pub(crate) type HashSet<K> = std::collections::HashSet<K, xxhash_rust::xxh3::Xxh3Builder>;

#[allow(unused)]
macro_rules! set {
    ($($x:expr),+ $(,)?) => {
        [$($x),+].into_iter().collect::<HashSet<_>>()
    }
}

macro_rules! fail_iter {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        }
    };
}

mod any_tree;

mod r#abstract;

#[doc(hidden)]
pub mod binary_search;

#[doc(hidden)]
pub mod blob_tree;

mod clipping_iter;
pub mod compaction;
mod compression;
mod config;

mod error;
// mod export;

#[doc(hidden)]
pub mod file;

mod key;

#[doc(hidden)]
pub mod level_manifest;

mod level_reader;
mod level_scanner;

mod manifest;
mod memtable;

#[doc(hidden)]
mod new_cache;

#[doc(hidden)]
mod new_descriptor_table;

#[doc(hidden)]
pub mod merge;

mod multi_reader;

#[doc(hidden)]
pub mod mvcc_stream;

mod path;

#[doc(hidden)]
pub mod range;

mod seqno;
mod snapshot;
mod windows;

#[doc(hidden)]
pub mod stop_signal;

mod time;
mod tree;
mod value;
mod version;

#[doc(hidden)]
pub mod super_segment;

/// KV-tuple, typically returned by an iterator
pub type KvPair = (UserKey, UserValue);

#[doc(hidden)]
pub use value_log::KeyRange;

#[doc(hidden)]
pub mod coding {
    pub use value_log::coding::{Decode, DecodeError, Encode, EncodeError};
}

#[doc(hidden)]
pub use {
    merge::BoxedIterator,
    new_cache::NewCache,
    new_descriptor_table::NewDescriptorTable,
    super_segment::{block::Checksum, GlobalSegmentId, SegmentId},
    tree::inner::TreeId,
    value::InternalValue,
};

pub use {
    coding::{DecodeError, EncodeError},
    compression::CompressionType,
    config::{Config, TreeType},
    error::{Error, Result},
    memtable::Memtable,
    new_cache::NewCache as Cache, // <- TODO: rename
    new_descriptor_table::NewDescriptorTable as DescriptorTable,
    r#abstract::AbstractTree,
    seqno::SequenceNumberCounter,
    snapshot::Snapshot,
    tree::Tree,
    value::{SeqNo, UserKey, UserValue, ValueType},
    version::Version,
};

pub use any_tree::AnyTree;

pub use blob_tree::BlobTree;

pub use value_log::{BlobCache, Slice};

/// Blob garbage collection utilities
pub mod gc {
    pub use value_log::{
        GcReport as Report, GcStrategy as Strategy, SpaceAmpStrategy, StaleThresholdStrategy,
    };
}
