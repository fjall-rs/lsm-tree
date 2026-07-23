// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

// This implementation was heavily inspired by:
//  * https://github.com/andy-kimball/arenaskl/tree/f7010085
//  * https://github.com/crossbeam-rs/crossbeam/tree/983d56b6/crossbeam-skiplist

//! This mod is a purpose-built concurrent skiplist intended for use
//! by the memtable.
//!
//! Due to the requirements of memtable, there are a number of notable in the
//! features it lacks:
//! - Updates
//! - Deletes
//! - Overwrites
//!
//! The main reasons for its existence are that it
//! - provides concurrent reads and inserts, and
//! - batches memory allocations
//!
//! Prior to this implementation, `crossbeam_skiplist` was used.

mod arena;
mod skipmap;

pub use skipmap::SkipMap;

#[cfg(test)]
mod test;
