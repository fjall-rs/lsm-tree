// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod io;
mod mem;

pub use io::{IoVersionSetProvider, Persister};
pub use mem::MemoryVersionSetProvider;

use crate::{version::SuperVersion, SeqNo};

/// Provides access to a history of super versions
pub trait VersionSetProvider {
    /// Returns the number of versions in the version set.
    fn version_history_len(&self) -> usize;

    /// Gets the latest super version.
    fn latest_version(&self) -> SuperVersion;

    /// Gets a super version visible to the given snapshot.
    fn get_version_for_seqno(&self, seqno: SeqNo) -> SuperVersion;
}
