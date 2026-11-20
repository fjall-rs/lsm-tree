// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod io;
mod mem;

pub use io::{IoSuperVersionSetProvider, Persister};
pub use mem::MemorySuperVersionSetProvider;

use crate::{version::SuperVersion, SeqNo};

/// Provides access to a history of [`SuperVersions`](`SuperVersion`)
pub trait SuperVersionSetProvider {
    /// Returns the number of [`SuperVersions`](`SuperVersion`) in the version set.
    fn version_history_len(&self) -> usize;

    /// Gets the latest [`SuperVersion`].
    fn latest_version(&self) -> SuperVersion;

    /// Gets a [`SuperVersion`] visible to the given snapshot seqno.
    fn get_version_for_seqno(&self, seqno: SeqNo) -> SuperVersion;
}
