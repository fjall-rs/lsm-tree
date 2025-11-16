// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod full;
mod partitioned;

pub use full::FullFilterWriter;
pub use partitioned::PartitionedFilterWriter;

use crate::{config::BloomConstructionPolicy, CompressionType, UserKey};

pub trait FilterWriter<W: std::io::Write> {
    // NOTE: We purposefully use a UserKey instead of &[u8]
    // so we can clone it without heap allocation, if needed
    /// Registers a key in the block index.
    fn register_key(&mut self, key: &UserKey) -> crate::Result<()>;

    /// Writes the filter to a file.
    ///
    /// Returns the number of filter blocks written (always 1 in case of full filter block).
    fn finish(self: Box<Self>, file_writer: &mut sfa::Writer) -> crate::Result<usize>;

    fn set_filter_policy(
        self: Box<Self>,
        policy: BloomConstructionPolicy,
    ) -> Box<dyn FilterWriter<W>>;

    fn use_tli_compression(
        self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn FilterWriter<W>>;
}
