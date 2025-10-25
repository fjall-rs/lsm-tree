// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod full;
// mod partitioned;

pub use full::FullFilterWriter;
// pub use partitioned::PartitionedFilterWriter;

use crate::config::BloomConstructionPolicy;

pub trait FilterWriter<W: std::io::Write> {
    /// Registers a key in the block index.
    fn register_key(&mut self, key: &[u8]) -> crate::Result<()>;

    /// Writes the filter to a file.
    fn finish(
        self: Box<Self>,
        file_writer: &mut sfa::Writer,
        bloom_policy: BloomConstructionPolicy,
    ) -> crate::Result<()>;
}
