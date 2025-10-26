// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod full;
mod partitioned;

pub use full::FullIndexWriter;
pub use partitioned::PartitionedIndexWriter;

use crate::{table::index_block::KeyedBlockHandle, CompressionType};

pub trait BlockIndexWriter<W: std::io::Write> {
    /// Registers a data block in the block index.
    fn register_data_block(&mut self, block_handle: KeyedBlockHandle) -> crate::Result<()>;

    /// Writes the block index to a file.
    ///
    /// Returns the number of index blocks written.
    fn finish(self: Box<Self>, file_writer: &mut sfa::Writer) -> crate::Result<usize>;

    fn use_compression(
        self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn BlockIndexWriter<W>>;
}
