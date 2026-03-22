// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod full;
mod partitioned;

pub use full::FullIndexWriter;
pub use partitioned::PartitionedIndexWriter;

use crate::{
    checksum::ChecksummedWriter, encryption::EncryptionProvider,
    table::index_block::KeyedBlockHandle, CompressionType,
};
use std::{fs::File, io::BufWriter, sync::Arc};

pub trait BlockIndexWriter<W: std::io::Write> {
    /// Registers a data block in the block index.
    fn register_data_block(&mut self, block_handle: KeyedBlockHandle) -> crate::Result<()>;

    /// Writes the block index to a file.
    ///
    /// Returns the number of index blocks written.
    fn finish(
        self: Box<Self>,
        file_writer: &mut sfa::Writer<ChecksummedWriter<BufWriter<File>>>,
    ) -> crate::Result<usize>;

    fn use_compression(
        self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn BlockIndexWriter<W>>;

    fn use_partition_size(self: Box<Self>, size: u32) -> Box<dyn BlockIndexWriter<W>>;

    /// Sets the encryption provider for index blocks.
    fn use_encryption(
        self: Box<Self>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> Box<dyn BlockIndexWriter<W>>;
}
