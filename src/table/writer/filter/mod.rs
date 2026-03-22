// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod full;
mod partitioned;

pub use full::FullFilterWriter;
pub use partitioned::PartitionedFilterWriter;

use crate::{
    checksum::ChecksummedWriter, config::BloomConstructionPolicy, encryption::EncryptionProvider,
    prefix::PrefixExtractor, CompressionType, UserKey,
};
use std::{fs::File, io::BufWriter, sync::Arc};

// All methods are required (no defaults) by design so that implementations must
// explicitly handle configuration changes (e.g., filter policies, prefix extractors).
pub trait FilterWriter<W: std::io::Write> {
    // NOTE: We purposefully use a UserKey instead of &[u8]
    // so we can clone it without heap allocation, if needed
    /// Registers a key in the block index.
    fn register_key(&mut self, key: &UserKey) -> crate::Result<()>;

    /// Writes the filter to a file.
    ///
    /// Returns the number of filter blocks written (always 1 in case of full filter block).
    fn finish(
        self: Box<Self>,
        file_writer: &mut sfa::Writer<ChecksummedWriter<BufWriter<File>>>,
    ) -> crate::Result<usize>;

    fn set_filter_policy(
        self: Box<Self>,
        policy: BloomConstructionPolicy,
    ) -> Box<dyn FilterWriter<W>>;

    fn use_tli_compression(
        self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn FilterWriter<W>>;

    fn use_partition_size(self: Box<Self>, size: u32) -> Box<dyn FilterWriter<W>>;

    fn set_prefix_extractor(
        self: Box<Self>,
        extractor: Option<Arc<dyn PrefixExtractor>>,
    ) -> Box<dyn FilterWriter<W>>;

    /// Sets the encryption provider for filter blocks.
    fn use_encryption(
        self: Box<Self>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> Box<dyn FilterWriter<W>>;
}
