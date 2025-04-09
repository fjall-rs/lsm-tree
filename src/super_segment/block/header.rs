use crate::{segment::block::offset::BlockOffset, Checksum};

/// Header of a disk-based block
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Header {
    /// Checksum value to verify integrity of data
    pub checksum: Checksum,

    /// File offset of previous block - only used for data blocks
    pub previous_block_offset: BlockOffset,

    /// On-disk size of data segment
    pub data_length: u32,
}
