// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{super_segment::BlockOffset, SeqNo, UserKey};

pub struct Metadata {
    /// Written data block count
    pub data_block_count: usize,

    /// Written item count
    pub item_count: usize,

    /// Tombstone count
    pub tombstone_count: usize,

    // TODO: 3.0.0 - https://github.com/fjall-rs/lsm-tree/issues/101
    /// Written key count (unique keys)
    pub key_count: usize,

    /// Current file position of writer
    pub file_pos: BlockOffset,

    /// Only takes user data into account
    pub uncompressed_size: u64,

    /// First encountered key
    pub first_key: Option<UserKey>,

    /// Last encountered key
    pub last_key: Option<UserKey>,

    /// Lowest encountered seqno
    pub lowest_seqno: SeqNo,

    /// Highest encountered seqno
    pub highest_seqno: SeqNo,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            data_block_count: 0,

            item_count: 0,
            tombstone_count: 0,
            key_count: 0,
            file_pos: BlockOffset(0),
            uncompressed_size: 0,

            first_key: None,
            last_key: None,

            lowest_seqno: SeqNo::MAX,
            highest_seqno: 0,
        }
    }
}
