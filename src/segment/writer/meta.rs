use crate::{SeqNo, UserKey};

pub struct Metadata {
    /// Written block count
    pub block_count: usize,

    /// Written item count
    pub item_count: usize,

    /// Tombstone count
    pub tombstone_count: usize,

    /// Written key count (unique keys)
    pub key_count: usize,

    /// Current file position of writer
    pub file_pos: u64,

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
            block_count: 0,
            item_count: 0,
            tombstone_count: 0,
            key_count: 0,
            file_pos: 0,
            uncompressed_size: 0,

            first_key: None,
            last_key: None,

            lowest_seqno: SeqNo::MAX,
            highest_seqno: 0,
        }
    }
}