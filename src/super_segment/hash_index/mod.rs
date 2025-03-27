mod builder;
mod reader;

use xxhash_rust::xxh3::xxh3_64;

const MARKER_FREE: u8 = u8::MAX - 1;
const MARKER_CONFLICT: u8 = u8::MAX;

// NOTE: We know the hash index has a bucket count <= u8
#[allow(clippy::cast_possible_truncation)]
fn calculate_bucket_position(key: &[u8], bucket_count: u32) -> usize {
    let hash = xxh3_64(key);
    (hash % u64::from(bucket_count)) as usize
}

pub use builder::{Builder, MAX_POINTERS_FOR_HASH_INDEX};
pub use reader::Reader;
