// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! The hash index is a lightweight (typically <=1 byte per KV) index
//! embeddeded into a block to speed up point reads
//!
//! The index is initialized with `hash_ratio * item_count` buckets.
//!
//! Each bucket is initialized as 254 (FREE).
//!
//! During block building, each key is hashed into a bucket.
//! If the bucket is FREE, it is set to the index of the binary index pointer
//! pointing to the item's restart interval.
//!
//! If the given bucket is already < FREE, it is set to CONFLICT.
//!
//! During a point read, `CONFLICT`ed buckets are skipped, and the binary index
//! is consulted instead.

mod builder;
mod reader;

pub use builder::{Builder, MAX_POINTERS_FOR_HASH_INDEX};
pub use reader::Reader;

pub(crate) const MARKER_FREE: u8 = u8::MAX - 1; // 254
pub(crate) const MARKER_CONFLICT: u8 = u8::MAX; // 255

// NOTE: We know the hash index has a bucket count <= u32
#[allow(clippy::cast_possible_truncation)]
/// Calculates the bucket index for the given key.
fn calculate_bucket_position(key: &[u8], bucket_count: u32) -> usize {
    use crate::hash::hash64;

    let hash = hash64(key);
    (hash % u64::from(bucket_count)) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn v3_hash_index_build_simple() {
        let mut hash_index = Builder::with_bucket_count(100);

        hash_index.set(b"a", 5);
        hash_index.set(b"b", 8);
        hash_index.set(b"c", 10);

        let bytes = hash_index.into_inner();

        // NOTE: Hash index bytes need to be consistent across machines and compilations etc.
        assert_eq!(
            [
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 10, 254, 254, 254, 8, 254,
                254, 254, 5, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254
            ],
            &*bytes
        );

        let reader = Reader::new(&bytes, 0, 100);
        assert_eq!(0, reader.conflict_count());

        assert_eq!(5, reader.get(b"a"));
        assert_eq!(8, reader.get(b"b"));
        assert_eq!(10, reader.get(b"c"));
        assert_eq!(MARKER_FREE, reader.get(b"d"));
    }

    #[test]
    fn v3_hash_index_build_conflict() {
        let mut hash_index = Builder::with_bucket_count(1);

        hash_index.set(b"a", 5);
        hash_index.set(b"b", 8);

        let bytes = hash_index.into_inner();

        assert_eq!([255], &*bytes);

        assert_eq!(1, Reader::new(&bytes, 0, 1).conflict_count());
    }

    #[test]
    fn v3_hash_index_build_same_offset() {
        let mut hash_index = Builder::with_bucket_count(1);

        hash_index.set(b"a", 5);
        hash_index.set(b"b", 5);

        let bytes = hash_index.into_inner();

        assert_eq!([5], &*bytes);

        let reader = Reader::new(&bytes, 0, 1);
        assert_eq!(0, reader.conflict_count());
        assert_eq!(5, reader.get(b"a"));
        assert_eq!(5, reader.get(b"b"));
    }

    #[test]
    fn v3_hash_index_build_mix() {
        let mut hash_index = Builder::with_bucket_count(1);

        hash_index.set(b"a", 5);
        hash_index.set(b"b", 5);
        hash_index.set(b"c", 6);

        let bytes = hash_index.into_inner();

        assert_eq!([255], &*bytes);

        assert_eq!(1, Reader::new(&bytes, 0, 1).conflict_count());
    }

    #[test]
    fn v3_hash_index_read_conflict() {
        let mut hash_index = Builder::with_bucket_count(1);

        hash_index.set(b"a", 5);
        hash_index.set(b"b", 8);

        let bytes = hash_index.into_inner();

        let reader = Reader::new(&bytes, 0, 1);
        assert_eq!(MARKER_CONFLICT, reader.get(b"a"));
        assert_eq!(MARKER_CONFLICT, reader.get(b"b"));
        assert_eq!(MARKER_CONFLICT, reader.get(b"c"));

        assert_eq!(1, Reader::new(&bytes, 0, 1).conflict_count());
    }
}
