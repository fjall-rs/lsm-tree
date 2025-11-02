pub fn hash64(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}

pub fn hash128(bytes: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128(bytes)
}
