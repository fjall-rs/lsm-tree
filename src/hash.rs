pub fn hash64(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}

pub fn hash128(bytes: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_hash64() {
        assert_eq!(16_959_823_422_411_450_475, hash64(&[0, 0, 0]));
        assert_eq!(8_004_557_073_989_523_290, hash64(&[0, 0, 1]));
    }

    #[test]
    fn test_hash128() {
        assert_eq!(
            321_827_061_816_535_117_015_859_907_874_601_773_163,
            hash128(&[0, 0, 0])
        );
        assert_eq!(
            154_036_699_985_066_753_773_347_827_765_470_844_762,
            hash128(&[0, 0, 1])
        );
    }
}
