/// A trait allowing to customize prefix extraction in operations with bloom filter.
/// It defines how prefix should be extracted from the key, when update or read
/// a bloom filter.
pub trait PrefixExtractor: Sync + Send {
    /// Extracts prefix from original key
    fn transform<'a>(&self, key: &'a [u8]) -> &'a [u8];
    /// Checks if a key is in domain and prefix can be extracted from it.
    /// For example if `PrefixExtractor` suppose to extract first 4 bytes from key,
    /// `in_domain(&[0, 2, 3])` should return false
    fn in_domain(&self, key: &[u8]) -> bool;
}
