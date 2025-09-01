// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::Arc;

/// Trait for extracting prefixes from keys for prefix bloom filters.
///
/// A prefix extractor allows the bloom filter to index prefixes of keys
/// instead of (or in addition to) the full keys. This enables efficient
/// filtering for prefix-based queries.
///
/// # Examples
///
/// ## Simple fixed-length prefix:
/// ```
/// use lsm_tree::prefix::PrefixExtractor;
///
/// struct FixedPrefixExtractor(usize);
///
/// impl PrefixExtractor for FixedPrefixExtractor {
///     fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         Box::new(std::iter::once(key.get(0..self.0).unwrap_or(key)))
///     }
///     
///     fn name(&self) -> &str {
///         "fixed_prefix"
///     }
/// }
/// ```
///
/// ## Segmented prefixes (e.g., account_id#user_id):
/// ```
/// use lsm_tree::prefix::PrefixExtractor;
///
/// struct SegmentedPrefixExtractor;
///
/// impl PrefixExtractor for SegmentedPrefixExtractor {
///     fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         let mut prefixes = vec![];
///         let mut end = 0;
///         for (i, &byte) in key.iter().enumerate() {
///             if byte == b'#' {
///                 prefixes.push(&key[0..i]);
///                 end = i;
///             }
///         }
///         if end < key.len() {
///             prefixes.push(key);
///         }
///         Box::new(prefixes.into_iter())
///     }
///     
///     fn name(&self) -> &str {
///         "segmented_prefix"
///     }
/// }
/// ```
pub trait PrefixExtractor: Send + Sync {
    /// Extracts zero or more prefixes from a key.
    ///
    /// All prefixes will be added to the bloom filter during segment construction.
    ///
    /// An empty iterator means the key is "out of domain" and won't be added to the bloom filter.
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a>;

    /// Returns a name/identifier for this prefix extractor.
    /// This is useful for debugging and configuration.
    fn name(&self) -> &str;
}

/// A no-op prefix extractor that extracts the entire key.
///
/// This is equivalent to the default bloom filter behavior.
#[derive(Clone, Debug)]
pub struct FullKeyExtractor;

impl PrefixExtractor for FullKeyExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(std::iter::once(key))
    }

    fn name(&self) -> &str {
        "full_key"
    }
}

/// Extracts a fixed-length prefix from keys.
///
/// If a key is shorter than the specified length, the entire key is used.
/// Similar to RocksDB's NewCappedPrefixTransform.
#[derive(Clone, Debug)]
pub struct FixedPrefixExtractor {
    /// Length of the prefix to extract
    pub length: usize,
}

impl FixedPrefixExtractor {
    /// Creates a new fixed-length prefix extractor.
    pub fn new(length: usize) -> Self {
        Self { length }
    }
}

impl PrefixExtractor for FixedPrefixExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        let prefix = if key.len() <= self.length {
            key
        } else {
            &key[0..self.length]
        };
        Box::new(std::iter::once(prefix))
    }

    fn name(&self) -> &str {
        "fixed_prefix"
    }
}

/// Extracts a fixed-length prefix only if the key is long enough.
///
/// Keys shorter than the specified length are "out of domain" and return an empty iterator.
/// Similar to RocksDB's NewFixedPrefixTransform.
#[derive(Clone, Debug)]
pub struct FixedLengthExtractor {
    /// Required minimum length for keys
    pub length: usize,
}

impl FixedLengthExtractor {
    /// Creates a new fixed-length extractor.
    pub fn new(length: usize) -> Self {
        Self { length }
    }
}

impl PrefixExtractor for FixedLengthExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        if key.len() < self.length {
            // Key is too short - out of domain
            Box::new(std::iter::empty())
        } else {
            Box::new(std::iter::once(&key[0..self.length]))
        }
    }

    fn name(&self) -> &str {
        "fixed_length"
    }
}

/// Examples of custom multi-prefix extractors.
///
/// Users can implement their own prefix extractors that return multiple prefixes.
/// The bloom filter will include all returned prefixes.
///
/// # Example
///
/// ```
/// use lsm_tree::prefix::PrefixExtractor;
/// use std::sync::Arc;
///
/// // Example 1: Hierarchical prefix extractor based on delimiter
/// // For key "user/123/data" with delimiter '/', generates:
/// // - "user"
/// // - "user/123"
/// // - "user/123/data" (full key)
/// struct HierarchicalPrefixExtractor {
///     delimiter: u8,
/// }
///
/// impl PrefixExtractor for HierarchicalPrefixExtractor {
///     fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         let delimiter = self.delimiter;
///         let mut prefixes = Vec::new();
///         
///         // Generate all prefixes up to each delimiter
///         for (i, &byte) in key.iter().enumerate() {
///             if byte == delimiter {
///                 prefixes.push(&key[0..i]);
///             }
///         }
///         
///         // Always include the full key
///         prefixes.push(key);
///         
///         Box::new(prefixes.into_iter())
///     }
///     
///     fn name(&self) -> &str {
///         "hierarchical_prefix"
///     }
/// }
///
/// // Example 2: Domain-based extractor for email-like keys
/// // For "user@example.com", generates:
/// // - "example.com" (domain)
/// // - "user@example.com" (full key)
/// struct EmailDomainExtractor;
///
/// impl PrefixExtractor for EmailDomainExtractor {
///     fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         if let Ok(key_str) = std::str::from_utf8(key) {
///             if let Some(at_pos) = key_str.find('@') {
///                 // Return both domain and full email
///                 let domain = &key[at_pos + 1..];
///                 return Box::new(vec![domain, key].into_iter());
///             }
///         }
///         // If not an email format, just return the full key
///         Box::new(std::iter::once(key))
///     }
///     
///     fn name(&self) -> &str {
///         "email_domain"
///     }
/// }
///
/// // Usage:
/// # let path = tempfile::tempdir()?;
/// let tree = lsm_tree::Config::new(path)
///     .prefix_extractor(Arc::new(HierarchicalPrefixExtractor { delimiter: b'/' }))
///     .open()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```

/// Type alias for a shared prefix extractor
pub type SharedPrefixExtractor = Arc<dyn PrefixExtractor>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_key_extractor() {
        let extractor = FullKeyExtractor;
        let key = b"test_key";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], key);
    }

    #[test]
    fn test_fixed_prefix_extractor() {
        let extractor = FixedPrefixExtractor::new(5);

        // Key longer than prefix length
        let key = b"test_key_long";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], b"test_");

        // Key shorter than prefix length
        let key = b"key";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], b"key");

        // Key exactly prefix length
        let key = b"exact";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], b"exact");
    }

    #[test]
    fn test_empty_key() {
        let full_key = FullKeyExtractor;
        let fixed = FixedPrefixExtractor::new(5);

        let key = b"";

        let prefixes: Vec<_> = full_key.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], b"");

        let prefixes: Vec<_> = fixed.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], b"");
    }

    #[test]
    fn test_fixed_length_extractor() {
        let extractor = FixedLengthExtractor::new(5);

        // Key shorter than required length - out of domain
        let key = b"abc";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 0); // Empty iterator

        // Key exactly required length
        let key = b"exact";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], b"exact");

        // Key longer than required length
        let key = b"longer_key";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], b"longe");
    }
}
