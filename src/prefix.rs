// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::Arc;

/// Trait for extracting prefixes from keys for prefix filters.
///
/// A prefix extractor allows the filter to index prefixes of keys
/// instead of (or in addition to) the full keys.
/// This enables efficient filtering for prefix-based queries.
///
/// # Examples
///
/// ## Simple fixed-length
///
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
///
/// let ex = FixedPrefixExtractor(3);
/// assert_eq!(ex.name(), "fixed_prefix");
/// assert_eq!(ex.extract_first(b"abcdef"), Some(b"abc".as_ref()));
/// assert_eq!(ex.extract_first(b"ab"), Some(b"ab".as_ref()));
/// ```
///
/// ## Segmented prefixes (e.g., `account_id#user_id)`
///
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
///
/// let ex = SegmentedPrefixExtractor;
/// assert_eq!(ex.name(), "segmented_prefix");
/// let prefixes: Vec<_> = ex.extract(b"acc#usr#data").collect();
/// assert_eq!(prefixes, vec![b"acc".as_ref(), b"acc#usr", b"acc#usr#data"]);
/// let prefixes: Vec<_> = ex.extract(b"plain_key").collect();
/// assert_eq!(prefixes, vec![b"plain_key".as_ref()]);
/// ```
pub trait PrefixExtractor:
    Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe
{
    /// Extracts zero or more prefixes from a key.
    ///
    /// All prefixes will be added to the filter during table construction.
    ///
    /// An empty iterator means the key is "out of domain" and won't be added to the filter.
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a>;

    /// Extracts the first prefix from a key.
    ///
    /// By default, this is derived from `extract`, meaning it is equivalent to `extract(key).next()`,
    /// however it can overridden to skip the Box allocation of `extract`.
    fn extract_first<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        self.extract(key).next()
    }

    /// Returns a unique name for this prefix extractor.
    fn name(&self) -> &str;
}

/// A prefix extractor that returns the full key.
///
/// Useful when callers want prefix-aware filtering that behaves identically to
/// full-key filtering (e.g., for testing or as an explicit no-op extractor).
pub struct FullKeyExtractor;

impl PrefixExtractor for FullKeyExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(std::iter::once(key))
    }

    fn extract_first<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        Some(key)
    }

    fn name(&self) -> &'static str {
        "full_key"
    }
}

/// A prefix extractor that returns a fixed-length prefix.
///
/// If the key is shorter than the prefix length, returns the full key.
pub struct FixedPrefixExtractor {
    length: usize,
    name: String,
}

impl FixedPrefixExtractor {
    /// Creates a new fixed-length prefix extractor.
    #[must_use]
    pub fn new(length: usize) -> Self {
        Self {
            length,
            name: format!("fixed_prefix:{length}"),
        }
    }
}

impl PrefixExtractor for FixedPrefixExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(self.extract_first(key).into_iter())
    }

    fn extract_first<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        if key.len() <= self.length {
            Some(key)
        } else {
            key.get(..self.length)
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A prefix extractor that requires keys to be at least a certain length.
///
/// Keys shorter than the required length are considered "out of domain"
/// and won't be added to the filter.
/// This matches `RocksDB`'s behavior.
pub struct FixedLengthExtractor {
    length: usize,
    name: String,
}

impl FixedLengthExtractor {
    /// Creates a new fixed-length extractor.
    #[must_use]
    pub fn new(length: usize) -> Self {
        Self {
            length,
            name: format!("fixed_length:{length}"),
        }
    }
}

impl PrefixExtractor for FixedLengthExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(self.extract_first(key).into_iter())
    }

    fn extract_first<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        if key.len() < self.length {
            // Key is too short - out of domain
            None
        } else {
            #[expect(
                clippy::expect_used,
                reason = "key.len() >= self.length is checked above"
            )]
            Some(
                key.get(..self.length)
                    .expect("prefix slice should be in bounds"),
            )
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Users can implement their own prefix extractors that return multiple prefixes.
/// The filter will include all returned prefixes.
///
/// # Examples
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
/// // Example 2: Extract domain prefix for flipped email keys
/// // For "example.com@user", this extracts:
/// // - "example.com" (domain prefix for range scans)
/// // - "example.com@user" (full key for point lookups)
/// struct DomainPrefixExtractor;
///
/// impl PrefixExtractor for DomainPrefixExtractor {
///     fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         if let Ok(key_str) = std::str::from_utf8(key) {
///             if let Some(at_pos) = key_str.find('@') {
///                 // Return both domain prefix and full key
///                 let domain_prefix = &key[..at_pos];
///                 return Box::new(vec![domain_prefix, key].into_iter());
///             }
///         }
///         // If not a flipped email format, just return the full key
///         Box::new(std::iter::once(key))
///     }
///     
///     fn name(&self) -> &str {
///         "domain_prefix"
///     }
/// }
///
/// let ex = DomainPrefixExtractor;
/// assert_eq!(ex.name(), "domain_prefix");
/// let prefixes: Vec<_> = ex.extract(b"example.com@user").collect();
/// assert_eq!(prefixes, vec![b"example.com".as_ref(), b"example.com@user"]);
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
        assert_eq!(prefixes.first(), Some(&b"test_key".as_ref()));
    }

    #[test]
    fn test_fixed_prefix_extractor() {
        let extractor = FixedPrefixExtractor::new(5);

        // Key longer than prefix
        let key = b"longer_key";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"longe".as_ref()));

        // Key shorter than prefix
        let key = b"key";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"key".as_ref()));

        // Key exactly prefix length
        let key = b"exact";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"exact".as_ref()));
    }

    #[test]
    fn test_empty_key() {
        let full_key = FullKeyExtractor;
        let fixed = FixedPrefixExtractor::new(5);

        let key = b"";

        let prefixes: Vec<_> = full_key.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"".as_ref()));

        let prefixes: Vec<_> = fixed.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"".as_ref()));
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
        assert_eq!(prefixes.first(), Some(&b"exact".as_ref()));

        // Key longer than required length
        let key = b"longer_key";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"longe".as_ref()));
    }

    #[test]
    fn test_extractor_names() {
        assert_eq!(FullKeyExtractor.name(), "full_key");
        assert_eq!(FixedPrefixExtractor::new(4).name(), "fixed_prefix:4");
        assert_eq!(FixedPrefixExtractor::new(3).name(), "fixed_prefix:3");
        assert_eq!(FixedLengthExtractor::new(4).name(), "fixed_length:4");
        assert_eq!(FixedLengthExtractor::new(3).name(), "fixed_length:3");
    }
}
