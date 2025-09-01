// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::{
    prefix::{FixedLengthExtractor, FixedPrefixExtractor, FullKeyExtractor, PrefixExtractor},
    AbstractTree, Config,
};
use std::sync::Arc;
use test_log::test;

fn generate_test_key(prefix: &str, suffix: &str) -> Vec<u8> {
    format!("{}{}", prefix, suffix).into_bytes()
}

fn generate_delimiter_key(parts: &[&str], delimiter: u8) -> Vec<u8> {
    let delimiter_str = String::from_utf8(vec![delimiter]).unwrap();
    parts.join(&delimiter_str).into_bytes()
}

#[test]
fn test_prefix_bloom_filter_with_fixed_prefix() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let prefix_len = 8;

    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
        .open()?;

    // Insert keys with common prefixes
    let prefix1 = "prefix01";
    let prefix2 = "prefix02";

    for i in 0..100 {
        let key1 = generate_test_key(prefix1, &format!("_{:04}", i));
        let key2 = generate_test_key(prefix2, &format!("_{:04}", i));

        tree.insert(key1, b"value1", 0);
        tree.insert(key2, b"value2", 0);
    }

    tree.flush_active_memtable(0)?;

    // Test that keys with matching prefixes are found
    for i in 0..100 {
        let key1 = generate_test_key(prefix1, &format!("_{:04}", i));
        let key2 = generate_test_key(prefix2, &format!("_{:04}", i));

        assert!(tree.get(&key1, None)?.is_some());
        assert!(tree.get(&key2, None)?.is_some());
    }

    // Test that keys with non-matching prefixes are correctly filtered
    let non_existent_prefix = "notexist";
    for i in 0..10 {
        let key = generate_test_key(non_existent_prefix, &format!("_{:04}", i));
        assert!(tree.get(&key, None)?.is_none());
    }

    Ok(())
}

// Custom delimiter-based prefix extractor for testing
struct TestDelimiterExtractor {
    delimiter: u8,
}

impl PrefixExtractor for TestDelimiterExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        let delimiter = self.delimiter;
        let mut prefixes = Vec::new();

        for (i, &byte) in key.iter().enumerate() {
            if byte == delimiter {
                prefixes.push(&key[0..i]);
            }
        }

        prefixes.push(key);
        Box::new(prefixes.into_iter())
    }

    fn name(&self) -> &str {
        "test_delimiter"
    }
}

#[test]
fn test_prefix_bloom_filter_with_delimiter() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let delimiter = b'#';

    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(TestDelimiterExtractor { delimiter }))
        .open()?;

    // Insert hierarchical keys
    let accounts = vec!["acc001", "acc002", "acc003"];
    let users = vec!["user1", "user2", "user3"];
    let items = vec!["item_a", "item_b", "item_c"];

    for account in &accounts {
        for user in &users {
            for item in &items {
                let key = generate_delimiter_key(&[account, user, item], delimiter);
                let value = format!("{}-{}-{}", account, user, item);
                tree.insert(key, value.as_bytes(), 0);
            }
        }
    }

    tree.flush_active_memtable(0)?;

    // Test exact key lookups
    for account in &accounts {
        for user in &users {
            for item in &items {
                let key = generate_delimiter_key(&[account, user, item], delimiter);
                let result = tree.get(&key, None)?;
                assert!(
                    result.is_some(),
                    "Failed to find key: {:?}",
                    String::from_utf8_lossy(&key)
                );
            }
        }
    }

    // Test that non-existent hierarchical keys are filtered
    let non_existent_key = generate_delimiter_key(&["nonacc", "nonuser", "nonitem"], delimiter);
    assert!(tree.get(&non_existent_key, None)?.is_none());

    Ok(())
}

// Custom extractor with max segments for testing
struct TestMaxSegmentExtractor {
    delimiter: u8,
    max_segments: usize,
}

impl PrefixExtractor for TestMaxSegmentExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        let delimiter = self.delimiter;
        let max_segments = self.max_segments;
        let mut prefixes = Vec::new();
        let mut segment_count = 0;

        for (i, &byte) in key.iter().enumerate() {
            if byte == delimiter {
                segment_count += 1;
                prefixes.push(&key[0..i]);
                if segment_count >= max_segments {
                    break;
                }
            }
        }

        prefixes.push(key);
        Box::new(prefixes.into_iter())
    }

    fn name(&self) -> &str {
        "test_max_segment"
    }
}

#[test]
fn test_prefix_bloom_filter_with_max_segments() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let delimiter = b'/';

    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(TestMaxSegmentExtractor {
            delimiter,
            max_segments: 2,
        }))
        .open()?;

    // Insert path-like keys
    let paths = vec![
        "root/dir1/subdir1/file1.txt",
        "root/dir1/subdir2/file2.txt",
        "root/dir2/subdir3/file3.txt",
        "root/dir2/subdir4/file4.txt",
        "other/dir3/subdir5/file5.txt",
    ];

    for path in &paths {
        tree.insert(path.as_bytes(), b"content", 0);
    }

    tree.flush_active_memtable(0)?;

    // Test that all inserted paths are found
    for path in &paths {
        assert!(tree.get(path.as_bytes(), None)?.is_some());
    }

    // Test that non-existent paths are filtered
    let non_existent_paths = vec![
        "nonexistent/dir/file.txt",
        "root/nondir/file.txt",
        "other/nondir/file.txt",
    ];

    for path in &non_existent_paths {
        assert!(tree.get(path.as_bytes(), None)?.is_none());
    }

    Ok(())
}

#[test]
fn test_prefix_bloom_filter_empty_keys() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(5)))
        .open()?;

    // Insert very short keys (empty keys not allowed)
    tree.insert(b"a", b"single_char", 0);
    tree.insert(b"ab", b"two_chars", 0);
    tree.insert(b"abc", b"three_chars", 0);
    tree.insert(b"abcd", b"four_chars", 0);
    tree.insert(b"abcde", b"five_chars", 0); // Exactly prefix length
    tree.insert(b"abcdef", b"six_chars", 0); // Longer than prefix

    tree.flush_active_memtable(0)?;

    // Verify all keys can be retrieved
    assert_eq!(tree.get(b"a", None)?.unwrap().as_ref(), b"single_char");
    assert_eq!(tree.get(b"ab", None)?.unwrap().as_ref(), b"two_chars");
    assert_eq!(tree.get(b"abc", None)?.unwrap().as_ref(), b"three_chars");
    assert_eq!(tree.get(b"abcd", None)?.unwrap().as_ref(), b"four_chars");
    assert_eq!(tree.get(b"abcde", None)?.unwrap().as_ref(), b"five_chars");
    assert_eq!(tree.get(b"abcdef", None)?.unwrap().as_ref(), b"six_chars");

    Ok(())
}

#[test]
fn test_prefix_bloom_filter_with_full_key_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // This should behave like a regular bloom filter
    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(FullKeyExtractor))
        .open()?;

    let keys = vec![
        b"key1".to_vec(),
        b"key2".to_vec(),
        b"key3".to_vec(),
        b"different_key".to_vec(),
        b"another_different_key".to_vec(),
    ];

    for key in &keys {
        tree.insert(key, b"value", 0);
    }

    tree.flush_active_memtable(0)?;

    // All inserted keys should be found
    for key in &keys {
        assert!(tree.get(key, None)?.is_some());
    }

    // Non-existent keys should not be found
    let non_existent = vec![
        b"nonexistent1".to_vec(),
        b"nonexistent2".to_vec(),
        b"key".to_vec(),   // prefix of "key1" but not exact match
        b"key11".to_vec(), // extends "key1" but not exact match
    ];

    for key in &non_existent {
        assert!(tree.get(key, None)?.is_none());
    }

    Ok(())
}

#[test]
fn test_prefix_bloom_filter_with_unicode() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(12))) // 12 bytes = 4 UTF-8 chars for many unicode
        .open()?;

    let unicode_keys = vec![
        "用户_001_数据".as_bytes().to_vec(),
        "用户_002_数据".as_bytes().to_vec(),
        "客户_001_信息".as_bytes().to_vec(),
        "客户_002_信息".as_bytes().to_vec(),
    ];

    for key in &unicode_keys {
        tree.insert(key, b"value", 0);
    }

    tree.flush_active_memtable(0)?;

    // All keys should be retrievable
    for key in &unicode_keys {
        assert!(tree.get(key, None)?.is_some());
    }

    Ok(())
}

#[test]
fn test_prefix_bloom_filter_large_scale() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(8)))
        .bloom_bits_per_key(10)
        .open()?;

    // Insert many keys with various prefixes
    let prefixes = vec!["prefix00", "prefix01", "prefix02", "prefix03", "prefix04"];
    let items_per_prefix = 1000;

    for prefix in &prefixes {
        for i in 0..items_per_prefix {
            let key = format!("{}_{:06}", prefix, i);
            let value = format!("value_{}", i);
            tree.insert(key.as_bytes(), value.as_bytes(), 0);
        }
    }

    tree.flush_active_memtable(0)?;

    // Verify a sample of keys from each prefix
    for prefix in &prefixes {
        for i in (0..items_per_prefix).step_by(100) {
            let key = format!("{}_{:06}", prefix, i);
            assert!(tree.get(key.as_bytes(), None)?.is_some());
        }
    }

    // Test that keys with non-existent prefixes are filtered
    for i in 0..100 {
        let key = format!("nonexist_{:06}", i);
        assert!(tree.get(key.as_bytes(), None)?.is_none());
    }

    Ok(())
}

#[test]
fn test_prefix_bloom_filter_with_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(TestDelimiterExtractor { delimiter: b':' }))
        .level_count(3)
        .open()?;

    // Insert data in multiple batches to trigger compaction
    for batch in 0..5 {
        for i in 0..100 {
            let key = format!("namespace{}:key_{:04}", batch, i);
            let value = format!("value_batch{}_item{}", batch, i);
            tree.insert(key.as_bytes(), value.as_bytes(), batch);
        }
        tree.flush_active_memtable(0)?;
    }

    // Force compaction
    tree.major_compact(u64::MAX, 0)?;

    // Verify keys are still accessible after compaction
    for batch in 0..5 {
        for i in (0..100).step_by(10) {
            let key = format!("namespace{}:key_{:04}", batch, i);
            assert!(tree.get(key.as_bytes(), None)?.is_some());
        }
    }

    // Verify non-existent namespaces are filtered
    for i in 0..10 {
        let key = format!("nonexistent:key_{:04}", i);
        assert!(tree.get(key.as_bytes(), None)?.is_none());
    }

    Ok(())
}

#[test]
fn test_fixed_length_extractor_with_short_keys() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Requires keys to be at least 8 bytes long
    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(FixedLengthExtractor::new(8)))
        .open()?;

    // Insert keys of various lengths
    tree.insert(b"short", b"too_short", 0); // 5 bytes - out of domain
    tree.insert(b"seven77", b"still_short", 0); // 7 bytes - out of domain
    tree.insert(b"exactly8", b"exact_match", 0); // 8 bytes - in domain
    tree.insert(b"longer_than_8", b"long_key", 0); // 13 bytes - in domain

    tree.flush_active_memtable(0)?;

    // Verify all keys are stored (even out-of-domain ones)
    assert_eq!(tree.get(b"short", None)?.unwrap().as_ref(), b"too_short");
    assert_eq!(
        tree.get(b"seven77", None)?.unwrap().as_ref(),
        b"still_short"
    );
    assert_eq!(
        tree.get(b"exactly8", None)?.unwrap().as_ref(),
        b"exact_match"
    );
    assert_eq!(
        tree.get(b"longer_than_8", None)?.unwrap().as_ref(),
        b"long_key"
    );

    // Keys with non-matching 8-byte prefixes should not be found
    assert!(tree.get(b"notexist", None)?.is_none()); // Exactly 8 bytes but different prefix
    assert!(tree.get(b"different_prefix", None)?.is_none()); // Different 8-byte prefix

    Ok(())
}

#[test]
fn test_custom_prefix_extractor() -> lsm_tree::Result<()> {
    // Create a custom prefix extractor that extracts email domains
    struct EmailDomainExtractor;

    impl PrefixExtractor for EmailDomainExtractor {
        fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            let key_str = std::str::from_utf8(key).unwrap_or("");
            if let Some(at_pos) = key_str.find('@') {
                // Return both the domain and the full email
                let domain = &key[at_pos + 1..];
                Box::new(vec![domain, key].into_iter())
            } else {
                // Not an email, just return the full key
                Box::new(std::iter::once(key))
            }
        }

        fn name(&self) -> &str {
            "email_domain"
        }
    }

    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(EmailDomainExtractor))
        .open()?;

    // Insert email-like keys
    let emails = vec![
        "user1@example.com",
        "user2@example.com",
        "admin@example.com",
        "info@company.org",
        "support@company.org",
        "contact@other.net",
    ];

    for email in &emails {
        tree.insert(email.as_bytes(), b"user_data", 0);
    }

    tree.flush_active_memtable(0)?;

    // All emails should be found
    for email in &emails {
        assert!(tree.get(email.as_bytes(), None)?.is_some());
    }

    // Non-existent emails with non-existent domains should be filtered
    let non_existent = vec!["user@nonexistent.com", "admin@notreal.org"];

    for email in &non_existent {
        assert!(tree.get(email.as_bytes(), None)?.is_none());
    }

    Ok(())
}

#[test]
fn test_prefix_bloom_filter_with_updates() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(6)))
        .open()?;

    // Initial insert
    for i in 0..100 {
        let key = format!("prefix_{:04}", i);
        tree.insert(key.as_bytes(), b"initial", 0);
    }

    tree.flush_active_memtable(0)?;

    // Update values
    for i in 0..100 {
        let key = format!("prefix_{:04}", i);
        tree.insert(key.as_bytes(), b"updated", 1);
    }

    tree.flush_active_memtable(0)?;

    // Verify updated values are retrieved
    for i in 0..100 {
        let key = format!("prefix_{:04}", i);
        let value = tree.get(key.as_bytes(), None)?;
        assert_eq!(value.unwrap().as_ref(), b"updated");
    }

    Ok(())
}

#[test]
fn test_prefix_bloom_filter_with_deletes() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .prefix_extractor(Arc::new(TestDelimiterExtractor { delimiter: b'-' }))
        .open()?;

    // Insert keys
    let keys = vec![
        "category-item-001",
        "category-item-002",
        "category-item-003",
        "product-item-001",
        "product-item-002",
    ];

    for key in &keys {
        tree.insert(key.as_bytes(), b"value", 0);
    }

    tree.flush_active_memtable(0)?;

    // Delete some keys
    tree.remove("category-item-002", 1);
    tree.remove("product-item-001", 1);

    tree.flush_active_memtable(0)?;

    // Verify deleted keys return None
    assert!(tree.get(b"category-item-002", None)?.is_none());
    assert!(tree.get(b"product-item-001", None)?.is_none());

    // Verify non-deleted keys are still present
    assert!(tree.get(b"category-item-001", None)?.is_some());
    assert!(tree.get(b"category-item-003", None)?.is_some());
    assert!(tree.get(b"product-item-002", None)?.is_some());

    Ok(())
}
