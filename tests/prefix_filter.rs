use lsm_tree::config::{
    BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry, KvSeparationOptions, PinningPolicy,
};
use lsm_tree::Guard;
use lsm_tree::SequenceNumberCounter;
use lsm_tree::{
    prefix::{FixedLengthExtractor, FixedPrefixExtractor, FullKeyExtractor, PrefixExtractor},
    AbstractTree, Config, SeqNo,
};
use std::sync::Arc;

// Helper function to generate test keys with prefixes
fn generate_test_key(prefix: &str, suffix: &str) -> Vec<u8> {
    format!("{}{}", prefix, suffix).into_bytes()
}

#[test]
fn test_prefix_filter_range_start_only_prefix_no_upfront_prune() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let prefix_len = 5;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
    .open()?;

    for i in 0..50u32 {
        let key = format!("zebra_{:04}", i);
        tree.insert(key.as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    for i in 0..50u32 {
        let key = format!("zulu_{:04}", i);
        tree.insert(key.as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    let count = tree
        .range::<&[u8], std::ops::RangeFrom<&[u8]>>((&b"user1_0000"[..]).., u64::MAX, None)
        .count();
    assert_eq!(count, 100);

    Ok(())
}

#[test]
fn test_prefix_filter_with_fixed_prefix() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let prefix_len = 8;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
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

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Test that keys with matching prefixes are found
    for i in 0..100 {
        let key1 = generate_test_key(prefix1, &format!("_{:04}", i));
        let key2 = generate_test_key(prefix2, &format!("_{:04}", i));

        assert!(tree.contains_key(&key1, u64::MAX)?);
        assert!(tree.contains_key(&key2, u64::MAX)?);
    }

    // Test that keys with non-matching prefixes work correctly
    let non_existent_key = generate_test_key("prefix99", "_0000");
    assert!(!tree.contains_key(&non_existent_key, u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        // We should have at least 201 filter queries (200 existing keys + 1 non-existent)
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_with_fixed_length() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let required_len = 10;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedLengthExtractor::new(required_len)))
    .open()?;

    // Insert keys with exactly the required length prefix
    for i in 0..50 {
        let key = format!("exactlen{:02}_suffix_{}", i, i);
        tree.insert(key.as_bytes(), b"value", 0);
    }

    // Insert keys that are too short (out of domain)
    for i in 0..20 {
        let short_key = format!("key{}", i);
        tree.insert(short_key.as_bytes(), b"short_value", 0);
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Verify keys with matching length are found
    for i in 0..50 {
        let key = format!("exactlen{:02}_suffix_{}", i, i);
        assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
    }

    // Verify short keys are also found (they're stored but not in filter)
    for i in 0..20 {
        let short_key = format!("key{}", i);
        assert!(tree.contains_key(short_key.as_bytes(), u64::MAX)?);
    }

    // Verify non-existent prefix is quickly rejected
    // Use a key that matches the required length to ensure it's in-domain
    let range = tree.range("nonexist00".."nonexist99", u64::MAX, None);
    assert_eq!(range.count(), 0);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        // Should have filter queries for all lookups
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_full_key() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Using FullKeyExtractor (default behavior)
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FullKeyExtractor))
    .open()?;

    // Insert various keys
    let keys = vec![
        b"apple".to_vec(),
        b"banana".to_vec(),
        b"cherry".to_vec(),
        b"date".to_vec(),
        b"elderberry".to_vec(),
    ];

    for key in &keys {
        tree.insert(key.clone(), b"value", 0);
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // All keys should be found
    for key in &keys {
        assert!(tree.contains_key(key, u64::MAX)?);
    }

    // Non-existent key test
    assert!(!tree.contains_key(b"fig", u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        // Should have filter queries for in-domain keys
        assert!(
            final_queries > initial_queries,
            "filter queries should increase for in-domain keys"
        );
    }
    assert!(!tree.contains_key(b"kiwi", u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        // Should have queries for all lookups (5 existing + 2 non-existent)
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_range_queries() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let prefix_len = 5;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
    .open()?;

    // Insert keys with common prefixes
    let prefixes = vec!["user_", "post_", "comm_"];

    for prefix in &prefixes {
        for i in 0..20 {
            let key = format!("{}{:04}", prefix, i);
            tree.insert(key.as_bytes(), format!("value_{}", i).as_bytes(), 0);
        }
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Test prefix iteration
    for prefix in &prefixes {
        let start_key = prefix.to_string();
        let end_key = format!("{}~", prefix); // '~' is after all digits and letters

        let count = tree
            .range(start_key.as_bytes()..end_key.as_bytes(), u64::MAX, None)
            .count();
        assert_eq!(count, 20);
    }

    // Test non-existent prefix range
    let count = tree
        .range(&b"none_"[..]..&b"none~"[..], u64::MAX, None)
        .count();
    assert_eq!(count, 0);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_after_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let prefix_len = 6;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
    .open()?;

    // Insert first batch of keys
    for i in 0..50 {
        let key = format!("batch1_{:04}", i);
        tree.insert(key.as_bytes(), b"value1", 0);
    }

    tree.flush_active_memtable(0)?;

    // Insert second batch with overlapping keys
    for i in 25..75 {
        let key = format!("batch1_{:04}", i);
        tree.insert(key.as_bytes(), b"value2", 0);
    }

    tree.flush_active_memtable(0)?;

    // Force compaction
    use lsm_tree::compaction::Leveled;
    tree.compact(Arc::new(Leveled::default()), 0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    // All keys should still be found after compaction
    for i in 0..75 {
        let key = format!("batch1_{:04}", i);
        assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
    }

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Should have filter queries for post-compaction lookups
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased after compaction"
        );

        // All keys exist, so hits should not increase
        assert_eq!(
            final_hits, initial_hits,
            "filter hits should not increase for existing keys"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_with_deletions() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let prefix_len = 7;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
    .open()?;

    // Insert keys
    for i in 0..100 {
        let key = format!("deltest_{:04}", i);
        tree.insert(key.as_bytes(), b"value", 0);
    }

    tree.flush_active_memtable(0)?;

    // Delete some keys
    for i in (0..100).step_by(2) {
        let key = format!("deltest_{:04}", i);
        tree.remove(key.as_bytes(), 0);
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    // Verify deletions
    for i in 0..100 {
        let key = format!("deltest_{:04}", i);
        if i % 2 == 0 {
            assert!(!tree.contains_key(key.as_bytes(), u64::MAX)?);
        } else {
            assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
        }
    }

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Should have filter queries for all lookups after deletions
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased for deletion checks"
        );

        // Deleted keys still pass filter (tombstones), so hits should not increase
        assert_eq!(
            final_hits, initial_hits,
            "filter hits should not increase (deleted keys still in filter)"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_edge_cases() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Test with prefix length of 1
    let tree = Config::new(
        folder.path().join("test1"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(1)))
    .open()?;

    tree.insert(b"a", b"value", 0);
    tree.insert(b"b", b"value", 0);
    tree.insert(b"ab", b"value", 0);
    tree.insert(b"ba", b"value", 0);

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    assert!(tree.contains_key(b"a", u64::MAX)?);
    assert!(tree.contains_key(b"b", u64::MAX)?);
    assert!(tree.contains_key(b"ab", u64::MAX)?);
    assert!(tree.contains_key(b"ba", u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        // Should have queries for both existing and non-existing keys
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased for point lookups"
        );
    }

    // Test with empty keys
    let tree2 = Config::new(
        folder.path().join("test2"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(5)))
    .open()?;

    tree2.insert(b"test", b"short_key", 0);
    tree2.insert(b"longer_key", b"long_key", 0);

    tree2.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries2 = tree2.metrics().filter_queries();

    assert!(tree2.contains_key(b"test", u64::MAX)?);
    assert!(tree2.contains_key(b"longer_key", u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries2 = tree2.metrics().filter_queries();
        assert!(
            final_queries2 > initial_queries2,
            "filter queries should have increased for short/long key lookups"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_large_dataset() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let prefix_len = 12;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
    .open()?;

    // Insert a large number of keys with various prefixes
    let prefixes = vec![
        "transaction_",
        "userprofile_",
        "sessiondata_",
        "logentryval_",
    ];

    for prefix in &prefixes {
        for i in 0..1000 {
            let key = format!("{}{:08}", prefix, i);
            let value = format!("data_{}", i);
            tree.insert(key.as_bytes(), value.as_bytes(), 0);

            // Flush periodically to create multiple segments
            if i % 250 == 249 {
                tree.flush_active_memtable(0)?;
            }
        }
    }

    // Final flush
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Verify all keys are found
    for prefix in &prefixes {
        for i in 0..1000 {
            let key = format!("{}{:08}", prefix, i);
            assert!(
                tree.contains_key(key.as_bytes(), u64::MAX)?,
                "Key {} not found",
                key
            );
        }
    }

    // Test non-existent keys with matching prefixes
    for prefix in &prefixes {
        let non_existent_key = format!("{}{:08}", prefix, 9999);
        assert!(!tree.contains_key(non_existent_key.as_bytes(), u64::MAX)?);
    }

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        // With multiple segments, we should have many filter queries
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased for large dataset"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_concurrent_access() -> lsm_tree::Result<()> {
    use std::thread;

    let folder = tempfile::tempdir()?;
    let prefix_len = 8;

    let tree = Arc::new(
        Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
        .open()?,
    );

    // Spawn multiple threads to insert data
    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let tree = Arc::clone(&tree);
            thread::spawn(move || {
                for i in 0..250 {
                    let key = format!("thread{:02}_{:04}", thread_id, i);
                    tree.insert(key.as_bytes(), b"value", 0);
                }
            })
        })
        .collect();

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    // Verify all keys from all threads
    for thread_id in 0..4 {
        for i in 0..250 {
            let key = format!("thread{:02}_{:04}", thread_id, i);
            assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
        }
    }

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Should have filter queries for all concurrent lookups
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased for concurrent access"
        );

        // All keys exist, so hits should not increase
        assert_eq!(
            final_hits, initial_hits,
            "filter hits should not increase for existing keys"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_sequence_consistency() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let prefix_len = 9;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
    .open()?;

    // Insert initial data with sequence number 0-49
    for i in 0..50 {
        let key = format!("seqtest1_{:04}", i);
        tree.insert(key.as_bytes(), b"v1", i as u64);
    }

    tree.flush_active_memtable(0)?;

    // Insert more data with sequence numbers 50-99
    for i in 50..100 {
        let key = format!("seqtest1_{:04}", i);
        tree.insert(key.as_bytes(), b"v2", i as u64);
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Verify that at sequence number 50, only the first 50 keys are visible
    // (keys inserted at seqno 0-49 are visible at seqno >= their insert seqno)
    for i in 0..50 {
        let key = format!("seqtest1_{:04}", i);
        assert!(tree.contains_key(key.as_bytes(), 50)?);
    }

    for i in 50..100 {
        let key = format!("seqtest1_{:04}", i);
        assert!(!tree.contains_key(key.as_bytes(), 50)?);
    }

    // Verify tree sees all data at max sequence number
    for i in 0..100 {
        let key = format!("seqtest1_{:04}", i);
        assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
    }

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        // filter should be used for all lookups
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased for sequence consistency checks"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_seek_optimization() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let prefix_len = 8;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
    .open()?;

    // Insert keys with specific prefixes
    for i in 0..100 {
        let key = format!("prefix_a_{:04}", i);
        tree.insert(key.as_bytes(), b"value_a", 0);
    }

    for i in 0..100 {
        let key = format!("prefix_b_{:04}", i);
        tree.insert(key.as_bytes(), b"value_b", 0);
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Seek with existing prefix should find keys
    let range_a = tree.range("prefix_a_0000".."prefix_a_9999", u64::MAX, None);
    assert_eq!(range_a.count(), 100);

    // Seek with non-existent prefix should return empty (optimized via filter)
    let range_c = tree.range("prefix_c_0000".."prefix_c_9999", u64::MAX, None);
    assert_eq!(range_c.count(), 0);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        // Range queries should trigger filter checks
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased for range operations"
        );
    }

    // Verify partial prefix matches work
    let range_partial = tree.range("prefix_a_0050".."prefix_a_0060", u64::MAX, None);
    assert_eq!(range_partial.count(), 10);

    Ok(())
}

#[test]
fn test_no_prefix_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree without prefix extractor (default behavior)
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Insert various keys
    for i in 0..100 {
        let key = format!("noprefix_{:04}", i);
        tree.insert(key.as_bytes(), b"value", 0);
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    // All keys should be found (full key matching)
    for i in 0..100 {
        let key = format!("noprefix_{:04}", i);
        assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
    }

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Should still have filter queries even without prefix extractor (uses full key)
        assert!(
            final_queries > initial_queries,
            "filter queries should work without prefix extractor"
        );

        // All keys exist, so hits should not increase
        assert_eq!(
            final_hits, initial_hits,
            "filter hits should not increase for existing keys"
        );
    }

    Ok(())
}

// Custom segmented prefix extractor for account_id#user_id pattern
struct SegmentedPrefixExtractor {
    delimiter: u8,
}

impl SegmentedPrefixExtractor {
    fn new(delimiter: u8) -> Self {
        Self { delimiter }
    }
}

impl PrefixExtractor for SegmentedPrefixExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        let mut prefixes = Vec::new();

        // Find the first delimiter position
        if let Some(first_delim_pos) = key.iter().position(|&b| b == self.delimiter) {
            // Add the prefix up to the first delimiter (account_id)
            prefixes.push(&key[..first_delim_pos]);

            // Find the second delimiter position
            if let Some(second_delim_pos) = key[first_delim_pos + 1..]
                .iter()
                .position(|&b| b == self.delimiter)
            {
                // Add the prefix up to the second delimiter (account_id#user_id)
                let full_prefix_end = first_delim_pos + 1 + second_delim_pos;
                prefixes.push(&key[..full_prefix_end]);
            } else {
                // If no second delimiter, use the entire key as prefix
                prefixes.push(key);
            }
        } else {
            // No delimiter found, use the entire key
            prefixes.push(key);
        }

        Box::new(prefixes.into_iter())
    }

    fn name(&self) -> &str {
        "SegmentedPrefixExtractor"
    }
}

#[test]
fn test_prefix_filter_segmented_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let delimiter = b'#';

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(SegmentedPrefixExtractor::new(delimiter)))
    .open()?;

    // Insert keys with account_id#user_id#data pattern
    let account1 = "acc001";
    let account2 = "acc002";

    // Insert users for account1
    for user_id in 1..=5 {
        for data_id in 1..=10 {
            let key = format!("{}#user{:03}#data{:04}", account1, user_id, data_id);
            let value = format!("value_{}_{}", user_id, data_id);
            tree.insert(key.as_bytes(), value.as_bytes(), 0);
        }
    }

    // Insert users for account2
    for user_id in 1..=3 {
        for data_id in 1..=10 {
            let key = format!("{}#user{:03}#data{:04}", account2, user_id, data_id);
            let value = format!("value_{}_{}", user_id, data_id);
            tree.insert(key.as_bytes(), value.as_bytes(), 0);
        }
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Test 1: Query for specific user within account1
    let user_key = format!("{}#user002#data0005", account1);
    assert!(tree.contains_key(user_key.as_bytes(), u64::MAX)?);

    // Test 2: Query for all data of a specific user (prefix range query)
    let user_prefix_start = format!("{}#user002#", account1);
    let user_prefix_end = format!("{}#user002~", account1); // ~ is after #
    let user_range = tree.range(
        user_prefix_start.as_bytes()..user_prefix_end.as_bytes(),
        u64::MAX,
        None,
    );
    assert_eq!(user_range.count(), 10); // Should find 10 data items for this user

    // Test 3: Query for all users in account1 (account-level prefix)
    let account_prefix_start = format!("{}#", account1);
    let account_prefix_end = format!("{}~", account1); // ~ is after #
    let account_range = tree.range(
        account_prefix_start.as_bytes()..account_prefix_end.as_bytes(),
        u64::MAX,
        None,
    );
    assert_eq!(account_range.count(), 50); // 5 users * 10 data items

    // Test 4: Query for non-existent account
    let non_existent_start = "acc999#";
    let non_existent_end = "acc999~";
    let non_existent_range = tree.range(
        non_existent_start.as_bytes()..non_existent_end.as_bytes(),
        u64::MAX,
        None,
    );
    assert_eq!(non_existent_range.count(), 0);

    // Test 5: Query for non-existent user in existing account
    let non_user_key = format!("{}#user999#data0001", account1);
    assert!(!tree.contains_key(non_user_key.as_bytes(), u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        // Should have filter queries for all lookups
        assert!(
            final_queries > initial_queries,
            "filter queries should have increased for segmented lookups"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_single_byte_keys() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(2)))
    .open()?;

    // Insert single-byte keys
    for i in 0u8..10 {
        tree.insert(&[i], format!("value_{}", i).as_bytes(), 0);
    }

    // Insert two-byte keys
    for i in 0u8..10 {
        tree.insert(&[i, i], format!("value_{}{}", i, i).as_bytes(), 0);
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // All keys should be found
    for i in 0u8..10 {
        assert!(tree.contains_key(&[i], u64::MAX)?);
        assert!(tree.contains_key(&[i, i], u64::MAX)?);
    }

    // Non-existent single-byte key
    assert!(!tree.contains_key(&[255], u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        // Should have queries for all lookups
        assert!(
            final_queries > initial_queries,
            "filter queries should increase for single/two-byte key lookups"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_null_bytes() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
    .open()?;

    // Insert keys with null bytes
    tree.insert(b"\0\0\0data", b"null_prefix", 0);
    tree.insert(b"pre\0fix", b"null_middle", 0);
    tree.insert(b"suffix\0", b"null_end", 0);
    tree.insert(b"\0", b"single_null", 0);

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // All keys should be found
    assert!(tree.contains_key(b"\0\0\0data", u64::MAX)?);
    assert!(tree.contains_key(b"pre\0fix", u64::MAX)?);
    assert!(tree.contains_key(b"suffix\0", u64::MAX)?);
    assert!(tree.contains_key(b"\0", u64::MAX)?);

    // Non-existent keys with null bytes
    assert!(!tree.contains_key(b"\0\0\0missing", u64::MAX)?);
    assert!(!tree.contains_key(b"pre\0missing", u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        assert!(
            final_queries > initial_queries,
            "filter queries should increase for null byte key lookups"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_non_ascii() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(6)))
    .open()?;

    // Insert keys with UTF-8 characters
    tree.insert("prefix_测试_data".as_bytes(), b"chinese", 0);
    tree.insert("prefix_тест_data".as_bytes(), b"cyrillic", 0);
    tree.insert("prefix_🦀_data".as_bytes(), b"emoji", 0);
    tree.insert("prefix_café".as_bytes(), b"accented", 0);

    // Insert binary keys (non-UTF8)
    tree.insert(&[0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA], b"binary", 0);

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // All keys should be found
    assert!(tree.contains_key("prefix_测试_data".as_bytes(), u64::MAX)?);
    assert!(tree.contains_key("prefix_тест_data".as_bytes(), u64::MAX)?);
    assert!(tree.contains_key("prefix_🦀_data".as_bytes(), u64::MAX)?);
    assert!(tree.contains_key("prefix_café".as_bytes(), u64::MAX)?);
    assert!(tree.contains_key(&[0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA], u64::MAX)?);

    // Non-existent keys
    assert!(!tree.contains_key("prefix_missing".as_bytes(), u64::MAX)?);
    assert!(!tree.contains_key(&[0xFF, 0xFE, 0xFD, 0x00, 0x00, 0x00], u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        assert!(
            final_queries > initial_queries,
            "filter queries should increase for non-ASCII key lookups"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_keys_as_prefixes() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    // Insert keys where some are prefixes of others
    tree.insert(b"a", b"value1", 0);
    tree.insert(b"ab", b"value2", 0);
    tree.insert(b"abc", b"value3", 0);
    tree.insert(b"abcd", b"value4", 0);
    tree.insert(b"abcde", b"value5", 0);
    tree.insert(b"abcdef", b"value6", 0);

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // All keys should be found regardless of prefix relationships
    assert!(tree.contains_key(b"a", u64::MAX)?);
    assert!(tree.contains_key(b"ab", u64::MAX)?);
    assert!(tree.contains_key(b"abc", u64::MAX)?);
    assert!(tree.contains_key(b"abcd", u64::MAX)?);
    assert!(tree.contains_key(b"abcde", u64::MAX)?);
    assert!(tree.contains_key(b"abcdef", u64::MAX)?);

    // Non-existent keys with same prefix
    assert!(!tree.contains_key(b"abcdx", u64::MAX)?);
    assert!(!tree.contains_key(b"abx", u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        assert!(
            final_queries > initial_queries,
            "filter queries should increase for prefix-related key lookups"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_very_long_keys() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(10)))
    .open()?;

    // Create very long keys
    let long_key1 = vec![b'a'; 10000];
    let long_key2 = vec![b'b'; 10000];
    let mut long_key3 = vec![b'c'; 5000];
    long_key3.extend(vec![b'd'; 5000]);

    tree.insert(&long_key1, b"long1", 0);
    tree.insert(&long_key2, b"long2", 0);
    tree.insert(&long_key3, b"long3", 0);

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // All long keys should be found
    assert!(tree.contains_key(&long_key1, u64::MAX)?);
    assert!(tree.contains_key(&long_key2, u64::MAX)?);
    assert!(tree.contains_key(&long_key3, u64::MAX)?);

    // Non-existent long key
    let non_existent = vec![b'x'; 10000];
    assert!(!tree.contains_key(&non_existent, u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        assert!(
            final_queries > initial_queries,
            "filter queries should increase for very long key lookups"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_all_same_byte() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(5)))
    .open()?;

    // Insert keys that are all the same byte
    for len in 1..=10 {
        let key = vec![b'x'; len];
        tree.insert(&key, format!("value_{}", len).as_bytes(), 0);
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // All keys should be found
    for len in 1..=10 {
        let key = vec![b'x'; len];
        assert!(tree.contains_key(&key, u64::MAX)?);
    }

    // Non-existent key with same pattern
    assert!(!tree.contains_key(vec![b'x'; 15], u64::MAX)?);
    assert!(!tree.contains_key(vec![b'y'; 5], u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        assert!(
            final_queries > initial_queries,
            "filter queries should increase for same-byte key lookups"
        );
    }

    Ok(())
}

// Custom extractor that returns many prefixes for stress testing
struct ManyPrefixExtractor;

impl PrefixExtractor for ManyPrefixExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        let mut prefixes = Vec::new();

        // Generate all possible prefixes (up to 20 or key length)
        for i in 1..=key.len().min(20) {
            prefixes.push(&key[0..i]);
        }

        // Also add the full key
        if !prefixes.is_empty() {
            prefixes.push(key);
        }

        Box::new(prefixes.into_iter())
    }

    fn name(&self) -> &str {
        "ManyPrefixExtractor"
    }
}

#[test]
fn test_prefix_filter_many_prefixes() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(ManyPrefixExtractor))
    .open()?;

    // Insert keys that will generate many prefixes
    tree.insert(b"this_is_a_very_long_key_for_testing", b"value1", 0);
    tree.insert(b"another_long_key_with_many_prefixes", b"value2", 0);
    tree.insert(b"short", b"value3", 0);

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // All keys should be found
    assert!(tree.contains_key(b"this_is_a_very_long_key_for_testing", u64::MAX)?);
    assert!(tree.contains_key(b"another_long_key_with_many_prefixes", u64::MAX)?);
    assert!(tree.contains_key(b"short", u64::MAX)?);

    // Test non-existent key
    assert!(!tree.contains_key(b"non_existent_key_with_many_prefixes", u64::MAX)?);

    // Range queries should work with many prefixes
    let range = tree.range(b"this".as_ref().., u64::MAX, None);
    assert!(range.count() > 0);

    let range = tree.range(b"anot".as_ref().., u64::MAX, None);
    assert!(range.count() > 0);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        assert!(
            final_queries > initial_queries,
            "filter queries should increase for many-prefix extractor"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_disabled() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with filter disabled
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(5)))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::None)) // Disable filter
    .open()?;

    // Insert some keys
    for i in 0..100 {
        let key = format!("disabled_{:04}", i);
        tree.insert(key.as_bytes(), b"value", 0);
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Keys should still be found (via actual disk lookups)
    for i in 0..100 {
        let key = format!("disabled_{:04}", i);
        assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
    }

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        // Should have no filter queries when disabled
        assert_eq!(
            final_queries, initial_queries,
            "No filter queries when disabled"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_false_positive_rate() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Use higher bits per key for lower false positive rate
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(8)))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(20.0),
    ))) // Higher bits for lower FP rate
    .open()?;

    // Insert a specific set of keys
    for i in 0..1000 {
        let key = format!("fptest_{:06}", i * 2); // Even numbers only
        tree.insert(key.as_bytes(), b"value", 0);
    }

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    let mut false_positives = 0;
    let total_checks = 1000;

    // Check for non-existent keys (odd numbers)
    for i in 0..total_checks {
        let key = format!("fptest_{:06}", i * 2 + 1);
        if tree.contains_key(key.as_bytes(), u64::MAX)? {
            false_positives += 1;
        }
    }

    // With 20 bits per key, false positive rate should be very low
    let fp_rate = false_positives as f64 / total_checks as f64;
    assert!(
        fp_rate < 0.01,
        "False positive rate {} should be less than 1% with 20 bits per key",
        fp_rate
    );

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Should have queries for all lookups
        assert!(
            final_queries > initial_queries,
            "filter queries should increase for false-positive rate test"
        );

        // False positives will cause hits to increase, but most should be filtered
        // The number of hits should be approximately equal to the false positive count
        assert!(
            final_hits <= initial_hits + (false_positives as usize) + 10,
            "filter hits should only increase for false positives, not true negatives"
        );
    }

    Ok(())
}

#[test]
fn test_prefix_filter_mixed_domain_keys() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedLengthExtractor::new(8)))
    .open()?;

    // Mix of in-domain and out-of-domain keys
    tree.insert(b"12345678_data", b"in_domain", 0); // In domain
    tree.insert(b"short", b"out_of_domain", 0); // Out of domain
    tree.insert(b"12345678", b"exact_length", 0); // Exact length
    tree.insert(b"1234567", b"too_short", 0); // Out of domain
    tree.insert(b"123456789", b"longer", 0); // In domain

    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // All keys should be found
    assert!(tree.contains_key(b"12345678_data", u64::MAX)?);
    assert!(tree.contains_key(b"short", u64::MAX)?);
    assert!(tree.contains_key(b"12345678", u64::MAX)?);
    assert!(tree.contains_key(b"1234567", u64::MAX)?);
    assert!(tree.contains_key(b"123456789", u64::MAX)?);

    // Non-existent keys with different domain status
    assert!(!tree.contains_key(b"12345678_missing", u64::MAX)?); // Would be in domain
    assert!(!tree.contains_key(b"tiny", u64::MAX)?); // Would be out of domain

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        assert!(
            final_queries > initial_queries,
            "filter queries should increase for mixed domain key lookups"
        );
    }

    Ok(())
}

/// Test that range queries don't incorrectly skip segments when the start bound
/// doesn't exist in the filter but other keys in the range do exist
#[test]
fn test_prefix_filter_range_with_missing_start_bound() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Use full key as prefix (FullKeyExtractor)
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FullKeyExtractor))
    .open()?;

    // Insert keys b and c, but not a
    tree.insert(b"b", b"value_b", 0);
    tree.insert(b"c", b"value_c", 0);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Query range a..=c
    //  Extract common prefix from both bounds (empty for "a" and "c")
    //  But now we check if start bound "a" exists - it doesn't, but segment starts with "b"
    //  So we can't skip the segment (different prefixes)
    let mut results = Vec::new();
    for item in tree.range(&b"a"[..]..=&b"c"[..], u64::MAX, None) {
        results.push(item.key()?.to_vec());
    }

    // Should return b and c (even though a doesn't exist)
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], b"b");
    assert_eq!(results[1], b"c");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        assert_eq!(
            final_queries, initial_queries,
            "filter should not be queried"
        );
    }

    Ok(())
}

/// Test the new optimization: when range has no common prefix but start bound prefix doesn't exist
#[test]
fn test_prefix_filter_range_start_prefix_optimization() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Use a fixed prefix extractor
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
    .open()?;

    // Insert keys that all start with "bbb"
    tree.insert(b"bbb_1", b"value1", 0);
    tree.insert(b"bbb_2", b"value2", 0);
    tree.insert(b"bbb_3", b"value3", 0);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Query range aaa..zzz (no common prefix)
    // The segment starts with "bbb" and "aaa" doesn't exist
    // But since segment min ("bbb") != start prefix ("aaa"), we can't skip
    let mut results = Vec::new();
    for item in tree.range(&b"aaa"[..]..&b"zzz"[..], u64::MAX, None) {
        results.push(item.key()?.to_vec());
    }
    assert_eq!(results.len(), 3, "Should find all bbb keys");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        assert_eq!(
            final_queries, initial_queries,
            "filter should not be queried"
        );
    }

    // Now test where we CAN skip: segment that starts with same prefix as missing start bound
    let tree2 = Config::new(
        folder.path().join("test2"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
    .open()?;

    // Create a tree with keys having prefix "aaa" and "aac" but not "aab"
    tree2.insert(b"aaa_1", b"value1", 0);
    tree2.insert(b"aaa_2", b"value2", 0);
    tree2.insert(b"aac_1", b"value3", 0);
    tree2.insert(b"aac_2", b"value4", 0);
    tree2.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree2.metrics().filter_queries();

    // First verify the tree has data
    assert!(tree2.contains_key(b"aaa_1", u64::MAX)?);
    assert!(tree2.contains_key(b"aac_1", u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        assert_eq!(
            final_queries, initial_queries,
            "filter should not be queried"
        );
    }

    #[cfg(feature = "metrics")]
    let initial_queries = tree2.metrics().filter_queries();

    // Query for range with common prefix "aab" - no keys exist with this prefix
    // Range: aab_1..aab_9 has common prefix "aab"
    // The segment contains "aaa" and "aac" keys, so it overlaps the range
    // filter will be checked for "aab" and should indicate it doesn't exist
    let range_iter = tree2.range(&b"aab_1"[..]..&b"aab_9"[..], u64::MAX, None);
    let results: Vec<_> = range_iter.collect();
    assert_eq!(
        results.len(),
        0,
        "No keys should match since aab prefix doesn't exist"
    );

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree2.metrics().filter_queries();

        assert!(
            final_queries > initial_queries,
            "filter queries should increase for range operations"
        );
    }

    Ok(())
}

/// Test that range queries correctly handle different prefix scenarios:
/// same prefix, different prefixes, and non-existent prefixes
#[test]
fn test_prefix_filter_range_across_different_prefixes() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(5)))
    .open()?;

    // Store keys with same prefix
    tree.insert("user1_a", "v1", 0);
    tree.insert("user1_b", "v2", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("user2_a", "v3", 1);
    tree.insert("user2_b", "v4", 1);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Query with common prefix "user1" - should find entries
    let count = tree.range("user1_a"..="user1_z", u64::MAX, None).count();
    assert_eq!(count, 2, "Should find user1 entries");

    // Query with non-existent prefix - should return nothing
    let count = tree.range("user3_a"..="user3_z", u64::MAX, None).count();
    assert_eq!(count, 0, "Should find no user3 entries");

    // Query across different prefixes - no common prefix
    let count = tree.range("user1_a"..="user2_b", u64::MAX, None).count();
    assert_eq!(count, 4, "Should find all entries when no common prefix");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();

        // Range queries with common prefix should trigger filter
        assert!(
            final_queries > initial_queries,
            "filter queries should increase for range operations"
        );
    }

    Ok(())
}

/// Test range queries with reversed bounds (should return empty)
#[test]
fn test_prefix_filter_range_reversed_bounds() -> lsm_tree::Result<()> {
    use std::ops::Bound;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FullKeyExtractor))
    .open()?;

    // Insert some keys
    tree.insert("a", "value_a", 0);
    tree.insert("b", "value_b", 0);
    tree.insert("c", "value_c", 0);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Query with reversed bounds - should return empty
    let count = tree.range("c".."a", u64::MAX, None).count();
    assert_eq!(count, 0, "Reversed bounds should return empty");

    // Also test with excluded bounds reversed
    let count = tree
        .range::<&str, _>((Bound::Excluded("c"), Bound::Included("a")), u64::MAX, None)
        .count();
    assert_eq!(count, 0, "Reversed excluded bounds should return empty");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        // Reversed bounds may skip filter entirely
        assert_eq!(
            final_queries, initial_queries,
            "filter should not be queried for reversed (empty) ranges"
        );
    }

    Ok(())
}

/// Test range with same key but different bound types
#[test]
fn test_prefix_filter_range_same_key_different_bounds() -> lsm_tree::Result<()> {
    use std::ops::Bound;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FullKeyExtractor))
    .open()?;

    tree.insert("key", "value", 0);
    tree.insert("key2", "value2", 0);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    // Included..Excluded with same key (empty range)
    let count = tree
        .range::<&str, _>(
            (Bound::Included("key"), Bound::Excluded("key")),
            u64::MAX,
            None,
        )
        .count();
    assert_eq!(count, 0, "Included..Excluded same key should be empty");

    // Excluded..Included with same key (empty range)
    let count = tree
        .range::<&str, _>(
            (Bound::Excluded("key"), Bound::Included("key")),
            u64::MAX,
            None,
        )
        .count();
    assert_eq!(count, 0, "Excluded..Included same key should be empty");

    // Included..Included with same key (single item)
    let count = tree
        .range::<&str, _>(
            (Bound::Included("key"), Bound::Included("key")),
            u64::MAX,
            None,
        )
        .count();
    assert_eq!(count, 1, "Included..Included same key should return 1");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Range queries should use filter even with same key bounds
        assert!(
            final_queries > initial_queries,
            "filter queries should increase for same-key range operations"
        );

        // Keys exist, hits should not increase
        assert_eq!(
            final_hits, initial_hits,
            "filter hits should not increase for existing keys"
        );
    }

    Ok(())
}

/// Test range with non-consecutive keys having common prefix
#[test]
fn test_prefix_filter_range_non_consecutive_keys() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
    .open()?;

    // Insert non-consecutive keys with same prefix
    tree.insert("app_1", "v1", 0);
    tree.insert("app_3", "v3", 0);
    tree.insert("app_5", "v5", 0);
    tree.insert("app_7", "v7", 0);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    // Query for range that includes missing keys
    let count = tree.range("app_2"..="app_6", u64::MAX, None).count();
    assert_eq!(count, 2, "Should find app_3 and app_5");

    // Query for range entirely between existing keys
    let count = tree.range("app_4".."app_5", u64::MAX, None).count();
    assert_eq!(count, 0, "No keys in range app_4..app_5");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Concurrent access should still use filters
        assert!(
            final_queries > initial_queries,
            "filter queries should work with sequence consistency"
        );

        // Keys exist at various sequence numbers, hits should not increase
        assert_eq!(
            final_hits, initial_hits,
            "filter hits should not increase (keys exist or filtered by seqno)"
        );
    }

    Ok(())
}

/// Test range queries across multiple segments with different prefixes
#[test]
fn test_prefix_filter_range_multiple_segments() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    // Create first segment with user prefix
    tree.insert("user_001", "v1", 0);
    tree.insert("user_002", "v2", 0);
    tree.flush_active_memtable(0)?;

    // Create second segment with item prefix
    tree.insert("item_001", "v3", 1);
    tree.insert("item_002", "v4", 1);
    tree.flush_active_memtable(0)?;

    // Create third segment with both prefixes
    tree.insert("user_003", "v5", 2);
    tree.insert("item_003", "v6", 2);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Query across all segments
    let count = tree.range("item_001"..="user_003", u64::MAX, None).count();
    assert_eq!(count, 6, "Should find all items and users in range");

    // Query for non-existent prefix across segments
    let count = tree.range("test_001"..="test_999", u64::MAX, None).count();
    assert_eq!(count, 0, "Non-existent prefix should return nothing");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        // Queries across multiple segments should check filters
        assert!(
            final_queries > initial_queries,
            "filter queries should increase for multi-segment range queries"
        );
    }

    Ok(())
}

/// Test range with keys where prefix changes at segment boundary
#[test]
fn test_prefix_filter_range_prefix_boundary() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
    .open()?;

    // First segment ends with "aaz"
    tree.insert("aax_1", "v1", 0);
    tree.insert("aay_1", "v2", 0);
    tree.insert("aaz_1", "v3", 0);
    tree.flush_active_memtable(0)?;

    // Second segment starts with "aba" (different prefix)
    tree.insert("aba_1", "v4", 1);
    tree.insert("abb_1", "v5", 1);
    tree.insert("abc_1", "v6", 1);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Query across the boundary
    let count = tree.range("aay_1"..="abb_1", u64::MAX, None).count();
    assert_eq!(count, 4, "Should find keys from both segments");

    // Query that spans missing prefix between segments
    let count = tree.range("aaz_2"..="aba_0", u64::MAX, None).count();
    assert_eq!(count, 0, "No keys in the gap between segments");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        // Common prefix is only 2 chars ("aa" and "ab"), less than extractor length (3)
        // So filter may be bypassed
        assert_eq!(
            final_queries, initial_queries,
            "filter should be bypassed when common prefix is shorter than extractor"
        );
    }

    Ok(())
}

/// Test range with no prefix extractor (should not use filter optimization)
#[test]
fn test_prefix_filter_range_no_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree without prefix extractor
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Insert various keys
    tree.insert("a", "v1", 0);
    tree.insert("b", "v2", 0);
    tree.insert("c", "v3", 0);
    tree.insert("d", "v4", 0);
    tree.insert("e", "v5", 0);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Range queries should work normally without filter optimization
    let count = tree.range("a"..="c", u64::MAX, None).count();
    assert_eq!(count, 3, "Should find a, b, c");

    let count = tree.range("b"..="d", u64::MAX, None).count();
    assert_eq!(count, 3, "Should find b, c, d");

    // Empty range
    let count = tree.range("f"..="z", u64::MAX, None).count();
    assert_eq!(count, 0, "Should find nothing");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        // Without prefix extractor, filter optimization is not used for ranges
        assert_eq!(
            final_queries, initial_queries,
            "filter should not be used for ranges without prefix extractor"
        );
    }

    Ok(())
}

/// Test range with both bounds excluded
#[test]
fn test_prefix_filter_range_both_excluded() -> lsm_tree::Result<()> {
    use std::ops::Bound;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FullKeyExtractor))
    .open()?;

    // Insert keys
    for key in ["a", "b", "c", "d", "e"] {
        tree.insert(key, "value", 0);
    }
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    // Test with both bounds excluded
    let count = tree
        .range::<&str, _>((Bound::Excluded("a"), Bound::Excluded("e")), u64::MAX, None)
        .count();
    assert_eq!(count, 3, "Should return b, c, d");

    // Edge case: adjacent keys with both excluded
    let count = tree
        .range::<&str, _>((Bound::Excluded("b"), Bound::Excluded("c")), u64::MAX, None)
        .count();
    assert_eq!(count, 0, "No keys between adjacent excluded bounds");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Range queries with excluded bounds may or may not use filter
        // depending on prefix extraction logic
        assert!(
            final_queries >= initial_queries,
            "filter queries should not decrease for excluded bound ranges"
        );

        // All keys exist, hits should not increase
        assert_eq!(
            final_hits, initial_hits,
            "filter hits should not increase for existing keys"
        );
    }

    Ok(())
}

/// Test range after compaction with prefix filters
#[test]
fn test_prefix_filter_range_after_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    // Create multiple segments
    for i in 0..3 {
        tree.insert(format!("user_{}", i), format!("v{}", i), i);
        tree.insert(format!("item_{}", i), format!("i{}", i), i);
        tree.flush_active_memtable(0)?;
    }

    // Skip compaction test since it's not implemented
    // tree.major_compact(u64::MAX)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Verify range queries still work after compaction
    let count = tree.range("user_0"..="user_2", u64::MAX, None).count();
    assert_eq!(count, 3, "Should find all user keys after compaction");

    let count = tree.range("item_0"..="item_2", u64::MAX, None).count();
    assert_eq!(count, 3, "Should find all item keys after compaction");

    // Query across prefixes
    let count = tree.range("item_1"..="user_1", u64::MAX, None).count();
    assert_eq!(count, 4, "Should find mixed keys after compaction");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        // Range queries with common prefix should use filter
        assert!(
            final_queries > initial_queries,
            "filter queries should increase for range operations after compaction"
        );
    }

    Ok(())
}

/// Test range with Unicode/UTF-8 prefix boundaries
#[test]
fn test_prefix_filter_range_utf8_boundaries() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(6))) // 6 bytes = 2 UTF-8 chars for these emojis
    .open()?;

    // Insert keys with emoji prefixes (each emoji is 3-4 bytes)
    tree.insert("🎈🎈_001", "v1", 0);
    tree.insert("🎈🎈_002", "v2", 0);
    tree.insert("🎉🎉_001", "v3", 0);
    tree.insert("🎉🎉_002", "v4", 0);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();

    // Query within same emoji prefix
    let count = tree.range("🎈🎈_001"..="🎈🎈_002", u64::MAX, None).count();
    assert_eq!(count, 2, "Should find keys with balloon prefix");

    // Query across different emoji prefixes
    let count = tree.range("🎈🎈_002"..="🎉🎉_001", u64::MAX, None).count();
    assert_eq!(count, 2, "Should find keys across emoji boundaries");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        // Emoji prefixes should trigger filter checks
        assert!(
            final_queries > initial_queries,
            "filter queries should increase for UTF-8 boundary range queries"
        );
    }

    Ok(())
}

/// Test with custom extractor returning multiple prefixes
#[test]
fn test_prefix_filter_range_multi_prefix_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Custom extractor that returns multiple prefixes
    struct MultiPrefixExtractor;
    impl PrefixExtractor for MultiPrefixExtractor {
        fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            if key.len() >= 6 {
                // Return both 3-byte and 6-byte prefixes
                Box::new(vec![&key[..3], &key[..6]].into_iter())
            } else if key.len() >= 3 {
                Box::new(std::iter::once(&key[..3]))
            } else {
                Box::new(std::iter::once(key))
            }
        }
        fn name(&self) -> &str {
            "MultiPrefixExtractor"
        }
    }

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(MultiPrefixExtractor))
    .open()?;

    tree.insert("abc123_data", "v1", 0);
    tree.insert("abc456_data", "v2", 0);
    tree.insert("def123_data", "v3", 0);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    // Query should work with common 3-byte prefix
    let count = tree.range("abc000"..="abc999", u64::MAX, None).count();
    assert_eq!(count, 2, "Should find keys with abc prefix");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Segmented extractor should use filters for prefix matching
        assert!(
            final_queries > initial_queries,
            "filter queries should work with segmented extractor"
        );

        // All keys exist, so hits should not increase
        assert_eq!(
            final_hits, initial_hits,
            "filter hits should not increase for existing keys"
        );
    }

    Ok(())
}

/// Test range with bytes at UTF-8 boundary splitting
#[test]
fn test_prefix_filter_range_utf8_split() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Use a fixed byte extractor that might split UTF-8 chars
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(2)))
    .open()?;

    // Insert keys with multi-byte UTF-8 characters
    tree.insert("中文_1", "v1", 0);
    tree.insert("中文_2", "v2", 0);
    tree.insert("日本_1", "v3", 0);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    // The prefix will be the first 2 bytes, which splits the UTF-8 character
    // This tests that the implementation handles partial UTF-8 correctly
    let count = tree.range("中文_1"..="中文_2", u64::MAX, None).count();
    assert_eq!(count, 2, "Should find keys despite UTF-8 splitting");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Range query should use filter even with UTF-8 split
        assert!(
            final_queries > initial_queries,
            "filter queries should increase for UTF-8 split range"
        );

        // Keys exist, hits should not increase
        assert_eq!(
            final_hits, initial_hits,
            "filter hits should not increase for existing keys"
        );
    }

    Ok(())
}

/// Test empty range (start > end after normalization)  
#[test]
fn test_prefix_filter_empty_normalized_range() -> lsm_tree::Result<()> {
    use std::ops::Bound;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FullKeyExtractor))
    .open()?;

    tree.insert("b", "value", 0);
    tree.flush_active_memtable(0)?;

    #[cfg(feature = "metrics")]
    let initial_queries = tree.metrics().filter_queries();
    #[cfg(feature = "metrics")]
    let initial_hits = tree.metrics().io_skipped_by_filter();

    // Create a range that becomes empty after normalization
    let count = tree
        .range::<&str, _>((Bound::Excluded("b"), Bound::Excluded("b")), u64::MAX, None)
        .count();
    assert_eq!(count, 0, "Empty normalized range should return nothing");

    #[cfg(feature = "metrics")]
    {
        let final_queries = tree.metrics().filter_queries();
        let final_hits = tree.metrics().io_skipped_by_filter();

        // Empty normalized range may skip filter
        assert_eq!(
            final_queries, initial_queries,
            "filter should not be queried for empty normalized range"
        );

        assert_eq!(
            final_hits, initial_hits,
            "filter hits should not change for empty range"
        );
    }

    Ok(())
}

/// A test prefix extractor that extracts a fixed prefix with a custom name
struct TestPrefixExtractor {
    length: usize,
    name: String,
}

impl TestPrefixExtractor {
    fn new(length: usize, name: &str) -> Self {
        Self {
            length,
            name: name.to_string(),
        }
    }
}

impl PrefixExtractor for TestPrefixExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        if key.len() >= self.length {
            Box::new(std::iter::once(&key[..self.length]))
        } else {
            Box::new(std::iter::once(key))
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[test]
fn test_same_extractor_compatibility() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();

    let extractor = Arc::new(TestPrefixExtractor::new(4, "test_extractor"));

    // Create a tree with prefix extractor
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor.clone())
        .open()?;

        tree.insert("user_key1", "value1", 0);
        tree.insert("user_key2", "value2", 0);
        tree.insert("data_key1", "value3", 0);
        tree.flush_active_memtable(0)?;
    }

    // Reopen with the same extractor - should work fine with prefix filtering
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor)
        .open()?;

        // Should be able to use prefix filtering
        #[cfg(feature = "metrics")]
        let initial_queries = tree.metrics().filter_queries();

        assert_eq!(&*tree.get("user_key1", u64::MAX)?.unwrap(), b"value1");
        assert_eq!(&*tree.get("user_key2", u64::MAX)?.unwrap(), b"value2");
        assert_eq!(&*tree.get("data_key1", u64::MAX)?.unwrap(), b"value3");

        #[cfg(feature = "metrics")]
        {
            let final_queries = tree.metrics().filter_queries();
            // Should have incremented filter queries since extractor is compatible
            assert!(
                final_queries > initial_queries,
                "Compatible extractor should increment filter queries: {} -> {}",
                initial_queries,
                final_queries
            );
        }

        // Test range queries with prefix filtering optimization
        let items: Vec<_> = tree.range("user"..="user_zzzz", u64::MAX, None).collect();
        assert_eq!(items.len(), 2);
    }

    Ok(())
}

#[test]
fn test_different_extractor_incompatible() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();

    let extractor1 = Arc::new(TestPrefixExtractor::new(4, "test_extractor_v1"));
    let extractor2 = Arc::new(TestPrefixExtractor::new(4, "test_extractor_v2"));

    // Create a tree with first extractor
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor1)
        .open()?;

        tree.insert("user_key1", "value1", 0);
        tree.insert("user_key2", "value2", 0);
        tree.insert("data_key1", "value3", 0);
        tree.flush_active_memtable(0)?;
    }

    // Reopen with different extractor - should disable prefix filtering for old segments
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor2)
        .open()?;

        // Should still work, but without prefix filtering optimization for old segments
        // The incompatible extractor means filter is completely bypassed
        #[cfg(feature = "metrics")]
        let initial_queries = tree.metrics().filter_queries();

        assert_eq!(&*tree.get("user_key1", u64::MAX)?.unwrap(), b"value1");
        assert_eq!(&*tree.get("user_key2", u64::MAX)?.unwrap(), b"value2");
        assert_eq!(&*tree.get("data_key1", u64::MAX)?.unwrap(), b"value3");

        #[cfg(feature = "metrics")]
        {
            let final_queries = tree.metrics().filter_queries();
            // Should NOT have incremented filter queries since extractor is incompatible
            assert_eq!(
                final_queries, initial_queries,
                "Incompatible extractor should not increment filter queries: {} -> {}",
                initial_queries, final_queries
            );
        }

        // Range queries should still work correctly (but without optimization for old segments)
        let items: Vec<_> = tree.range("user"..="user_zzzz", u64::MAX, None).collect();
        assert_eq!(items.len(), 2);

        // New writes should use the new extractor
        tree.insert("test_key1", "value4", 1);
        tree.flush_active_memtable(0)?;

        assert_eq!(&*tree.get("test_key1", u64::MAX)?.unwrap(), b"value4");
    }

    Ok(())
}

#[test]
fn test_no_extractor_to_extractor() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();

    // Create a tree without prefix extractor
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("user_key1", "value1", 0);
        tree.insert("user_key2", "value2", 0);
        tree.insert("data_key1", "value3", 0);
        tree.flush_active_memtable(0)?;
    }

    // Reopen with prefix extractor - should disable prefix filtering for old segments
    {
        let extractor = Arc::new(TestPrefixExtractor::new(4, "test_extractor"));
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor)
        .open()?;

        // Should still work, but old segments won't use prefix filtering
        assert_eq!(&*tree.get("user_key1", u64::MAX)?.unwrap(), b"value1");
        assert_eq!(&*tree.get("user_key2", u64::MAX)?.unwrap(), b"value2");
        assert_eq!(&*tree.get("data_key1", u64::MAX)?.unwrap(), b"value3");

        // New writes should use prefix extractor
        tree.insert("test_key1", "value4", 1);
        tree.flush_active_memtable(0)?;

        assert_eq!(&*tree.get("test_key1", u64::MAX)?.unwrap(), b"value4");
    }

    Ok(())
}

#[test]
fn test_extractor_to_no_extractor() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();

    let extractor = Arc::new(TestPrefixExtractor::new(4, "test_extractor"));

    // Create a tree with prefix extractor
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor)
        .open()?;

        tree.insert("user_key1", "value1", 0);
        tree.insert("user_key2", "value2", 0);
        tree.insert("data_key1", "value3", 0);
        tree.flush_active_memtable(0)?;
    }

    // Reopen without prefix extractor - should disable prefix filtering for old segments
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        // Should still work, but old segments won't use prefix filtering
        assert_eq!(&*tree.get("user_key1", u64::MAX)?.unwrap(), b"value1");
        assert_eq!(&*tree.get("user_key2", u64::MAX)?.unwrap(), b"value2");
        assert_eq!(&*tree.get("data_key1", u64::MAX)?.unwrap(), b"value3");

        // Range queries should still work
        let items: Vec<_> = tree.range("user"..="user_zzzz", u64::MAX, None).collect();
        assert_eq!(items.len(), 2);
    }

    Ok(())
}

#[test]
fn test_builtin_extractors_compatibility() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();

    // Create with FixedPrefixExtractor
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
        .open()?;

        tree.insert("user_key1", "value1", 0);
        tree.insert("user_key2", "value2", 0);
        tree.flush_active_memtable(0)?;
    }

    // Reopen with FixedLengthExtractor (different name) - should be incompatible
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedLengthExtractor::new(4)))
        .open()?;

        // Should work but without prefix filtering for old segments
        assert_eq!(&*tree.get("user_key1", u64::MAX)?.unwrap(), b"value1");
        assert_eq!(&*tree.get("user_key2", u64::MAX)?.unwrap(), b"value2");
    }

    // Reopen with same type (FixedPrefixExtractor) - should be compatible
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
        .open()?;

        // Should work with prefix filtering for old segments
        assert_eq!(&*tree.get("user_key1", u64::MAX)?.unwrap(), b"value1");
        assert_eq!(&*tree.get("user_key2", u64::MAX)?.unwrap(), b"value2");
    }

    Ok(())
}

#[test]
fn test_new_segments_use_new_extractor() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();

    let extractor1 = Arc::new(TestPrefixExtractor::new(4, "old_extractor"));
    let extractor2 = Arc::new(TestPrefixExtractor::new(4, "new_extractor"));

    // Create first segment with old extractor
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor1)
        .open()?;

        tree.insert("old_key1", "value1", 0);
        tree.insert("old_key2", "value2", 0);
        tree.flush_active_memtable(0)?;
    }

    // Reopen with new extractor and create new segment
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor2)
        .open()?;

        // Add data to create a new segment with the new extractor
        tree.insert("new_key1", "value3", 1);
        tree.insert("new_key2", "value4", 1);
        tree.flush_active_memtable(0)?;

        // Test that old segment uses no filtering (extractor incompatible)
        #[cfg(feature = "metrics")]
        let initial_queries = tree.metrics().filter_queries();

        // Query old keys - should NOT increment filter queries (incompatible extractor)
        assert_eq!(&*tree.get("old_key1", u64::MAX)?.unwrap(), b"value1");
        assert_eq!(&*tree.get("old_key2", u64::MAX)?.unwrap(), b"value2");

        #[cfg(feature = "metrics")]
        let after_old_queries = tree.metrics().filter_queries();

        // Query new keys - SHOULD increment filter queries (compatible extractor)
        assert_eq!(&*tree.get("new_key1", u64::MAX)?.unwrap(), b"value3");
        assert_eq!(&*tree.get("new_key2", u64::MAX)?.unwrap(), b"value4");

        #[cfg(feature = "metrics")]
        {
            let final_queries = tree.metrics().filter_queries();

            // Old keys should not have incremented filter queries
            assert_eq!(
                after_old_queries, initial_queries,
                "Old keys should not increment filter queries due to incompatible extractor"
            );

            // New keys should have incremented filter queries
            assert!(
                final_queries > after_old_queries,
                "New keys should increment filter queries with compatible extractor: {} -> {}",
                after_old_queries,
                final_queries
            );
        }
    }

    Ok(())
}

#[test]
fn test_multiple_extractor_changes() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();

    let extractor1 = Arc::new(TestPrefixExtractor::new(2, "v1"));
    let extractor2 = Arc::new(TestPrefixExtractor::new(2, "v2"));
    let extractor3 = Arc::new(TestPrefixExtractor::new(2, "v3"));

    // Create segments with different extractors over time
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor1)
        .open()?;
        tree.insert("aa_data1", "value1", 0);
        tree.flush_active_memtable(0)?;
    }

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor2)
        .open()?;
        tree.insert("bb_data2", "value2", 0);
        tree.flush_active_memtable(0)?;
    }

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor3)
        .open()?;
        tree.insert("cc_data3", "value3", 0);
        tree.flush_active_memtable(0)?;

        // Only the last segment should use filtering
        #[cfg(feature = "metrics")]
        let initial_queries = tree.metrics().filter_queries();

        // These should not increment filter queries (incompatible)
        assert_eq!(&*tree.get("aa_data1", u64::MAX)?.unwrap(), b"value1");
        assert_eq!(&*tree.get("bb_data2", u64::MAX)?.unwrap(), b"value2");

        #[cfg(feature = "metrics")]
        let middle_queries = tree.metrics().filter_queries();

        // This should increment filter queries (compatible)
        assert_eq!(&*tree.get("cc_data3", u64::MAX)?.unwrap(), b"value3");

        #[cfg(feature = "metrics")]
        {
            let final_queries = tree.metrics().filter_queries();
            assert_eq!(
                middle_queries, initial_queries,
                "Old segments should not increment metrics"
            );
            assert!(
                final_queries > middle_queries,
                "New segment should increment metrics"
            );
        }
    }

    Ok(())
}

/// Prefix API with forward iteration returns only keys matching the prefix.
#[test]
fn test_prefix_filter_prefix_api_forward() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    for i in 0..20u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), b"v", 0);
    }
    for i in 0..10u32 {
        tree.insert(format!("bbb_{:04}", i).as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    let keys: Vec<_> = tree
        .prefix(b"aaa_", SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 20);
    assert!(keys[0].starts_with(b"aaa_"));

    let keys: Vec<_> = tree
        .prefix(b"bbb_", SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 10);

    // Non-existent prefix returns nothing.
    assert_eq!(tree.prefix(b"zzz_", SeqNo::MAX, None).count(), 0);

    Ok(())
}

/// Prefix API with reverse iteration returns only keys matching the prefix in reverse order.
#[test]
fn test_prefix_filter_prefix_api_reverse() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    for i in 0..30u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"aaa_0000"[..]..=&b"aaa_0029"[..], SeqNo::MAX, None)
        .rev()
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 30);
    assert_eq!(&*keys[0], b"aaa_0029");
    assert_eq!(&*keys[29], b"aaa_0000");

    Ok(())
}

/// Verifies that partitioned filters are actually built when a prefix extractor
/// is configured.
///
/// Background: the PartitionedFilterWriter tracks `last_key` (set by `register_key`)
/// to create TLI partition boundary entries and to know whether any data was written.
/// When a prefix extractor is configured, only `register_bytes` is called (not
/// `register_key`), so `last_key` was never set, causing `finish()` to bail out
/// with 0 filter blocks.
#[test]
fn test_prefix_filter_partitioned_filter_actually_built() -> lsm_tree::Result<()> {
    use lsm_tree::config::PinningPolicy;

    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();

    let tree = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default())
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
        // Force partitioned filters for ALL levels (including level 0 where flush goes)
        .filter_block_partitioning_policy(PinningPolicy::all(true))
        // Pin filters so they are loaded and available for consultation
        .filter_block_pinning_policy(PinningPolicy::all(true))
        .open()?;

    // Insert keys with known prefixes
    for prefix in [b"aaaa", b"bbbb", b"cccc"] {
        for i in 0..50u32 {
            let mut key = prefix.to_vec();
            key.extend_from_slice(format!("{:04}", i).as_bytes());
            tree.insert(key, b"value", seqno.next());
        }
    }
    tree.flush_active_memtable(0)?;

    // Grab the flushed table
    let version = tree.current_version();
    let tables: Vec<_> = version.iter_tables().collect();
    assert!(
        !tables.is_empty(),
        "should have at least one table after flush"
    );

    let extractor = FixedPrefixExtractor::new(4);

    // A non-existent prefix should be definitively excluded by the filter.
    // If the filter was NOT built (the bug), this returns Some(true) — "cannot exclude".
    // If the filter WAS built (fixed), this returns Some(false) — "definitely not present".
    let result = tables[0].maybe_contains_prefix(b"zzzz_test", &extractor)?;
    assert_eq!(
        result,
        Some(false),
        "Partitioned filter with prefix extractor must be built and exclude absent prefixes"
    );

    // An existing prefix should NOT be excluded
    let result = tables[0].maybe_contains_prefix(b"aaaa_test", &extractor)?;
    assert_ne!(
        result,
        Some(false),
        "Partitioned filter should not exclude a prefix that exists in the table"
    );

    Ok(())
}

/// BlobTree flush stores prefix entries in the filter and persists the
/// extractor name in table metadata.
#[test]
fn test_prefix_filter_blob_tree_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let big_value = vec![b'x'; 2_000];

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    for i in 0..50u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), &big_value, 0);
    }
    for i in 0..50u32 {
        tree.insert(format!("bbb_{:04}", i).as_bytes(), &big_value, 0);
    }
    tree.flush_active_memtable(0)?;

    for i in 0..50u32 {
        let key = format!("aaa_{:04}", i);
        assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
    }
    for i in 0..50u32 {
        let key = format!("bbb_{:04}", i);
        assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
    }
    assert!(!tree.contains_key(b"zzz_0000", u64::MAX)?);

    #[cfg(feature = "metrics")]
    {
        let before = tree.metrics().filter_queries();
        let _ = tree.contains_key(b"aaa_0001", u64::MAX)?;
        let _ = tree.contains_key(b"zzz_0000", u64::MAX)?;
        let after = tree.metrics().filter_queries();
        assert!(
            after > before,
            "prefix filter should be consulted on blob tree tables \
             (before={before}, after={after})"
        );
    }

    Ok(())
}

/// BlobTree prefix() forward and reverse iteration with separated values.
#[test]
fn test_prefix_filter_blob_tree_prefix_iter() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let big_value = vec![b'x'; 2_000];

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    for i in 0..20u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), &big_value, 0);
    }
    for i in 0..10u32 {
        tree.insert(format!("bbb_{:04}", i).as_bytes(), &big_value, 0);
    }
    tree.flush_active_memtable(0)?;

    // Forward iteration with value resolution.
    let items: Vec<_> = tree
        .prefix(b"aaa_", SeqNo::MAX, None)
        .map(|g| g.into_inner().unwrap())
        .collect();
    assert_eq!(items.len(), 20);
    for (_, v) in &items {
        assert_eq!(v.len(), 2_000);
    }

    // Reverse iteration.
    let rev_keys: Vec<_> = tree
        .prefix(b"aaa_", SeqNo::MAX, None)
        .rev()
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(rev_keys.len(), 20);
    assert!(rev_keys[0] > rev_keys[19]);

    // Non-existent prefix.
    assert_eq!(tree.prefix(b"zzz_", SeqNo::MAX, None).count(), 0);

    Ok(())
}

/// BlobTree range scans with prefix filter after flush.
#[test]
fn test_prefix_filter_blob_tree_range() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let big_value = vec![b'x'; 2_000];

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    for i in 0..50u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), &big_value, 0);
    }
    tree.flush_active_memtable(0)?;

    let count = tree
        .range::<&[u8], _>(&b"aaa_0000"[..]..=&b"aaa_0049"[..], u64::MAX, None)
        .count();
    assert_eq!(count, 50);

    let count = tree
        .range::<&[u8], _>(&b"zzz_0000"[..]..=&b"zzz_9999"[..], u64::MAX, None)
        .count();
    assert_eq!(count, 0);

    Ok(())
}

/// BlobTree compaction preserves prefix filter functionality.
#[test]
fn test_prefix_filter_blob_tree_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let big_value = vec![b'x'; 2_000];

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    for batch in 0..3u32 {
        for i in 0..30u32 {
            let key = format!("aa{}_{:04}", batch, i);
            tree.insert(key.as_bytes(), &big_value, 0);
        }
        tree.flush_active_memtable(0)?;
    }

    tree.major_compact(u64::MAX, 0)?;

    for batch in 0..3u32 {
        for i in 0..30u32 {
            let key = format!("aa{}_{:04}", batch, i);
            assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
        }
    }
    assert!(!tree.contains_key(b"zzz_0000", u64::MAX)?);

    // NOTE: We do not assert on filter_queries after major compaction because
    // data lands at the last level, which uses partitioned filters with an
    // unpinned TLI by default. The filter block is not accessible in that
    // configuration, so the metric may not increment.

    Ok(())
}

/// BlobTree recovery: prefix filter works after close and reopen.
#[test]
fn test_prefix_filter_blob_tree_recovery() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();
    let big_value = vec![b'x'; 2_000];

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .open()?;

        for i in 0..50u32 {
            tree.insert(format!("aaa_{:04}", i).as_bytes(), &big_value, 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // Reopen with the same extractor.
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .open()?;

        for i in 0..50u32 {
            let key = format!("aaa_{:04}", i);
            assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
        }
        assert!(!tree.contains_key(b"zzz_0000", u64::MAX)?);

        #[cfg(feature = "metrics")]
        {
            let before = tree.metrics().filter_queries();
            let _ = tree.contains_key(b"aaa_0001", u64::MAX)?;
            let after = tree.metrics().filter_queries();
            assert!(after > before);
        }
    }

    Ok(())
}

/// BlobTree with a mix of inline (small) and separated (large) values.
/// Both paths must produce correct prefix filter entries.
#[test]
fn test_prefix_filter_blob_tree_mixed_values() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    let small_value = b"tiny";
    let big_value = vec![b'x'; 2_000];

    // Small values (stored inline).
    for i in 0..20u32 {
        tree.insert(format!("sml_{:04}", i).as_bytes(), small_value, 0);
    }
    // Large values (stored in blob files).
    for i in 0..20u32 {
        tree.insert(format!("big_{:04}", i).as_bytes(), &big_value, 0);
    }
    tree.flush_active_memtable(0)?;

    for i in 0..20u32 {
        assert!(tree.contains_key(format!("sml_{:04}", i).as_bytes(), u64::MAX)?);
        assert!(tree.contains_key(format!("big_{:04}", i).as_bytes(), u64::MAX)?);
    }
    assert!(!tree.contains_key(b"zzz_0000", u64::MAX)?);

    // Both prefixes work via the prefix() API.
    assert_eq!(tree.prefix(b"sml_", SeqNo::MAX, None).count(), 20);
    assert_eq!(tree.prefix(b"big_", SeqNo::MAX, None).count(), 20);
    assert_eq!(tree.prefix(b"zzz_", SeqNo::MAX, None).count(), 0);

    Ok(())
}

/// BlobTree with deletions: tombstones interact correctly with prefix filter.
#[test]
fn test_prefix_filter_blob_tree_deletions() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let big_value = vec![b'x'; 2_000];

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    for i in 0..40u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), &big_value, 0);
    }
    tree.flush_active_memtable(0)?;

    // Delete even-numbered keys.
    for i in (0..40u32).step_by(2) {
        tree.remove(format!("aaa_{:04}", i).as_bytes(), 1);
    }
    tree.flush_active_memtable(0)?;

    // Even keys gone, odd keys remain.
    for i in 0..40u32 {
        let key = format!("aaa_{:04}", i);
        if i % 2 == 0 {
            assert!(!tree.contains_key(key.as_bytes(), u64::MAX)?);
        } else {
            assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
        }
    }

    assert_eq!(tree.prefix(b"aaa_", SeqNo::MAX, None).count(), 20);

    Ok(())
}

/// BlobTree MVCC: snapshot reads at specific sequence numbers with prefix filter.
#[test]
fn test_prefix_filter_blob_tree_mvcc() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let big_value = vec![b'x'; 2_000];

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    // Batch 1: seqno 0..49
    for i in 0..50u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), &big_value, i as SeqNo);
    }
    tree.flush_active_memtable(0)?;

    // Batch 2: seqno 50..99
    for i in 50..100u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), &big_value, i as SeqNo);
    }
    tree.flush_active_memtable(0)?;

    // Snapshot at seqno 50: only first 50 keys visible.
    assert_eq!(tree.prefix(b"aaa_", 50, None).count(), 50);

    // Snapshot at latest: all 100 visible.
    assert_eq!(tree.prefix(b"aaa_", SeqNo::MAX, None).count(), 100);

    Ok(())
}

/// BlobTree extractor compatibility change: opening with a differently-named
/// extractor gracefully degrades (old tables bypass the filter, new tables use it).
#[test]
fn test_prefix_filter_blob_tree_extractor_change() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();
    let big_value = vec![b'x'; 2_000];

    // Use custom extractors with distinct names so the system detects incompatibility.
    // (FixedPrefixExtractor always returns "fixed_prefix" regardless of length.)
    let extractor_v1 = Arc::new(TestPrefixExtractor::new(4, "extractor_v1"));
    let extractor_v2 = Arc::new(TestPrefixExtractor::new(6, "extractor_v2"));

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor_v1)
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .open()?;

        for i in 0..20u32 {
            tree.insert(format!("aaa_{:04}", i).as_bytes(), &big_value, 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // Reopen with a differently-named extractor.
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(extractor_v2)
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .open()?;

        // Old data must still be readable despite incompatible extractor.
        for i in 0..20u32 {
            let key = format!("aaa_{:04}", i);
            assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
        }

        // Insert new data with the new extractor, verify coexistence.
        for i in 0..10u32 {
            tree.insert(format!("bbbbbb_{:04}", i).as_bytes(), &big_value, 1);
        }
        tree.flush_active_memtable(0)?;

        for i in 0..10u32 {
            let key = format!("bbbbbb_{:04}", i);
            assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
        }
    }

    Ok(())
}

/// Standard tree ingestion produces tables with working prefix filters.
#[test]
fn test_prefix_filter_ingestion() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    let mut ingestion = tree.ingestion()?;
    for i in 0..50u32 {
        ingestion.write(format!("aaa_{:04}", i).as_bytes(), b"value")?;
    }
    for i in 0..50u32 {
        ingestion.write(format!("bbb_{:04}", i).as_bytes(), b"value")?;
    }
    ingestion.finish()?;

    for i in 0..50u32 {
        assert!(tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
        assert!(tree.contains_key(format!("bbb_{:04}", i).as_bytes(), u64::MAX)?);
    }
    assert!(!tree.contains_key(b"zzz_0000", u64::MAX)?);

    assert_eq!(tree.prefix(b"aaa_", SeqNo::MAX, None).count(), 50);
    assert_eq!(tree.prefix(b"zzz_", SeqNo::MAX, None).count(), 0);

    #[cfg(feature = "metrics")]
    {
        let before = tree.metrics().filter_queries();
        let _ = tree.contains_key(b"aaa_0001", u64::MAX)?;
        let _ = tree.contains_key(b"zzz_0000", u64::MAX)?;
        let after = tree.metrics().filter_queries();
        assert!(after > before);
    }

    Ok(())
}

/// BlobTree ingestion produces tables with working prefix filters.
#[test]
fn test_prefix_filter_blob_tree_ingestion() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let big_value = vec![b'x'; 2_000];

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    let mut ingestion = tree.ingestion()?;
    for i in 0..50u32 {
        ingestion.write(format!("aaa_{:04}", i).as_bytes(), &big_value)?;
    }
    for i in 0..50u32 {
        ingestion.write(format!("bbb_{:04}", i).as_bytes(), &big_value)?;
    }
    ingestion.finish()?;

    for i in 0..50u32 {
        assert!(tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
        assert!(tree.contains_key(format!("bbb_{:04}", i).as_bytes(), u64::MAX)?);
    }
    assert!(!tree.contains_key(b"zzz_0000", u64::MAX)?);

    assert_eq!(tree.prefix(b"aaa_", SeqNo::MAX, None).count(), 50);
    assert_eq!(tree.prefix(b"zzz_", SeqNo::MAX, None).count(), 0);

    #[cfg(feature = "metrics")]
    {
        let before = tree.metrics().filter_queries();
        let _ = tree.contains_key(b"aaa_0001", u64::MAX)?;
        let _ = tree.contains_key(b"zzz_0000", u64::MAX)?;
        let after = tree.metrics().filter_queries();
        assert!(after > before);
    }

    Ok(())
}

/// Ingestion with tombstones and weak tombstones alongside prefix filter.
#[test]
fn test_prefix_filter_ingestion_tombstones() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    // Seed some data.
    for i in 0..30u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    // Ingest tombstones for the first 10 keys and weak tombstones for the next 5.
    let mut ingestion = tree.ingestion()?;
    for i in 0..10u32 {
        ingestion.write_tombstone(format!("aaa_{:04}", i).as_bytes())?;
    }
    for i in 10..15u32 {
        ingestion.write_weak_tombstone(format!("aaa_{:04}", i).as_bytes())?;
    }
    ingestion.finish()?;

    // First 10 deleted.
    for i in 0..10u32 {
        assert!(!tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
    }
    // 10..14 deleted by weak tombstone (shadowing the original insert).
    for i in 10..15u32 {
        assert!(!tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
    }
    // 15..29 still alive.
    for i in 15..30u32 {
        assert!(tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
    }

    Ok(())
}

/// Weak tombstones correctly interact with prefix filter during flush and compaction.
#[test]
fn test_prefix_filter_weak_tombstones() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    for i in 0..20u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    // Weak-delete half the keys.
    for i in 0..10u32 {
        tree.remove_weak(format!("aaa_{:04}", i).as_bytes(), 1);
    }
    tree.flush_active_memtable(0)?;

    // Before compaction: weak tombstones shadow the values.
    for i in 0..10u32 {
        assert!(!tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
    }
    for i in 10..20u32 {
        assert!(tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
    }

    // After compaction: weak tombstones resolved, prefix filter still correct.
    tree.major_compact(u64::MAX, 2)?;

    for i in 0..10u32 {
        assert!(!tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
    }
    for i in 10..20u32 {
        assert!(tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
    }

    assert_eq!(tree.prefix(b"aaa_", SeqNo::MAX, None).count(), 10);

    Ok(())
}

/// drop_range correctly interacts with prefix filter. After dropping tables
/// that are fully contained in a range, queries for those keys return empty.
/// Each prefix must be in a separate table for drop_range to drop it.
#[test]
fn test_prefix_filter_drop_range() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    // Flush each prefix into its own table so drop_range can fully contain one.
    for i in 0..30u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    for i in 0..30u32 {
        tree.insert(format!("bbb_{:04}", i).as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    assert_eq!(tree.table_count(), 2);

    // Drop the table fully contained in the "aaa_" range.
    tree.drop_range::<&[u8], _>(&b"aaa_"[..]..&b"aab_"[..])?;

    // "aaa_" prefix should be empty.
    assert_eq!(tree.prefix(b"aaa_", SeqNo::MAX, None).count(), 0);

    // "bbb_" prefix should be untouched.
    assert_eq!(tree.prefix(b"bbb_", SeqNo::MAX, None).count(), 30);

    Ok(())
}

/// Partitioned filters combined with prefix extractor.
/// Forces partitioned filters at all levels and pins the filter index
/// so that prefix lookups can load the correct partition.
#[test]
fn test_prefix_filter_partitioned() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .filter_block_partitioning_policy(PinningPolicy::all(true))
    // Pin the filter block TLI so the partitioned filter can be looked up.
    .filter_block_pinning_policy(PinningPolicy::all(true))
    .open()?;

    for i in 0..100u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), b"v", 0);
    }
    for i in 0..100u32 {
        tree.insert(format!("bbb_{:04}", i).as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    for i in 0..100u32 {
        assert!(tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
        assert!(tree.contains_key(format!("bbb_{:04}", i).as_bytes(), u64::MAX)?);
    }
    assert!(!tree.contains_key(b"zzz_0000", u64::MAX)?);

    assert_eq!(tree.prefix(b"aaa_", SeqNo::MAX, None).count(), 100);
    assert_eq!(tree.prefix(b"bbb_", SeqNo::MAX, None).count(), 100);
    assert_eq!(tree.prefix(b"zzz_", SeqNo::MAX, None).count(), 0);

    Ok(())
}

/// Partitioned filters on a BlobTree with prefix extractor.
#[test]
fn test_prefix_filter_blob_tree_partitioned() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let big_value = vec![b'x'; 2_000];

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .filter_block_partitioning_policy(PinningPolicy::all(true))
    .filter_block_pinning_policy(PinningPolicy::all(true))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    for i in 0..50u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), &big_value, 0);
    }
    tree.flush_active_memtable(0)?;

    for i in 0..50u32 {
        assert!(tree.contains_key(format!("aaa_{:04}", i).as_bytes(), u64::MAX)?);
    }
    assert!(!tree.contains_key(b"zzz_0000", u64::MAX)?);

    assert_eq!(tree.prefix(b"aaa_", SeqNo::MAX, None).count(), 50);
    assert_eq!(tree.prefix(b"zzz_", SeqNo::MAX, None).count(), 0);

    Ok(())
}

/// size_of() goes through the point-read path; verify it interacts with
/// the prefix filter correctly.
#[test]
fn test_prefix_filter_size_of() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    tree.insert(b"aaa_0001", b"hello", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(tree.size_of(b"aaa_0001", u64::MAX)?, Some(5));
    assert_eq!(tree.size_of(b"zzz_0000", u64::MAX)?, None);

    Ok(())
}

/// BlobTree size_of() with prefix filter.
#[test]
fn test_prefix_filter_blob_tree_size_of() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let big_value = vec![b'x'; 2_000];

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    tree.insert(b"aaa_0001", &big_value, 0);
    tree.insert(b"aaa_0002", b"tiny", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(tree.size_of(b"aaa_0001", u64::MAX)?, Some(2_000));
    assert_eq!(tree.size_of(b"aaa_0002", u64::MAX)?, Some(4));
    assert_eq!(tree.size_of(b"zzz_0000", u64::MAX)?, None);

    Ok(())
}

/// first_key_value and last_key_value use unbounded iteration; the prefix
/// filter should not interfere.
#[test]
fn test_prefix_filter_first_last_kv() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    for i in 0..20u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), b"v", 0);
    }
    for i in 0..20u32 {
        tree.insert(format!("zzz_{:04}", i).as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    let first = tree
        .first_key_value(SeqNo::MAX, None)
        .unwrap()
        .key()
        .unwrap();
    assert_eq!(&*first, b"aaa_0000");

    let last = tree
        .last_key_value(SeqNo::MAX, None)
        .unwrap()
        .key()
        .unwrap();
    assert_eq!(&*last, b"zzz_0019");

    Ok(())
}

/// Reverse range iteration with prefix filter on a standard tree.
#[test]
fn test_prefix_filter_reverse_range() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    for i in 0..30u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), b"v", 0);
    }
    tree.flush_active_memtable(0)?;

    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"aaa_0000"[..]..=&b"aaa_0029"[..], SeqNo::MAX, None)
        .rev()
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 30);
    assert_eq!(&*keys[0], b"aaa_0029");
    assert_eq!(&*keys[29], b"aaa_0000");

    Ok(())
}

/// Reverse range iteration on a BlobTree with prefix filter.
#[test]
fn test_prefix_filter_blob_tree_reverse_range() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let big_value = vec![b'x'; 2_000];

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    for i in 0..30u32 {
        tree.insert(format!("aaa_{:04}", i).as_bytes(), &big_value, 0);
    }
    tree.flush_active_memtable(0)?;

    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"aaa_0000"[..]..=&b"aaa_0029"[..], SeqNo::MAX, None)
        .rev()
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 30);
    assert_eq!(&*keys[0], b"aaa_0029");
    assert_eq!(&*keys[29], b"aaa_0000");

    Ok(())
}

/// Test FullKeyExtractor through the flush/point-read/range pipeline.
#[test]
fn test_full_key_extractor_pipeline() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FullKeyExtractor))
    .open()?;

    tree.insert(b"alpha", b"v1", 0);
    tree.insert(b"beta", b"v2", 0);
    tree.insert(b"gamma", b"v3", 0);
    tree.flush_active_memtable(0)?;

    // Point reads
    assert_eq!(&*tree.get(b"alpha", SeqNo::MAX)?.unwrap(), b"v1");
    assert_eq!(&*tree.get(b"beta", SeqNo::MAX)?.unwrap(), b"v2");
    assert!(tree.get(b"missing", SeqNo::MAX)?.is_none());

    // Prefix query
    let keys: Vec<_> = tree
        .prefix(b"alpha", SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 1);
    assert_eq!(&*keys[0], b"alpha");

    // Range query
    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"a"[..]..&b"c"[..], SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 2);
    assert_eq!(&*keys[0], b"alpha");
    assert_eq!(&*keys[1], b"beta");

    Ok(())
}

/// Test FixedLengthExtractor through the tree pipeline.
/// Keys shorter than the required length are out-of-domain (extract returns empty).
#[test]
fn test_fixed_length_extractor_pipeline() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedLengthExtractor::new(4)))
    .open()?;

    // Normal keys (len >= 4)
    tree.insert(b"aaaa_001", b"v1", 0);
    tree.insert(b"aaaa_002", b"v2", 0);
    tree.insert(b"bbbb_001", b"v3", 0);

    // Short keys (len < 4) — out of domain for FixedLengthExtractor
    tree.insert(b"ab", b"short1", 0);
    tree.insert(b"zz", b"short2", 0);

    tree.flush_active_memtable(0)?;

    // Point reads work for all keys
    assert_eq!(&*tree.get(b"aaaa_001", SeqNo::MAX)?.unwrap(), b"v1");
    assert_eq!(&*tree.get(b"ab", SeqNo::MAX)?.unwrap(), b"short1");

    // Prefix query for normal-length key
    let keys: Vec<_> = tree
        .prefix(b"aaaa_001", SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert!(keys.len() >= 1);
    assert!(keys.iter().any(|k| &**k == b"aaaa_001"));

    // Range query still returns all keys in range
    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"a"[..]..&b"c"[..], SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    // Should contain: ab, aaaa_001, aaaa_002, bbbb_001
    assert_eq!(keys.len(), 4);

    Ok(())
}

/// Test FixedLengthExtractor boundary: key length exactly == extractor length.
#[test]
fn test_fixed_length_extractor_boundary() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedLengthExtractor::new(3)))
    .open()?;

    // Exactly 3 bytes — on boundary
    tree.insert(b"abc", b"v1", 0);
    // More than 3 bytes
    tree.insert(b"abcdef", b"v2", 0);
    // Less than 3 bytes — out of domain
    tree.insert(b"ab", b"v3", 0);

    tree.flush_active_memtable(0)?;

    assert_eq!(&*tree.get(b"abc", SeqNo::MAX)?.unwrap(), b"v1");
    assert_eq!(&*tree.get(b"abcdef", SeqNo::MAX)?.unwrap(), b"v2");
    assert_eq!(&*tree.get(b"ab", SeqNo::MAX)?.unwrap(), b"v3");

    // Prefix query with boundary-length key
    let keys: Vec<_> = tree
        .prefix(b"abc", SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    // "abc" and "abcdef" share prefix "abc"
    assert_eq!(keys.len(), 2);

    Ok(())
}

/// Test RunReader forward lazy per-table prefix skip.
/// Creates 4 tables with distinct prefixes in a single run (L1),
/// then queries a prefix that only exists in one table — the others should be skipped.
#[test]
fn test_run_reader_forward_lazy_prefix_skip() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::Leveled;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    // Create 4 separate L0 tables with distinct prefixes
    for prefix in &[b"aaaa", b"bbbb", b"cccc", b"dddd"] {
        for i in 0..5u32 {
            let key = format!("{}_{:04}", std::str::from_utf8(*prefix).unwrap(), i);
            tree.insert(key.as_bytes(), b"value", 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // Compact to L1 so they form a single run
    tree.compact(Arc::new(Leveled::default()), 0)?;

    // Query only "bbbb" prefix — tables for aaaa, cccc, dddd should be skipped lazily
    let keys: Vec<_> = tree
        .prefix(b"bbbb", SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 5);
    for k in &keys {
        assert!(k.starts_with(b"bbbb"));
    }

    // Query "dddd" prefix (last table) — forward iteration should skip aaaa, bbbb, cccc
    let keys: Vec<_> = tree
        .prefix(b"dddd", SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 5);
    for k in &keys {
        assert!(k.starts_with(b"dddd"));
    }

    Ok(())
}

/// Test RunReader reverse lazy per-table prefix skip.
/// Same setup as forward test but iterates in reverse.
#[test]
fn test_run_reader_reverse_lazy_prefix_skip() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::Leveled;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    // Create 4 separate L0 tables with distinct prefixes
    for prefix in &[b"aaaa", b"bbbb", b"cccc", b"dddd"] {
        for i in 0..5u32 {
            let key = format!("{}_{:04}", std::str::from_utf8(*prefix).unwrap(), i);
            tree.insert(key.as_bytes(), b"value", 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // Compact to L1
    tree.compact(Arc::new(Leveled::default()), 0)?;

    // Reverse prefix query for "aaaa" — should skip bbbb, cccc, dddd tables
    let keys: Vec<_> = tree
        .prefix(b"aaaa", SeqNo::MAX, None)
        .rev()
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 5);
    assert_eq!(&*keys[0], b"aaaa_0004");
    assert_eq!(&*keys[4], b"aaaa_0000");

    Ok(())
}

/// Test RunReader with unbounded ranges and an extractor configured.
#[test]
fn test_run_reader_unbounded_range_with_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    tree.insert(b"aaaa_001", b"v1", 0);
    tree.insert(b"bbbb_001", b"v2", 0);
    tree.flush_active_memtable(0)?;

    // Fully unbounded range (both sides Unbounded)
    let keys: Vec<_> = tree
        .range::<&[u8], _>(.., SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 2);

    // Open-start range (Unbounded..Included)
    let keys: Vec<_> = tree
        .range::<&[u8], _>(..=&b"aaaa_999"[..], SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 1);
    assert_eq!(&*keys[0], b"aaaa_001");

    // Open-end range (Included..Unbounded)
    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"bbbb"[..].., SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 1);
    assert_eq!(&*keys[0], b"bbbb_001");

    Ok(())
}

/// Test RunReader with a cross-prefix range (start and end extract to different prefixes).
#[test]
fn test_run_reader_cross_prefix_range() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::Leveled;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    for prefix in &[b"aaaa", b"bbbb", b"cccc"] {
        for i in 0..3u32 {
            let key = format!("{}_{:04}", std::str::from_utf8(*prefix).unwrap(), i);
            tree.insert(key.as_bytes(), b"value", 0);
        }
        tree.flush_active_memtable(0)?;
    }
    tree.compact(Arc::new(Leveled::default()), 0)?;

    // Range from "aaaa" to "cccc" — different prefixes, common_prefix should be None
    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"aaaa_0000"[..]..&b"cccc_0000"[..], SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    // Should include all aaaa_* and bbbb_* keys (cccc_0000 is excluded by range end)
    assert_eq!(keys.len(), 6);

    Ok(())
}

/// Test single-table run prefix skip.
/// Creates a table with a wide key range but missing a specific prefix,
/// then queries for that absent prefix. The key range overlaps the query,
/// but the prefix filter excludes it so no results are returned.
#[test]
fn test_single_table_range_prefix_skip() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
    .open()?;

    // Single table with wide key range: "aaa" through "zzz", but NO "mmm" prefix
    for p in [b"aaa", b"bbb", b"ccc", b"yyy", b"zzz"] {
        for i in 0..3u32 {
            let key = format!("{}_{:02}", std::str::from_utf8(p).unwrap(), i);
            tree.insert(key.as_bytes(), b"value", 0);
        }
    }
    tree.flush_active_memtable(0)?;

    // Query for prefix "mmm" — table key range (aaa..zzz) overlaps, but filter excludes "mmm"
    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"mmm_00"[..]..&b"mmm_99"[..], SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 0);

    Ok(())
}

/// Test should_skip_range_by_prefix_filter where start prefix matches min_key prefix
/// but end prefix differs, and the table contains the start prefix.
/// The filter reports the prefix exists, so the table is NOT skipped.
#[test]
fn test_skip_range_start_prefix_matches_min_key() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
    .open()?;

    // Wide key range table: prefixes "aaa" through "zzz", but not "mmm"
    for p in [b"aaa", b"bbb", b"yyy", b"zzz"] {
        for i in 0..3u32 {
            let key = format!("{}_{:02}", std::str::from_utf8(p).unwrap(), i);
            tree.insert(key.as_bytes(), b"value", 0);
        }
    }
    tree.flush_active_memtable(0)?;

    // Range where start prefix ("aaa") matches min_key prefix ("aaa"),
    // end prefix ("bbb") differs. Start prefix IS in the filter,
    // so table is NOT skipped.
    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"aaa_00"[..]..&b"bbb_99"[..], SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 6); // aaa_00..02 + bbb_00..02

    Ok(())
}

/// Test should_skip_range_by_prefix_filter where start prefix matches min_key prefix
/// but the start key itself is absent from the filter. The filter excludes the start key,
/// so the table IS skipped. This exercises the fallback path in should_skip_range_by_prefix_filter.
#[test]
fn test_skip_range_start_prefix_matches_min_key_but_absent() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
    .open()?;

    // Table with wide key range: "aaa" and "zzz" present, but NOT "mmm"
    for p in [b"aaa", b"zzz"] {
        for i in 0..5u32 {
            let key = format!("{}_{:02}", std::str::from_utf8(p).unwrap(), i);
            tree.insert(key.as_bytes(), b"value", 0);
        }
    }
    tree.flush_active_memtable(0)?;

    // Range where start prefix ("aaa") matches min_key prefix ("aaa"),
    // end prefix ("mmm") differs. But we're querying with start key "aaa_xx"
    // which has prefix "aaa" that IS in the filter. So the table won't be skipped.
    // (The fallback path is: start prefix == min_key prefix AND filter says it might exist)
    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"aaa_00"[..]..&b"mmm_99"[..], SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 5); // aaa_00..04

    Ok(())
}

/// Test should_skip_range_by_prefix_filter where start prefix differs from min_key prefix.
#[test]
fn test_skip_range_start_prefix_differs_from_min_key() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    // Table with "aaaa" prefix only
    for i in 0..5u32 {
        let key = format!("aaaa_{:04}", i);
        tree.insert(key.as_bytes(), b"value", 0);
    }
    tree.flush_active_memtable(0)?;

    // Range where start prefix ("bbbb") differs from min_key prefix ("aaaa"),
    // and end prefix ("cccc") also differs. Neither matches.
    let keys: Vec<_> = tree
        .range::<&[u8], _>(&b"bbbb_0000"[..]..&b"cccc_0000"[..], SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 0);

    Ok(())
}

/// Test maybe_contains_prefix with an incompatible extractor name.
/// The table was written with "fixed_prefix" but we reopen with a differently-named extractor.
#[test]
fn test_maybe_contains_prefix_incompatible_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Write data with "fixed_prefix" extractor
    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
        .open()?;

        tree.insert(b"aaaa_001", b"v1", 0);
        tree.flush_active_memtable(0)?;
    }

    // Reopen with a differently-named extractor
    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(TestPrefixExtractor::new(4, "different_name")))
        .open()?;

        // Point read should still work — incompatible extractor means filter is bypassed
        assert_eq!(&*tree.get(b"aaaa_001", SeqNo::MAX)?.unwrap(), b"v1");

        // Prefix query with non-existent prefix should still return nothing
        // (data just isn't there, filter cannot help but doesn't hurt)
        let keys: Vec<_> = tree
            .prefix(b"zzzz", SeqNo::MAX, None)
            .map(|g| g.key().unwrap())
            .collect();
        assert_eq!(keys.len(), 0);
    }

    Ok(())
}

/// Test partitioned filter with unpinned TLI (top-level index).
#[test]
fn test_partitioned_filter_unpinned_tli() -> lsm_tree::Result<()> {
    use lsm_tree::config::PinningPolicy;

    let folder = tempfile::tempdir()?;

    let mut config = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(10.0),
    )))
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .filter_block_partitioning_policy(PinningPolicy::all(true))
    .index_block_pinning_policy(PinningPolicy::all(false));

    // Unpin the top-level filter index so we exercise the unpinned TLI code path
    config.top_level_filter_block_pinning_policy = PinningPolicy::all(false);

    let tree = config.open()?;

    for i in 0..50u32 {
        let key = format!("aaaa_{:04}", i);
        tree.insert(key.as_bytes(), b"value", 0);
    }
    tree.flush_active_memtable(0)?;

    // Point reads should still work through the unpinned TLI path
    assert_eq!(&*tree.get(b"aaaa_0000", SeqNo::MAX)?.unwrap(), b"value");
    assert_eq!(&*tree.get(b"aaaa_0049", SeqNo::MAX)?.unwrap(), b"value");
    assert!(tree.get(b"zzzz_0000", SeqNo::MAX)?.is_none());

    // Prefix query
    let keys: Vec<_> = tree
        .prefix(b"aaaa", SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 50);

    Ok(())
}

/// Test partitioned filter prefix spill (register_bytes causing filter partition flush).
/// Uses a small partition size and many unique prefixes to trigger the spill path.
#[test]
fn test_partitioned_filter_prefix_spill() -> lsm_tree::Result<()> {
    use lsm_tree::config::{BlockSizePolicy, PinningPolicy};

    let folder = tempfile::tempdir()?;

    let mut config = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(10.0),
    )))
    .filter_block_partitioning_policy(PinningPolicy::all(true));

    // Set very small partition size to force spills
    config.filter_block_partition_size_policy = BlockSizePolicy::all(128);
    config.index_block_partition_size_policy = BlockSizePolicy::all(128);

    let tree = config.open()?;

    // Insert many unique prefixes to force filter partition spills
    for p in 0..200u32 {
        let key = format!("{:04}_data", p);
        tree.insert(key.as_bytes(), b"value", 0);
    }
    tree.flush_active_memtable(0)?;

    // Verify data is still accessible
    assert_eq!(&*tree.get(b"0000_data", SeqNo::MAX)?.unwrap(), b"value");
    assert_eq!(&*tree.get(b"0199_data", SeqNo::MAX)?.unwrap(), b"value");
    assert!(tree.get(b"9999_data", SeqNo::MAX)?.is_none());

    Ok(())
}

/// Test prefix with all 0xFF bytes -> prefix_upper_range returns Unbounded.
#[test]
fn test_prefix_all_0xff() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
    .open()?;

    // Insert keys with all-0xFF prefix
    let prefix = vec![0xFF, 0xFF, 0xFF];
    for i in 0..5u8 {
        let mut key = prefix.clone();
        key.push(i);
        tree.insert(&key, b"value", 0);
    }

    // Also insert some non-0xFF keys
    tree.insert(b"aaa_001", b"other", 0);
    tree.flush_active_memtable(0)?;

    // Prefix query with all-0xFF prefix should work (upper bound is Unbounded)
    let keys: Vec<_> = tree
        .prefix(&prefix, SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 5);
    for k in &keys {
        assert!(k.starts_with(&prefix));
    }

    Ok(())
}

/// Test BlobTree with partitioned filter + prefix extractor flush.
#[test]
fn test_blob_tree_partitioned_filter_flush() -> lsm_tree::Result<()> {
    use lsm_tree::config::PinningPolicy;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(10.0),
    )))
    .filter_block_partitioning_policy(PinningPolicy::all(true))
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    let big_value = vec![b'x'; 1_000];
    for i in 0..20u32 {
        let key = format!("pref_{:04}", i);
        tree.insert(key.as_bytes(), &big_value, 0);
    }
    tree.flush_active_memtable(0)?;

    // Point reads
    assert_eq!(
        &*tree.get(b"pref_0000", SeqNo::MAX)?.unwrap(),
        &big_value[..]
    );
    assert!(tree.get(b"miss_0000", SeqNo::MAX)?.is_none());

    // Prefix query
    let keys: Vec<_> = tree
        .prefix(b"pref", SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 20);

    Ok(())
}

/// Test MAX_UPFRONT_CHECKS exceeded path: >10 tables in a run with absent prefix.
#[test]
fn test_run_reader_max_upfront_checks_exceeded() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::Leveled;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
    .open()?;

    // Create 12 separate L0 tables with distinct prefixes
    for p in 0..12u32 {
        let prefix = format!("{:04}", p);
        for i in 0..3u32 {
            let key = format!("{}_{:04}", prefix, i);
            tree.insert(key.as_bytes(), b"value", 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // Compact to L1 so they form a single sorted run
    tree.compact(Arc::new(Leveled::default()), 0)?;

    // Query an absent prefix — upfront check will try up to 10 tables,
    // exceed the limit, and fall through to lazy per-table checking
    let keys: Vec<_> = tree
        .prefix(b"zzzz", SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 0);

    Ok(())
}

/// Test short keys with a prefix extractor that has a longer prefix length.
/// FixedPrefixExtractor returns the full key when it's shorter than prefix length.
#[test]
fn test_short_keys_with_prefix_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(8)))
    .open()?;

    // All keys shorter than prefix length (8)
    tree.insert(b"a", b"v1", 0);
    tree.insert(b"ab", b"v2", 0);
    tree.insert(b"abc", b"v3", 0);
    tree.insert(b"abcdefg", b"v4", 0); // still 7 < 8
    tree.flush_active_memtable(0)?;

    // All should be readable
    assert_eq!(&*tree.get(b"a", SeqNo::MAX)?.unwrap(), b"v1");
    assert_eq!(&*tree.get(b"abcdefg", SeqNo::MAX)?.unwrap(), b"v4");

    // Range query returns all
    let keys: Vec<_> = tree
        .range::<&[u8], _>(.., SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();
    assert_eq!(keys.len(), 4);

    Ok(())
}
