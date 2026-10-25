use lsm_tree::Guard;
use lsm_tree::SequenceNumberCounter;
use lsm_tree::{
    prefix::{FixedPrefixExtractor, PrefixExtractor},
    AbstractTree, Config, SeqNo,
};
use std::sync::Arc;

#[test]
fn test_prefix_filter_recovery() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let prefix_len = 10;

    // Create and populate tree
    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
        .open()?;

        for i in 0..100 {
            let key = format!("persistent_{:04}", i);
            tree.insert(key.as_bytes(), b"value", 0);
        }
        // Sentinel with a different 10-byte prefix to widen the key range
        tree.insert(b"zzzzzzzzzz_sentinel", b"value", 0);

        tree.flush_active_memtable(0)?;
    }

    // Reopen tree and verify filter still works
    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(prefix_len)))
        .open()?;

        for i in 0..100 {
            let key = format!("persistent_{:04}", i);
            assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
        }

        // Look up a key with a prefix absent from the filter but within the
        // table's key range (between "persistent_*" and "zzzzzzzzzz_*").
        #[cfg(feature = "metrics")]
        {
            let initial_queries = tree.metrics().filter_queries();
            let initial_skips = tree.metrics().io_skipped_by_filter();

            let non_existent = b"qqqqqqqqqq_0000";
            assert!(!tree.contains_key(non_existent, u64::MAX)?);

            let final_queries = tree.metrics().filter_queries();
            let final_skips = tree.metrics().io_skipped_by_filter();

            assert!(
                final_queries > initial_queries,
                "filter should be consulted after recovery"
            );
            assert!(
                final_skips > initial_skips,
                "filter should skip absent prefix after recovery"
            );
        }
    }

    Ok(())
}

/// Test prefix extractor name persistence across recovery cycles.
#[test]
fn test_prefix_extractor_name_persistence() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Write with "fixed_prefix:4"
    {
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
    }

    // Reopen with same extractor — prefix filter should be used
    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
        .open()?;

        assert_eq!(&*tree.get(b"aaaa_001", SeqNo::MAX)?.unwrap(), b"v1");
        assert_eq!(&*tree.get(b"bbbb_001", SeqNo::MAX)?.unwrap(), b"v2");

        let keys: Vec<_> = tree
            .prefix(b"aaaa", SeqNo::MAX, None)
            .map(|g| g.key().unwrap())
            .collect();

        assert_eq!(keys.len(), 1);

        #[cfg(feature = "metrics")]
        {
            let initial_queries = tree.metrics().filter_queries();

            // Look up a key within the table's range whose prefix is absent
            // from the filter ("abcd" is between "aaaa" and "bbbb").
            assert!(tree.get(b"abcd_001", SeqNo::MAX)?.is_none());

            let final_queries = tree.metrics().filter_queries();
            assert!(final_queries > initial_queries, "filter should be used");
        }
    }

    // Reopen WITHOUT any extractor — filter should be bypassed but reads work
    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        assert_eq!(&*tree.get(b"aaaa_001", SeqNo::MAX)?.unwrap(), b"v1");
        assert_eq!(&*tree.get(b"bbbb_001", SeqNo::MAX)?.unwrap(), b"v2");

        #[cfg(feature = "metrics")]
        {
            let final_queries = tree.metrics().filter_queries();

            // Without an extractor, prefix filter should be bypassed
            assert_eq!(0, final_queries, "filter should not be used");
        }
    }

    Ok(())
}

/// Test should_skip_range_by_prefix_filter with an incompatible extractor.
#[test]
fn test_skip_range_incompatible_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    struct InvalidExtractor;

    impl PrefixExtractor for InvalidExtractor {
        fn extract<'a>(&self, _key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            unreachable!()
        }

        fn name(&self) -> &str {
            "asd"
        }
    }

    // Write data with "fixed_prefix:4" extractor
    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
        .open()?;

        for i in 0..10u32 {
            let key = format!("aaaa_{:04}", i);
            tree.insert(key.as_bytes(), b"value", 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // Reopen with incompatible extractor name — range should not skip tables
    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(InvalidExtractor))
        .open()?;

        let keys: Vec<_> = tree
            .range::<&[u8], _>(&b"aaaa_0000"[..]..&b"aaaa_9999"[..], SeqNo::MAX, None)
            .map(|g| g.key().unwrap())
            .collect();

        assert_eq!(keys.len(), 10);
    }

    Ok(())
}

/// Recovery: tree written with `whole_key_filtering=false` and a prefix
/// extractor is reopened with the same extractor. Reads must still work
/// (using prefix filter alone since no full-key hashes are persisted).
#[test]
fn test_recovery_wkf_false_same_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(5)))
        .whole_key_filtering(false)
        .open()?;

        for i in 0..50 {
            let key = format!("hello_{i:04}");
            tree.insert(key.as_bytes(), b"v", 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // Reopen with identical config.
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(5)))
    .whole_key_filtering(false)
    .open()?;

    for i in 0..50 {
        let key = format!("hello_{i:04}");
        assert!(
            tree.contains_key(key.as_bytes(), u64::MAX)?,
            "false negative after WKF=false reopen: {key}"
        );
    }

    // Prefix scan also works.
    let count = tree.prefix(b"hello", u64::MAX, None).count();
    assert_eq!(count, 50);

    Ok(())
}

/// Recovery: a legacy-style table (written without any prefix extractor)
/// is reopened with an extractor. `prefix_filter_allowed` should return
/// `false` for this table (table has no extractor name persisted), and
/// reads must fall through to the full-key Bloom path (legacy default
/// `whole_key_filtering=true` ensures the full-key Bloom is queryable).
#[test]
fn test_recovery_legacy_table_then_extractor_added() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Phase 1: write without extractor (simulates a table from before
    // the prefix-filter feature existed).
    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        for i in 0..50 {
            let key = format!("legacy_{i:04}");
            tree.insert(key.as_bytes(), b"v", 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // Phase 2: reopen with an extractor configured. The on-disk tables
    // have no extractor name, so prefix_filter_allowed returns false; but
    // they were written with full-key hashes, so point reads can still use
    // the full-key Bloom.
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(7)))
    .open()?;

    // Point reads: must find every key.
    for i in 0..50 {
        let key = format!("legacy_{i:04}");
        assert!(
            tree.contains_key(key.as_bytes(), u64::MAX)?,
            "false negative for legacy table on extractor-added reopen: {key}"
        );
    }

    // Prefix scan: should also work (falls back to bounds-based pruning
    // since the legacy table has no extractor name, so the prefix-aware
    // pruning is skipped per table).
    let count = tree.prefix(b"legacy_", u64::MAX, None).count();
    assert_eq!(
        count, 50,
        "prefix scan on legacy table should find all keys"
    );

    Ok(())
}

/// Recovery: a table written with extractor A is reopened with extractor B
/// (different name). `prefix_filter_allowed` returns false. With
/// `whole_key_filtering=true` (the default), the full-key Bloom is still
/// valid and reads succeed.
///
/// This locks in the audit's full-key-Bloom-on-mismatch optimization
/// (commit 9cf39ad9): a tree migrated between extractors must remain
/// readable AND use the full-key Bloom path (not just `get_without_filter`).
/// Without the metric assertion below, this test would pass even if the
/// optimization were reverted (because the data-block scan would still
/// find the keys); the metric check is what actually locks in the
/// optimization.
#[test]
#[cfg(feature = "metrics")]
fn test_recovery_extractor_change_with_wkf_true_reads_succeed() -> lsm_tree::Result<()> {
    use lsm_tree::AbstractTree;
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
        .whole_key_filtering(true)
        .open()?;

        for i in 0..50 {
            let key = format!("abc{i:05}");
            tree.insert(key.as_bytes(), b"v", 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // Reopen with extractor of different length → different name → mismatch.
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(FixedPrefixExtractor::new(5)))
    .whole_key_filtering(true)
    .open()?;

    // Existing keys are findable.
    for i in 0..50 {
        let key = format!("abc{i:05}");
        assert!(
            tree.contains_key(key.as_bytes(), u64::MAX)?,
            "false negative after extractor change with WKF=true: {key}"
        );
    }

    // Verify the optimization (9cf39ad9) is exercised: a missing-key lookup
    // whose key range falls inside the table should bump the
    // `io_skipped_by_filter` counter — meaning the full-key Bloom was
    // consulted and rejected the key. Without the optimization, reads
    // would fall through to `get_without_filter` and this counter would
    // not increment.
    let before = tree.metrics().io_skipped_by_filter();
    // Use a key that lies within the table's key_range but doesn't exist,
    // so it isn't pruned by key-range overlap before reaching the filter.
    for i in 0..50 {
        let key = format!("abc{i:05}_absent");
        assert!(!tree.contains_key(key.as_bytes(), u64::MAX)?);
    }
    let after = tree.metrics().io_skipped_by_filter();
    assert!(
        after > before,
        "full-key Bloom was not consulted on extractor mismatch — \
         9cf39ad9 optimization regressed (io_skipped_by_filter: {before} -> {after})"
    );

    Ok(())
}
