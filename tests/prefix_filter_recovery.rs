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
