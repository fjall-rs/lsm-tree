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

        #[cfg(feature = "metrics")]
        let initial_queries = tree.metrics().filter_queries();

        for i in 0..100 {
            let key = format!("persistent_{:04}", i);
            assert!(tree.contains_key(key.as_bytes(), u64::MAX)?);
        }

        // Non-existent keys should still be filtered
        let non_existent = b"persistent_9999";
        assert!(!tree.contains_key(non_existent, u64::MAX)?);

        #[cfg(feature = "metrics")]
        {
            let final_queries = tree.metrics().filter_queries();

            // After recovery, filters should still be working
            assert!(
                final_queries > initial_queries,
                "filter queries should work after recovery"
            );
        }
    }

    Ok(())
}

/// Test prefix extractor name persistence across recovery cycles.
#[test]
fn test_prefix_extractor_name_persistence() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Write with "fixed_prefix"
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
        assert!(tree.get(b"cccc_001", SeqNo::MAX)?.is_none());

        let keys: Vec<_> = tree
            .prefix(b"aaaa", SeqNo::MAX, None)
            .map(|g| g.key().unwrap())
            .collect();
        assert_eq!(keys.len(), 1);
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

    // Write data with "fixed_prefix" extractor
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
