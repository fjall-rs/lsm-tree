#[test_log::test]
#[cfg(feature = "metrics")]
fn tree_filter_hit_rate() -> lsm_tree::Result<()> {
    use lsm_tree::{
        get_tmp_folder, AbstractTree, Config, KvSeparationOptions, MemoryVersionSetProvider, SeqNo,
        SequenceNumberCounter,
    };
    use std::sync::Arc;

    let a = {
        let folder = get_tmp_folder();
        let path = folder.path();

        let seqno = SequenceNumberCounter::default();

        let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

        let tree = Config::new(path, version_set_provider.clone()).open()?;

        for k in 0u64..10_000 {
            tree.insert(k.to_be_bytes(), "abc", seqno.next());
        }

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            0,
        )?;

        for k in 0u64..10_000 {
            tree.get(k.to_be_bytes(), SeqNo::MAX).unwrap().unwrap();
        }

        tree.metrics().filter_efficiency()
    };

    {
        let folder = get_tmp_folder();
        let path = folder.path();

        let seqno = SequenceNumberCounter::default();

        let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

        let tree = Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
            .open()?;

        for k in 0u64..10_000 {
            tree.insert(k.to_be_bytes(), "abc", seqno.next());
        }

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            0,
        )?;

        for k in 0u64..10_000 {
            tree.get(k.to_be_bytes(), SeqNo::MAX).unwrap().unwrap();
        }

        assert_eq!(a, tree.metrics().filter_efficiency());
    }

    Ok(())
}

#[test_log::test]
#[cfg(feature = "metrics")]
fn tree_filter_hit_rate_hit() -> lsm_tree::Result<()> {
    use lsm_tree::{
        get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo,
        SequenceNumberCounter,
    };
    use std::sync::Arc;

    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(path, version_set_provider.clone()).open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("c", "a", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert_eq!(1, tree.metrics().filter_queries());
    assert_eq!(1, tree.metrics().io_skipped_by_filter());
    assert_eq!(1.0, tree.metrics().filter_efficiency());

    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    assert_eq!(1.0, tree.metrics().filter_efficiency());

    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    assert_eq!(1.0, tree.metrics().filter_efficiency());

    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    assert_eq!(1.0, tree.metrics().filter_efficiency());

    Ok(())
}
