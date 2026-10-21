#[test_log::test]
#[cfg(feature = "lz4")]
fn blob_tree_compression() -> lsm_tree::Result<()> {
    use lsm_tree::{
        blob_tree::FragmentationEntry, AbstractTree, KvSeparationOptions, MemoryVersionSetProvider,
        SeqNo, SequenceNumberCounter,
    };
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(path, version_set_provider.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .compression(lsm_tree::CompressionType::Lz4)
                .separation_threshold(1)
                .staleness_threshold(0.0000001)
                .age_cutoff(1.0),
        ))
        .open()?;

    let big_value = b"abc".repeat(50);

    let batch_seqno = seqno.next();
    tree.insert("a", &big_value, batch_seqno);
    tree.insert("b", b"smol", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    {
        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);

        let value = tree.get("b", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, b"smol");
    }

    tree.remove("b", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(2, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    {
        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);

        assert!(!tree.contains_key("b", SeqNo::MAX)?);
    }

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
        seqno.next(),
        1_000,
    )?;

    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    {
        let gc_stats = tree.current_version().gc_stats().clone();

        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                map.insert(
                    0,
                    FragmentationEntry::new(1, b"smol".len().try_into().unwrap(), 5),
                );
                map
            },
            &*gc_stats,
        );
    }

    {
        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);

        assert!(!tree.contains_key("b", SeqNo::MAX)?);
    }

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
        seqno.next(),
        1_000,
    )?;

    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    {
        let gc_stats = tree.current_version().gc_stats().clone();

        assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
    }

    {
        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);

        assert!(!tree.contains_key("b", SeqNo::MAX)?);
    }

    Ok(())
}
