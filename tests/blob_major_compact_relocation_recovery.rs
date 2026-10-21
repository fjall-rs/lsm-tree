#[test_log::test]
fn blob_tree_major_compact_relocation_recovery_no_comp() -> lsm_tree::Result<()> {
    use lsm_tree::{
        get_tmp_folder, AbstractTree, KvSeparationOptions, MemoryVersionSetProvider, SeqNo,
        SequenceNumberCounter,
    };
    use std::sync::Arc;

    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let big_value = b"neptune!".repeat(4_096);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default()
                    .compression(lsm_tree::CompressionType::None)
                    .separation_threshold(1)
                    .staleness_threshold(0.0000001)
                    .age_cutoff(1.0),
            ))
            .open()?;

        let batch_seqno = seqno.next();
        tree.insert("a", &big_value, batch_seqno);
        tree.insert("b", b"smol", batch_seqno);

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.remove("b", seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
            seqno.next(),
            1_000,
        )?;
        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
            seqno.next(),
            1_000,
        )?;

        assert_eq!(1, tree.blob_file_count());

        {
            let gc_stats = tree.current_version().gc_stats().clone();

            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);
    }

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);
    }

    Ok(())
}

#[test_log::test]
#[cfg(feature = "lz4")]
fn blob_tree_major_compact_relocation_recovery() -> lsm_tree::Result<()> {
    use lsm_tree::{
        get_tmp_folder, AbstractTree, KvSeparationOptions, MemoryVersionSetProvider, SeqNo,
        SequenceNumberCounter,
    };
    use std::sync::Arc;

    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let big_value = b"neptune!".repeat(4_096);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default()
                    .compression(lsm_tree::CompressionType::Lz4)
                    .separation_threshold(1)
                    .staleness_threshold(0.0000001)
                    .age_cutoff(1.0),
            ))
            .open()?;

        let batch_seqno = seqno.next();
        tree.insert("a", &big_value, batch_seqno);
        tree.insert("b", b"smol", batch_seqno);

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.remove("b", seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
            seqno.next(),
            1_000,
        )?;
        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
            seqno.next(),
            1_000,
        )?;

        assert_eq!(1, tree.blob_file_count());

        {
            let gc_stats = tree.current_version().gc_stats().clone();

            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);
    }

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);
    }

    Ok(())
}
