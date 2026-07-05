#[test_log::test]
#[cfg(feature = "lz4")]
fn blob_tree_major_compact_relocation_recovery() -> lsm_tree::Result<()> {
    use lsm_tree::{
        get_tmp_folder, AbstractTree, KvSeparationOptions, SeqNo, SequenceNumberCounter,
    };

    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(4_096);

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .compression(lsm_tree::CompressionType::Lz4)
                .separation_threshold(1)
                .staleness_threshold(0.0000001)
                .age_cutoff(1.0),
        ))
        .open()?;

        tree.insert("a", &big_value, 0);
        tree.insert("b", b"smol", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.remove("b", 1);
        tree.flush_active_memtable(0)?;

        tree.major_compact(u64::MAX, 1_000)?;
        tree.major_compact(u64::MAX, 1_000)?;
        assert_eq!(1, tree.blob_file_count());

        {
            let gc_stats = tree.current_version().gc_stats().clone();

            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);
    }

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(Default::default()))
        .open()?;

        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);
    }

    Ok(())
}
