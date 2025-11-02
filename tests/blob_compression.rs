#[test_log::test]
#[cfg(feature = "lz4")]
fn blob_tree_compression() -> lsm_tree::Result<()> {
    use lsm_tree::{
        blob_tree::FragmentationEntry, AbstractTree, KvSeparationOptions, SeqNo,
        SequenceNumberCounter,
    };

    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default())
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .compression(lsm_tree::CompressionType::Lz4)
                .separation_threshold(1)
                .staleness_threshold(0.0000001)
                .age_cutoff(1.0),
        ))
        .open()?;

    let big_value = b"abc".repeat(50);

    tree.insert("a", &big_value, 0);
    tree.insert("b", b"smol", 0);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    {
        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);

        let value = tree.get("b", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, b"smol");
    }

    tree.remove("b", 1);
    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    {
        let value = tree.get("a", SeqNo::MAX)?.unwrap();
        assert_eq!(&*value, big_value);

        assert!(!tree.contains_key("b", SeqNo::MAX)?);
    }

    tree.major_compact(u64::MAX, 1_000)?;
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

    tree.major_compact(u64::MAX, 1_000)?;
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
