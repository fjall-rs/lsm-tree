use lsm_tree::{
    blob_tree::FragmentationEntry, get_tmp_folder, AbstractTree, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
};
use test_log::test;

#[test]
fn blob_tree_nuke_gc_stats() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default(), SequenceNumberCounter::default())
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.drop_range::<&[u8], _>(..)?;

        // NOTE: Because the blob does not have any incoming references anymore
        // it is pruned from the Version
        assert_eq!(0, tree.blob_file_count());
        assert_eq!(0, tree.table_count());

        let gc_stats = tree.current_version().gc_stats().clone();

        // "big":0 was dropped
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                let size = big_value.len() as u64;
                map.insert(0, FragmentationEntry::new(1, size, size));
                map
            },
            &*gc_stats,
        );
    }

    Ok(())
}

#[test]
fn blob_tree_nuke_gc_stats_multi() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default(), SequenceNumberCounter::default())
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("big", &big_value, 1);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.drop_range::<&[u8], _>(..)?;

        // NOTE: Because the blob does not have any incoming references anymore
        // it is pruned from the Version
        assert_eq!(0, tree.blob_file_count());
        assert_eq!(0, tree.table_count());

        let gc_stats = tree.current_version().gc_stats().clone();

        // "big":0, big":1 were dropped
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                let size = big_value.len() as u64;
                map.insert(0, FragmentationEntry::new(2, size * 2, size * 2));
                map
            },
            &*gc_stats,
        );
    }

    Ok(())
}
