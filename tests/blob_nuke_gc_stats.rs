use lsm_tree::{
    blob_tree::FragmentationEntry, get_tmp_folder, AbstractTree, KvSeparationOptions,
    MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
#[ignore = "v4 restore drop_range"]
fn blob_tree_nuke_gc_stats() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let big_value = b"neptune!".repeat(128_000);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, seqno.next());

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        todo!();

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
#[ignore = "v4 restore drop_range"]
fn blob_tree_nuke_gc_stats_multi() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let big_value = b"neptune!".repeat(128_000);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, seqno.next());
        tree.insert("big", &big_value, seqno.next());

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        todo!();

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
