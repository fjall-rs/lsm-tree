use lsm_tree::{blob_tree::FragmentationEntry, AbstractTree, SeqNo};
use test_log::test;

#[test]
fn blob_tree_nuke_gc_stats() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path)
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(1)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(1, tree.blob_file_count());

        tree.drop_range::<&[u8], _>(..)?;

        // NOTE: Because the blob does not have any incoming references anymore
        // it is pruned from the Version
        assert_eq!(0, tree.blob_file_count());
        assert_eq!(0, tree.segment_count());

        let gc_stats = tree.current_version().gc_stats().clone();

        // "big":0 was dropped
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                map.insert(0, FragmentationEntry::new(1, big_value.len() as u64));
                map
            },
            &*gc_stats,
        );
    }

    Ok(())
}
