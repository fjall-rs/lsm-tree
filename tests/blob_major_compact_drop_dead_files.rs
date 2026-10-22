use lsm_tree::{
    blob_tree::FragmentationEntry, get_tmp_folder, AbstractTree, KvSeparationOptions,
    MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_major_compact_drop_dead_files() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());

        tree.insert("big", &big_value, seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.insert("big", &big_value, seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(2, tree.table_count());
        assert_eq!(2, tree.blob_file_count());

        tree.insert("big", &big_value, seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(3, tree.table_count());
        assert_eq!(3, tree.blob_file_count());

        tree.insert("big", &big_value, seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(4, tree.table_count());
        assert_eq!(4, tree.blob_file_count());

        tree.insert("big", &new_big_value, seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(5, tree.table_count());
        assert_eq!(5, tree.blob_file_count());

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        {
            let gc_stats = tree.current_version().gc_stats().clone();

            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;

        assert_eq!(1, tree.table_count());
        assert_eq!(5, tree.blob_file_count());

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        {
            let gc_stats = tree.current_version().gc_stats().clone();

            assert_eq!(
                &{
                    let mut map = lsm_tree::HashMap::default();
                    let size = big_value.len() as u64;
                    map.insert(0, FragmentationEntry::new(1, size, size));
                    map.insert(1, FragmentationEntry::new(1, size, size));
                    map.insert(2, FragmentationEntry::new(1, size, size));
                    map.insert(3, FragmentationEntry::new(1, size, size));
                    map
                },
                &*gc_stats,
            );
        }

        for blob_file_id in 0..4 {
            assert!(
                path.join("blobs")
                    .join(blob_file_id.to_string())
                    .try_exists()?,
                "blob file {blob_file_id} should exist",
            );
        }

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;
        version_set_provider.prune();

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        for blob_file_id in 0..4 {
            assert!(
                !path
                    .join("blobs")
                    .join(blob_file_id.to_string())
                    .try_exists()?,
                "blob file {blob_file_id} should have been deleted",
            );
        }

        {
            let gc_stats = tree.current_version().gc_stats().clone();
            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
    }

    Ok(())
}
