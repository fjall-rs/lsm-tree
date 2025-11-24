use lsm_tree::{
    blob_tree::FragmentationEntry, get_tmp_folder, AbstractTree, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
};
use test_log::test;

#[test]
fn blob_tree_major_compact_gc_stats() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default())
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("smol", "small value", 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.insert("big", &new_big_value, 1);

        tree.flush_active_memtable(0)?;

        // Blob file has no fragmentation before compaction (in stats)
        // so it is not rewritten
        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(2, tree.blob_file_count());

        let gc_stats = tree.current_version().gc_stats().clone();

        // "big":0 is expired
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
fn blob_tree_major_compact_gc_stats_tombstone() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default())
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("another_big", &big_value, 0);
        tree.insert("smol", "small value", 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.remove("big", 1);

        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        assert_eq!(
            Some(vec![lsm_tree::table::writer::LinkedFile {
                blob_file_id: 0,
                bytes: 2 * big_value.len() as u64,
                on_disk_bytes: 2 * big_value.len() as u64,
                len: 2,
            }]),
            tree.current_version()
                .iter_tables()
                .nth(1)
                .unwrap()
                .list_blob_file_references()?,
        );

        // Blob file has no fragmentation before compaction (in stats)
        // so it is not rewritten
        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let gc_stats = tree.current_version().gc_stats().clone();

        // "big":0 is expired
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                let size = big_value.len() as u64;
                map.insert(0, FragmentationEntry::new(1, size, size));
                map
            },
            &*gc_stats,
        );

        assert_eq!(
            Some(vec![lsm_tree::table::writer::LinkedFile {
                blob_file_id: 0,
                bytes: big_value.len() as u64,
                on_disk_bytes: big_value.len() as u64,
                len: 1,
            }]),
            tree.current_version()
                .iter_tables()
                .next()
                .unwrap()
                .list_blob_file_references()?,
        );
    }

    Ok(())
}
