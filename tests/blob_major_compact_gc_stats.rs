use lsm_tree::{
    blob_tree::FragmentationEntry, get_tmp_folder, AbstractTree, KvSeparationOptions,
    MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_major_compact_gc_stats() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        let batch_seqno = seqno.next();
        tree.insert("big", &big_value, batch_seqno);
        tree.insert("smol", "small value", batch_seqno);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.insert("big", &new_big_value, seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        // Blob file has no fragmentation before compaction (in stats)
        // so it is not rewritten
        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;

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
        assert_eq!(big_value.len() as u64, tree.stale_blob_bytes());
    }

    Ok(())
}

#[test]
fn blob_tree_major_compact_gc_stats_2() -> lsm_tree::Result<()> {
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
        tree.insert("big", &big_value, seqno.next());
        tree.insert("big", &big_value, seqno.next());
        tree.insert("big", &big_value, seqno.next());

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        // Blob file has no fragmentation before compaction (in stats)
        // so it is not rewritten
        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let gc_stats = tree.current_version().gc_stats().clone();

        // "big":0,1,2,3 are expired
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                let size = big_value.len() as u64;
                map.insert(0, FragmentationEntry::new(4, size * 4, size * 4));
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
        let batch_seqno = seqno.next();
        tree.insert("big", &big_value, batch_seqno);
        tree.insert("another_big", &big_value, batch_seqno);
        tree.insert("smol", "small value", batch_seqno);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.remove("big", seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

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
        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;

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
