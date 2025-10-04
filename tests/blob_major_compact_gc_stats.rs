use lsm_tree::{blob_tree::FragmentationEntry, AbstractTree, SeqNo};
use test_log::test;

// TODO: 3.0.0 check that decompressed value size is used (enable compression)

#[test]
fn blob_tree_major_compact_gc_stats() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path)
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("smol", "small value", 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(1, tree.blob_file_count());

        tree.insert("big", &new_big_value, 1);

        tree.flush_active_memtable(0)?;

        // Blob file has no fragmentation before compaction (in stats)
        // so it is not rewritten
        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(2, tree.blob_file_count());

        let gc_stats = tree
            .manifest()
            .read()
            .expect("lock is poisoned")
            .current_version()
            .gc_stats()
            .clone();

        // "big":0 is expired
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

#[test]
fn blob_tree_major_compact_gc_stats_tombstone() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path)
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("another_big", &big_value, 0);
        tree.insert("smol", "small value", 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(1, tree.blob_file_count());

        tree.remove("big", 1);

        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.segment_count());
        assert_eq!(1, tree.blob_file_count());

        assert_eq!(
            Some(vec![lsm_tree::segment::writer::LinkedFile {
                blob_file_id: 0,
                bytes: 2 * big_value.len() as u64,
                len: 2,
            }]),
            tree.manifest()
                .read()
                .expect("lock is poisoned")
                .current_version()
                .iter_segments()
                .nth(1)
                .unwrap()
                .get_linked_blob_files()?,
        );

        // Blob file has no fragmentation before compaction (in stats)
        // so it is not rewritten
        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(1, tree.blob_file_count());

        let gc_stats = tree
            .manifest()
            .read()
            .expect("lock is poisoned")
            .current_version()
            .gc_stats()
            .clone();

        // "big":0 is expired
        assert_eq!(
            &{
                let mut map = lsm_tree::HashMap::default();
                map.insert(0, FragmentationEntry::new(1, big_value.len() as u64));
                map
            },
            &*gc_stats,
        );

        assert_eq!(
            Some(vec![lsm_tree::segment::writer::LinkedFile {
                blob_file_id: 0,
                bytes: big_value.len() as u64,
                len: 1,
            }]),
            tree.manifest()
                .read()
                .expect("lock is poisoned")
                .current_version()
                .iter_segments()
                .next()
                .unwrap()
                .get_linked_blob_files()?,
        );
    }

    Ok(())
}
