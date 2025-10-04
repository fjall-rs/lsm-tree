use lsm_tree::{blob_tree::FragmentationEntry, AbstractTree, SeqNo};
use test_log::test;

#[test]
fn blob_tree_major_compact_drop_dead_files() -> lsm_tree::Result<()> {
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
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(1, tree.blob_file_count());

        tree.insert("big", &big_value, 1);
        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.segment_count());
        assert_eq!(2, tree.blob_file_count());

        tree.insert("big", &big_value, 2);
        tree.flush_active_memtable(0)?;
        assert_eq!(3, tree.segment_count());
        assert_eq!(3, tree.blob_file_count());

        tree.insert("big", &big_value, 3);
        tree.flush_active_memtable(0)?;
        assert_eq!(4, tree.segment_count());
        assert_eq!(4, tree.blob_file_count());

        tree.insert("big", &new_big_value, 4);
        tree.flush_active_memtable(0)?;
        assert_eq!(5, tree.segment_count());
        assert_eq!(5, tree.blob_file_count());

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        {
            let gc_stats = tree
                .manifest()
                .read()
                .expect("lock is poisoned")
                .current_version()
                .gc_stats()
                .clone();

            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(5, tree.blob_file_count());

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        {
            let gc_stats = tree
                .manifest()
                .read()
                .expect("lock is poisoned")
                .current_version()
                .gc_stats()
                .clone();

            assert_eq!(
                &{
                    let mut map = lsm_tree::HashMap::default();
                    map.insert(0, FragmentationEntry::new(1, big_value.len() as u64));
                    map.insert(1, FragmentationEntry::new(1, big_value.len() as u64));
                    map.insert(2, FragmentationEntry::new(1, big_value.len() as u64));
                    map.insert(3, FragmentationEntry::new(1, big_value.len() as u64));
                    map
                },
                &*gc_stats,
            );
        }

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(1, tree.blob_file_count());

        {
            let gc_stats = tree
                .manifest()
                .read()
                .expect("lock is poisoned")
                .current_version()
                .gc_stats()
                .clone();

            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
    }

    Ok(())
}
