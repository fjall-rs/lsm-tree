use lsm_tree::{
    blob_tree::FragmentationEntry, get_tmp_folder, AbstractTree, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
};
use test_log::test;

#[test]
fn blob_tree_major_compact_relocation_simple() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    {
        let tree = lsm_tree::Config::<lsm_tree::fs::StdFileSystem>::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .compression(lsm_tree::CompressionType::None)
                .age_cutoff(1.0),
        ))
        .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("big2", &big_value, 0);
        tree.insert("smol", "smol", 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("big2", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.insert("big", &new_big_value, 1);

        tree.flush_active_memtable(0)?;

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
        let value = tree.get("big2", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(2, tree.blob_file_count());

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
        let value = tree.get("big2", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");

        {
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

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(2, tree.blob_file_count());

        {
            let gc_stats = tree.current_version().gc_stats().clone();

            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
        let value = tree.get("big2", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
    }

    Ok(())
}

#[test]
fn blob_tree_major_compact_relocation_repeated_key() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(2_000);
    let very_big_value = b"winter!".repeat(128_000);

    {
        let tree = lsm_tree::Config::<lsm_tree::fs::StdFileSystem>::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .compression(lsm_tree::CompressionType::None)
                .age_cutoff(1.0),
        ))
        .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("a", &big_value, 0);
        tree.insert("b", &big_value, 0);
        tree.insert("c", &very_big_value, 0);
        tree.insert("d", &big_value, 0);
        tree.insert("e", &big_value, 0);

        let value = tree.get("a", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("b", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("c", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, very_big_value);
        let value = tree.get("d", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("e", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.remove("c", 1);

        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let value = tree.get("a", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("b", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("c", SeqNo::MAX)?;
        assert!(value.is_none());
        let value = tree.get("d", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("e", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let value = tree.get("a", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("b", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("c", SeqNo::MAX)?;
        assert!(value.is_none());
        let value = tree.get("d", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("e", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        {
            let gc_stats = tree.current_version().gc_stats().clone();

            assert_eq!(
                &{
                    let mut map = lsm_tree::HashMap::default();
                    let size = very_big_value.len() as u64;
                    map.insert(0, FragmentationEntry::new(1, size, size));
                    map
                },
                &*gc_stats,
            );
        }

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        {
            let gc_stats = tree.current_version().gc_stats().clone();

            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        let value = tree.get("a", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("b", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("c", SeqNo::MAX)?;
        assert!(value.is_none());
        let value = tree.get("d", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("e", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
    }

    Ok(())
}

#[test]
fn blob_tree_major_compact_relocation_interleaved() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(2_000);

    {
        let tree = lsm_tree::Config::<lsm_tree::fs::StdFileSystem>::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .compression(lsm_tree::CompressionType::None)
                .age_cutoff(1.0),
        ))
        .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("a", b"smol", 0);
        tree.insert("b", &big_value, 0);
        tree.insert("c", b"smol", 0);
        tree.insert("d", &big_value, 0);
        tree.insert("e", b"smol", 0);

        let value = tree.get("a", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
        let value = tree.get("b", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("c", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
        let value = tree.get("d", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("e", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.remove("d", 1);

        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let value = tree.get("a", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
        let value = tree.get("b", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("c", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
        let value = tree.get("d", SeqNo::MAX)?;
        assert!(value.is_none());
        let value = tree.get("e", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let value = tree.get("a", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
        let value = tree.get("b", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("c", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
        let value = tree.get("d", SeqNo::MAX)?;
        assert!(value.is_none());
        let value = tree.get("e", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
        {
            let gc_stats = tree.current_version().gc_stats().clone();

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

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        {
            let gc_stats = tree.current_version().gc_stats().clone();

            assert_eq!(&lsm_tree::HashMap::default(), &*gc_stats);
        }

        let value = tree.get("a", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
        let value = tree.get("b", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("c", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
        let value = tree.get("d", SeqNo::MAX)?;
        assert!(value.is_none());
        let value = tree.get("e", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");
    }

    Ok(())
}
