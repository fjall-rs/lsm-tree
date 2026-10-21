use lsm_tree::{
    blob_tree::FragmentationEntry, get_tmp_folder, AbstractTree, KvSeparationOptions,
    MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_major_compact_relocation_simple() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default()
                    .compression(lsm_tree::CompressionType::None)
                    .age_cutoff(1.0),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        let batch_seqno = seqno.next();
        tree.insert("big", &big_value, batch_seqno);
        tree.insert("big2", &big_value, batch_seqno);
        tree.insert("smol", "smol", batch_seqno);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("big2", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.insert("big", &new_big_value, seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
        let value = tree.get("big2", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"smol");

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;

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

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;

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

    let seqno = SequenceNumberCounter::default();
    let big_value = b"neptune!".repeat(2_000);
    let very_big_value = b"winter!".repeat(128_000);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default()
                    .compression(lsm_tree::CompressionType::None)
                    .age_cutoff(1.0),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        let batch_seqno = seqno.next();
        tree.insert("a", &big_value, batch_seqno);
        tree.insert("b", &big_value, batch_seqno);
        tree.insert("c", &very_big_value, batch_seqno);
        tree.insert("d", &big_value, batch_seqno);
        tree.insert("e", &big_value, batch_seqno);

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

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.remove("c", seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

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

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;

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

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;

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

    let seqno = SequenceNumberCounter::default();
    let big_value = b"neptune!".repeat(2_000);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default()
                    .compression(lsm_tree::CompressionType::None)
                    .age_cutoff(1.0),
            ))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        let batch_seqno = seqno.next();
        tree.insert("a", b"smol", batch_seqno);
        tree.insert("b", &big_value, batch_seqno);
        tree.insert("c", b"smol", batch_seqno);
        tree.insert("d", &big_value, batch_seqno);
        tree.insert("e", b"smol", batch_seqno);

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

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.remove("d", seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

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

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;

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

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::default()),
            seqno.next(),
            1_000,
        )?;

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
