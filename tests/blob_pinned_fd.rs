use lsm_tree::{get_tmp_folder, AbstractTree, KvSeparationOptions, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn blob_tree_with_pinned_fd() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .descriptor_table(None)
        .with_kv_separation(Some(
            KvSeparationOptions::default().compression(lsm_tree::CompressionType::None),
        ))
        .open()?;

        tree.insert("big", &big_value, 0);
        tree.insert("smol", "small value", 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

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

        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"small value");
    }

    Ok(())
}

#[test]
fn blob_tree_recovery_with_pinned_fd() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .descriptor_table(None)
        .with_kv_separation(Some(Default::default()))
        .open()?;

        tree.insert("big", &big_value, 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());
    }

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .descriptor_table(None)
        .with_kv_separation(Some(Default::default()))
        .open()?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let refs = tree
            .current_version()
            .iter_tables()
            .next()
            .unwrap()
            .list_blob_file_references()?;

        assert!(refs.is_some());
        let refs = refs.unwrap();
        assert_eq!(1, refs.len());
        assert_eq!(0, refs[0].blob_file_id);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
    }

    Ok(())
}
