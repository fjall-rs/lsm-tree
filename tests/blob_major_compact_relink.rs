use lsm_tree::{AbstractTree, SeqNo};
use test_log::test;

#[test]
fn blob_tree_major_compact_gc_stats() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

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

        tree.flush_active_memtable(1)?;

        tree.major_compact(64_000_000, 1_000)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(1, tree.blob_file_count());

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
