use lsm_tree::{AbstractTree, SeqNo};
use test_log::test;

#[test]
fn blob_tree_simple() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path).open_as_blob_tree()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("smol", "small value", 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;

        assert_eq!(1, tree.segment_count());
        assert_eq!(1, tree.blob_file_count());

        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"small value");

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.insert("big", &new_big_value, 1);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        tree.flush_active_memtable(0)?;

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        let value = tree.get("big", 1)?.expect("should exist");
        assert_eq!(&*value, big_value);
    }

    {
        let tree = lsm_tree::Config::new(path).open_as_blob_tree()?;

        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"small value");

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
    }

    Ok(())
}

#[cfg(feature = "lz4")]
#[test]
#[ignore = "wip"]
fn blob_tree_simple_compressed() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path)
        .compression(lsm_tree::CompressionType::Lz4)
        .open_as_blob_tree()?;

    let big_value = b"neptune!".repeat(128_000);

    assert!(tree.get("big", SeqNo::MAX)?.is_none());
    tree.insert("big", &big_value, 0);
    tree.insert("smol", "small value", 0);

    let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
    assert_eq!(&*value, big_value);

    tree.flush_active_memtable(0)?;

    let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
    assert_eq!(&*value, big_value);

    let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
    assert_eq!(&*value, b"small value");

    let new_big_value = b"winter!".repeat(128_000);
    tree.insert("big", &new_big_value, 1);

    let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
    assert_eq!(&*value, new_big_value);

    tree.flush_active_memtable(0)?;

    let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
    assert_eq!(&*value, new_big_value);

    Ok(())
}
