use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn blob_tree_simple() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path).open_as_blob_tree()?;

        assert!(tree.get("big", None)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("smol", "small value", 0);

        let value = tree.get("big", None)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.flush_active_memtable(0)?;

        let value = tree.get("big", None)?.expect("should exist");
        assert_eq!(&*value, big_value);

        let value = tree.get("smol", None)?.expect("should exist");
        assert_eq!(&*value, b"small value");

        tree.insert("big", &new_big_value, 1);

        let value = tree.get("big", None)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        tree.flush_active_memtable(0)?;

        let value = tree.get("big", None)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
    }

    {
        let tree = lsm_tree::Config::new(path).open_as_blob_tree()?;

        let value = tree.get("smol", None)?.expect("should exist");
        assert_eq!(&*value, b"small value");

        let value = tree.get("big", None)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
    }

    Ok(())
}

#[cfg(feature = "lz4")]
#[test]
fn blob_tree_simple_compressed() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path)
        .compression(lsm_tree::CompressionType::Lz4)
        .open_as_blob_tree()?;

    let big_value = b"neptune!".repeat(128_000);

    assert!(tree.get("big")?.is_none());
    tree.insert("big", &big_value, 0);
    tree.insert("smol", "small value", 0);

    let value = tree.get("big")?.expect("should exist");
    assert_eq!(&*value, big_value);

    tree.flush_active_memtable(0)?;

    let value = tree.get("big")?.expect("should exist");
    assert_eq!(&*value, big_value);

    let value = tree.get("smol")?.expect("should exist");
    assert_eq!(&*value, b"small value");

    let new_big_value = b"winter!".repeat(128_000);
    tree.insert("big", &new_big_value, 1);

    let value = tree.get("big")?.expect("should exist");
    assert_eq!(&*value, new_big_value);

    tree.flush_active_memtable(0)?;

    let value = tree.get("big")?.expect("should exist");
    assert_eq!(&*value, new_big_value);

    Ok(())
}

#[cfg(feature = "miniz")]
#[test]
fn blob_tree_simple_compressed_2() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path)
        .compression(lsm_tree::CompressionType::Miniz(10))
        .open_as_blob_tree()?;

    let big_value = b"neptune!".repeat(128_000);

    assert!(tree.get("big")?.is_none());
    tree.insert("big", &big_value, 0);
    tree.insert("smol", "small value", 0);

    let value = tree.get("big")?.expect("should exist");
    assert_eq!(&*value, big_value);

    tree.flush_active_memtable(0)?;

    let value = tree.get("big")?.expect("should exist");
    assert_eq!(&*value, big_value);

    let value = tree.get("smol")?.expect("should exist");
    assert_eq!(&*value, b"small value");

    let new_big_value = b"winter!".repeat(128_000);
    tree.insert("big", &new_big_value, 1);

    let value = tree.get("big")?.expect("should exist");
    assert_eq!(&*value, new_big_value);

    tree.flush_active_memtable(0)?;

    let value = tree.get("big")?.expect("should exist");
    assert_eq!(&*value, new_big_value);

    Ok(())
}
