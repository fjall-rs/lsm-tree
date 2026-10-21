use lsm_tree::{get_tmp_folder, AbstractTree, Guard, MemoryVersionSetProvider, SeqNo};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_simple_flush_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("smol", "small value", 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);
        assert_eq!(
            big_value.len() as u32,
            tree.size_of("big", SeqNo::MAX)?.unwrap(),
        );

        version_set_provider.flush_active_memtable(&tree, 1, 0)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"small value");

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.insert("big", &new_big_value, 2);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        version_set_provider.flush_active_memtable(&tree, 3, 0)?;

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);

        let value = tree.get("big", 1)?.expect("should exist");
        assert_eq!(&*value, big_value);

        let g = tree.prefix("big", 1, None).next().expect("should exist");
        assert_eq!(big_value.len() as u32, g.size()?);
    }

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        let value = tree.get("smol", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, b"small value");

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, new_big_value);
    }

    Ok(())
}
