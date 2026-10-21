use lsm_tree::{get_tmp_folder, AbstractTree, MemoryVersionSetProvider, SeqNo};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_recovery() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(4_096);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        tree.insert("big", &big_value, 0);
        version_set_provider.flush_active_memtable(&tree, 1, 0)?;

        tree.insert("big", &big_value, 2);
        version_set_provider.flush_active_memtable(&tree, 3, 0)?;

        tree.insert("big", &big_value, 4);
        version_set_provider.flush_active_memtable(&tree, 5, 0)?;

        tree.insert("big", &big_value, 6);
        version_set_provider.flush_active_memtable(&tree, 7, 0)?;

        tree.insert("big", &big_value, 8);
        version_set_provider.flush_active_memtable(&tree, 9, 0)?;

        assert!(!tree.is_empty(SeqNo::MAX, None)?);
        assert_eq!(5, tree.blob_file_count());
    }

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(!tree.is_empty(SeqNo::MAX, None)?);
        assert_eq!(5, tree.blob_file_count());
    }

    Ok(())
}
