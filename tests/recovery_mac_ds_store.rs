use lsm_tree::{get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider};
use std::sync::Arc;
use test_log::test;

#[test]
fn recovery_mac_ds_store() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;
        tree.insert("a", "a", 0);

        version_set_provider.flush_active_memtable(&tree, 1, 0)?;

        assert_eq!(1, tree.table_count());
    }

    let ds_store = folder.path().join("tables").join(".DS_Store");
    std::fs::File::create(&ds_store)?;
    assert!(ds_store.try_exists()?);

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;
        assert_eq!(1, tree.table_count());
    }
    assert!(ds_store.try_exists()?);

    Ok(())
}
