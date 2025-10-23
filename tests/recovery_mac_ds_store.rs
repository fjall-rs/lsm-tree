use lsm_tree::{AbstractTree, Config};
use test_log::test;

#[test]
fn recovery_mac_ds_store() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    {
        let tree = Config::new(&folder).open()?;
        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.segment_count());
    }

    let ds_store = folder.join("tables").join(".DS_Store");
    std::fs::File::create(&ds_store)?;
    assert!(ds_store.try_exists()?);

    {
        let tree = Config::new(&folder).open()?;
        assert_eq!(1, tree.segment_count());
    }
    assert!(ds_store.try_exists()?);

    Ok(())
}
