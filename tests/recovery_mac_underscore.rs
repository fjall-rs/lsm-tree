use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn recovery_mac_underscore_file() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;
        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
    }

    let ds_store = folder.path().join("tables").join("._0");
    std::fs::File::create(&ds_store)?;
    assert!(ds_store.try_exists()?);

    {
        let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;
        assert_eq!(1, tree.table_count());
    }
    assert!(ds_store.try_exists()?);

    Ok(())
}
