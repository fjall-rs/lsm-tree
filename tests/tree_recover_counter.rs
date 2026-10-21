use lsm_tree::{get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_recover_table_counter() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let counter_expected = {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

        tree.insert("a", "a", 0);
        version_set_provider.flush_active_memtable(&tree, 1, 0)?;

        tree.insert("b", "b", 2);
        version_set_provider.flush_active_memtable(&tree, 3, 0)?;

        tree.next_table_id()
    };

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;
        assert_eq!(counter_expected, tree.next_table_id());
    }

    Ok(())
}
