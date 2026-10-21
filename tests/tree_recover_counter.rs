use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_recover_table_counter() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let counter_expected = {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

        tree.insert("a", "a", seqno.next());
        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        tree.insert("b", "b", seqno.next());
        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        tree.next_table_id()
    };

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;
        assert_eq!(counter_expected, tree.next_table_id());
    }

    Ok(())
}
