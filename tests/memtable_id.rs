use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

// https://github.com/fjall-rs/lsm-tree/issues/230
#[test]
fn memtable_id() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "a", seqno.next());
    assert_eq!(0, tree.current_super_version().active_memtable.id());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.current_super_version().active_memtable.id());

    Ok(())
}
