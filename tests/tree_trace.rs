use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_trace() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let key = "1".as_bytes();

    let value = "oldvalue".as_bytes();
    tree.insert(key, value, 0);

    let value = "newvalue".as_bytes();
    tree.insert(key, value, 1);

    tree.flush_active_memtable(0)?;

    let value = "dsadsa".as_bytes();
    tree.insert(key, value, 2);
    tree.rotate_memtable();

    let value = "yxcyxc".as_bytes();
    tree.insert(key, value, 3);
    tree.print_trace(key)?;

    Ok(())
}
