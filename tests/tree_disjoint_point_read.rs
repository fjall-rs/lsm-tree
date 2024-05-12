use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_disjoint_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    tree.flush_active_memtable()?;

    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    tree.insert("f", "f", 0);

    tree.flush_active_memtable()?;

    for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}
