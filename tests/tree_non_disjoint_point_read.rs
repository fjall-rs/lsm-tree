use lsm_tree::{AbstractTree, Config};
use test_log::test;

#[test]
fn tree_non_disjoint_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("c", "c", 0);
    tree.insert("z", "z", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("a", "a", 0);
    tree.insert("d", "d", 0);
    tree.insert("z", "z", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("a", "a", 0);
    tree.insert("e", "e", 0);
    tree.insert("z", "z", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("a", "a", 0);
    tree.insert("f", "f", 0);
    tree.insert("z", "z", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("a", "a", 0);
    tree.insert("g", "g", 0);
    tree.insert("z", "z", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("a", "a", 0);
    tree.insert("h", "h", 0);
    tree.insert("z", "z", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("a", "a", 0);
    tree.insert("z", "z", 0);
    tree.flush_active_memtable(0)?;

    tree.get("c").unwrap().unwrap();
    tree.get("d").unwrap().unwrap();
    tree.get("e").unwrap().unwrap();
    tree.get("f").unwrap().unwrap();
    tree.get("g").unwrap().unwrap();
    tree.get("h").unwrap().unwrap();

    Ok(())
}
