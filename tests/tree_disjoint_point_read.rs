use lsm_tree::Config;
use std::sync::Arc;
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

#[test]
fn tree_disjoint_point_read_multiple_levels() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    tree.insert("z", "z", 0);
    tree.flush_active_memtable()?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable()?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable()?;

    tree.insert("d", "d", 0);
    tree.flush_active_memtable()?;

    tree.compact(Arc::new(lsm_tree::compaction::SizeTiered::new(10)))?;
    assert_eq!(
        1,
        tree.levels
            .read()
            .expect("asdasd")
            .levels
            .get(1)
            .unwrap()
            .len()
    );

    tree.insert("e", "e", 0);
    tree.flush_active_memtable()?;
    tree.insert("f", "f", 0);
    tree.flush_active_memtable()?;
    tree.insert("g", "g", 0);
    tree.flush_active_memtable()?;
    tree.insert("h", "h", 0);
    tree.flush_active_memtable()?;
    tree.insert("i", "i", 0);
    tree.flush_active_memtable()?;
    tree.insert("j", "j", 0);
    tree.flush_active_memtable()?;

    log::error!("{:#?}", tree.levels.read().expect("asdasd").levels);

    for key in [b"z", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}
