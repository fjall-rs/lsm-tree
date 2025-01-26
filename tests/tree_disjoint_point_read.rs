mod common;

use common::TestPrefixExtractor;
use lsm_tree::{AbstractTree, Config};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_disjoint_point_read_with_prefix_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .prefix_extractor(Arc::new(TestPrefixExtractor::new(3)))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    tree.flush_active_memtable(0)?;

    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    tree.insert("f", "f", 0);
    tree.insert("aa", "aa", 0);
    tree.insert("aac", "aac", 0);
    tree.insert("aacd", "aacd", 0);
    tree.insert("aabd", "aabd", 0);

    tree.flush_active_memtable(0)?;

    let keys = [
        b"a".to_vec(),
        b"b".to_vec(),
        b"c".to_vec(),
        b"d".to_vec(),
        b"e".to_vec(),
        b"f".to_vec(),
        b"aa".to_vec(),
        b"aac".to_vec(),
        b"aacd".to_vec(),
        b"aabd".to_vec(),
    ];
    for key in keys {
        let value = tree.get(&key).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}

#[test]
fn tree_disjoint_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    tree.flush_active_memtable(0)?;

    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    tree.insert("f", "f", 0);

    tree.flush_active_memtable(0)?;

    for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}

#[test]
fn tree_disjoint_point_read_blob() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .open_as_blob_tree()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    tree.flush_active_memtable(0)?;

    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    tree.insert("f", "f", 0);

    tree.flush_active_memtable(0)?;

    for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}

#[test]
fn tree_disjoint_point_read_multiple_levels() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .open()?;

    tree.insert("z", "z", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("d", "d", 0);
    tree.flush_active_memtable(0)?;

    tree.compact(Arc::new(lsm_tree::compaction::SizeTiered::new(10, 8)), 1)?;
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
    tree.flush_active_memtable(0)?;
    tree.insert("f", "f", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("g", "g", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("h", "h", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("i", "i", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("j", "j", 0);
    tree.flush_active_memtable(0)?;

    for key in [b"z", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}

#[test]
fn tree_disjoint_point_read_multiple_levels_blob() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .open_as_blob_tree()?;

    tree.insert("z", "z", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("d", "d", 0);
    tree.flush_active_memtable(0)?;

    tree.compact(Arc::new(lsm_tree::compaction::SizeTiered::new(10, 8)), 1)?;
    assert_eq!(
        1,
        tree.index
            .levels
            .read()
            .expect("asdasd")
            .levels
            .get(1)
            .unwrap()
            .len()
    );

    tree.insert("e", "e", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("f", "f", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("g", "g", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("h", "h", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("i", "i", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("j", "j", 0);
    tree.flush_active_memtable(0)?;

    for key in [b"z", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}
