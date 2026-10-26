use super::*;
use crate::{version_set::MemoryVersionSetProvider, KvSeparationOptions};
use test_log::test;

#[test]
#[expect(clippy::expect_used)]
fn tree_smoke_test_flush() -> crate::Result<()> {
    let tempdir = tempfile::tempdir()?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = crate::Config::new(&tempdir, version_set_provider.clone()).open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    assert_eq!(Some(b"a".as_slice()), tree.get(b"a", 1)?.as_deref());
    assert_eq!(Some(b"b".as_slice()), tree.get(b"b", 1)?.as_deref());
    assert_eq!(Some(b"c".as_slice()), tree.get(b"c", 1)?.as_deref());
    assert_eq!(3, tree.len(1, None)?);
    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.sealed_memtable_count());

    version_set_provider.rotate_active_memtable(&tree);

    assert_eq!(Some(b"a".as_slice()), tree.get(b"a", 1)?.as_deref());
    assert_eq!(Some(b"b".as_slice()), tree.get(b"b", 1)?.as_deref());
    assert_eq!(Some(b"c".as_slice()), tree.get(b"c", 1)?.as_deref());
    assert_eq!(3, tree.len(1, None)?);
    assert_eq!(0, tree.table_count());
    assert_eq!(1, tree.sealed_memtable_count());

    let flush_result = tree.new_flush(0)?.expect("should flush to table files");
    version_set_provider.register_flush(flush_result, 1);

    assert_eq!(Some(b"a".as_slice()), tree.get(b"a", 2)?.as_deref());
    assert_eq!(Some(b"b".as_slice()), tree.get(b"b", 2)?.as_deref());
    assert_eq!(Some(b"c".as_slice()), tree.get(b"c", 2)?.as_deref());
    assert_eq!(3, tree.len(2, None)?);
    assert_eq!(1, tree.table_count());
    assert_eq!(0, tree.sealed_memtable_count());

    Ok(())
}

#[test]
#[expect(clippy::expect_used)]
fn tree_smoke_test_flush_many_sealed() -> crate::Result<()> {
    let tempdir = tempfile::tempdir()?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = crate::Config::new(&tempdir, version_set_provider.clone()).open()?;

    tree.insert("a", "a", 0);
    version_set_provider.rotate_active_memtable(&tree);

    tree.insert("b", "b", 0);
    version_set_provider.rotate_active_memtable(&tree);

    tree.insert("c", "c", 0);
    version_set_provider.rotate_active_memtable(&tree);

    assert_eq!(Some(b"a".as_slice()), tree.get(b"a", 1)?.as_deref());
    assert_eq!(Some(b"b".as_slice()), tree.get(b"b", 1)?.as_deref());
    assert_eq!(Some(b"c".as_slice()), tree.get(b"c", 1)?.as_deref());
    assert_eq!(3, tree.len(1, None)?);
    assert_eq!(0, tree.table_count());
    assert_eq!(3, tree.sealed_memtable_count());

    let flush_result = tree.new_flush(0)?.expect("should flush to table files");
    version_set_provider.register_flush(flush_result, 1);

    assert_eq!(Some(b"a".as_slice()), tree.get(b"a", 2)?.as_deref());
    assert_eq!(Some(b"b".as_slice()), tree.get(b"b", 2)?.as_deref());
    assert_eq!(Some(b"c".as_slice()), tree.get(b"c", 2)?.as_deref());
    assert_eq!(3, tree.len(2, None)?);
    assert_eq!(1, tree.table_count());
    assert_eq!(0, tree.sealed_memtable_count());

    Ok(())
}

#[test]
#[expect(clippy::expect_used)]
fn tree_smoke_test_flush_blob() -> crate::Result<()> {
    let tempdir = tempfile::tempdir()?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = crate::Config::new(&tempdir, version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(2)))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "bbbbb", 0);
    tree.insert("c", "c", 0);

    assert_eq!(Some(b"a".as_slice()), tree.get(b"a", 1)?.as_deref());
    assert_eq!(Some(b"bbbbb".as_slice()), tree.get(b"b", 1)?.as_deref());
    assert_eq!(Some(b"c".as_slice()), tree.get(b"c", 1)?.as_deref());
    assert_eq!(3, tree.len(1, None)?);
    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.blob_file_count());
    assert_eq!(0, tree.sealed_memtable_count());

    version_set_provider.rotate_active_memtable(&tree);

    assert_eq!(Some(b"a".as_slice()), tree.get(b"a", 1)?.as_deref());
    assert_eq!(Some(b"bbbbb".as_slice()), tree.get(b"b", 1)?.as_deref());
    assert_eq!(Some(b"c".as_slice()), tree.get(b"c", 1)?.as_deref());
    assert_eq!(3, tree.len(1, None)?);
    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.blob_file_count());
    assert_eq!(1, tree.sealed_memtable_count());

    let flush_result = tree.new_flush(0)?.expect("should flush to table files");
    version_set_provider.register_flush(flush_result, 1);

    assert_eq!(Some(b"a".as_slice()), tree.get(b"a", 2)?.as_deref());
    assert_eq!(Some(b"bbbbb".as_slice()), tree.get(b"b", 2)?.as_deref());
    assert_eq!(Some(b"c".as_slice()), tree.get(b"c", 2)?.as_deref());
    assert_eq!(3, tree.len(2, None)?);
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());
    assert_eq!(0, tree.sealed_memtable_count());

    Ok(())
}

#[test]
fn tree_smoke_test_major_compact() -> crate::Result<()> {
    let tempdir = tempfile::tempdir()?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = crate::Config::new(&tempdir, version_set_provider.clone()).open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    assert_eq!(3, tree.len(2, None)?);
    assert_eq!(1, tree.table_count());
    assert_eq!(0, tree.sealed_memtable_count());

    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    tree.insert("f", "f", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    assert_eq!(6, tree.len(2, None)?);
    assert_eq!(2, tree.table_count());
    assert_eq!(0, tree.sealed_memtable_count());

    version_set_provider.compact(&tree, Arc::new(crate::compaction::Major::default()), 2, 0)?;

    assert_eq!(6, tree.len(2, None)?);
    assert_eq!(1, tree.table_count());
    assert_eq!(0, tree.sealed_memtable_count());

    Ok(())
}
