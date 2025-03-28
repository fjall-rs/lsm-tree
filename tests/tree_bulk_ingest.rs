use lsm_tree::{AbstractTree, Config};
use test_log::test;

const ITEM_COUNT: usize = 100_000;

#[test]
fn tree_bulk_ingest() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder).open()?;

    tree.ingest((0..ITEM_COUNT as u64).map(|x| {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        (k.into(), v.into())
    }))?;

    assert_eq!(tree.len(None, None)?, ITEM_COUNT);
    assert_eq!(
        tree.iter(None, None).filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.iter(None, None).rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );

    Ok(())
}

#[test]
fn tree_copy() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let src = Config::new(folder).open()?;

    src.ingest((0..ITEM_COUNT as u64).map(|x| {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        (k.into(), v.into())
    }))?;

    assert_eq!(src.len(None, None)?, ITEM_COUNT);
    assert_eq!(
        src.iter(None, None).filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        src.iter(None, None).rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert!(src.lock_active_memtable().is_empty());

    let folder = tempfile::tempdir()?;
    let dest = Config::new(folder).open()?;

    dest.ingest(src.iter(None, None).map(|kv| {
        let (k, v) = kv.unwrap();
        (k, v)
    }))?;

    assert_eq!(dest.len(None, None)?, ITEM_COUNT);
    assert_eq!(
        dest.iter(None, None).filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        dest.iter(None, None).rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert!(dest.lock_active_memtable().is_empty());

    Ok(())
}

/* #[test]
fn blob_tree_bulk_ingest() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder)
        .blob_file_separation_threshold(1)
        .open()?;

    tree.ingest((0..ITEM_COUNT as u64).map(|x| {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        (k.into(), v.into())
    }))?;

    assert_eq!(tree.len(None, None)?, ITEM_COUNT);
    assert_eq!(
        tree.iter(None, None).filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.iter(None, None).rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(1, tree.blob_file_count());

    Ok(())
}

#[test]
fn blob_tree_copy() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let src = Config::new(folder)
        .blob_file_separation_threshold(1)
        .open_as_blob_tree()?;

    src.ingest((0..ITEM_COUNT as u64).map(|x| {
        let k = x.to_be_bytes();
        let v = nanoid::nanoid!();
        (k.into(), v.into())
    }))?;

    assert_eq!(src.len(None, None)?, ITEM_COUNT);
    assert_eq!(
        src.iter(None, None).filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        src.iter(None, None).rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert!(src.lock_active_memtable().is_empty());
    assert_eq!(1, src.blob_file_count());

    let folder = tempfile::tempdir()?;
    let dest = Config::new(folder)
        .blob_file_separation_threshold(1)
        .open_as_blob_tree()?;

    dest.ingest(src.iter(None, None).map(|kv| {
        let (k, v) = kv.unwrap();
        (k, v)
    }))?;

    assert_eq!(dest.len(None, None)?, ITEM_COUNT);
    assert_eq!(
        dest.iter(None, None).filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        dest.iter(None, None).rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert!(dest.lock_active_memtable().is_empty());
    assert_eq!(1, dest.blob_file_count());

    Ok(())
} */
