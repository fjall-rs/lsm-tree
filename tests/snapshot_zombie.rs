use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

const ITEM_COUNT: usize = 5;

#[test]
fn snapshot_zombie_memtable() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    assert_eq!(ITEM_COUNT, tree.len(SeqNo::MAX, None)?);
    assert_eq!(ITEM_COUNT, tree.iter(SeqNo::MAX, None).rev().count());

    {
        let snapshot_seqno = seqno.get();
        assert_eq!(ITEM_COUNT, tree.len(snapshot_seqno, None)?);
        assert_eq!(ITEM_COUNT, tree.iter(snapshot_seqno, None).rev().count());
    }

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.remove(key, seqno.next());
    }

    assert_eq!(0, tree.len(SeqNo::MAX, None)?);
    assert_eq!(0, tree.iter(SeqNo::MAX, None).rev().count());

    {
        let snapshot_seqno = seqno.get();
        assert_eq!(0, tree.len(snapshot_seqno, None)?);
        assert_eq!(0, tree.iter(snapshot_seqno, None).rev().count());
    }

    Ok(())
}

#[test]
fn snapshot_zombie_table() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, "abc".as_bytes(), seqno.next());
        }

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(ITEM_COUNT, tree.len(SeqNo::MAX, None)?);
        assert_eq!(ITEM_COUNT, tree.iter(SeqNo::MAX, None).rev().count());

        {
            let snapshot_seqno = seqno.get();
            assert_eq!(ITEM_COUNT, tree.len(snapshot_seqno, None)?);
            assert_eq!(ITEM_COUNT, tree.iter(snapshot_seqno, None).rev().count());
        }

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.remove(key, seqno.next());
        }

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(0, tree.len(SeqNo::MAX, None)?);
        assert_eq!(0, tree.iter(SeqNo::MAX, None).rev().count());

        {
            let snapshot_seqno = seqno.get();
            assert_eq!(0, tree.len(snapshot_seqno, None)?);
            assert_eq!(0, tree.iter(snapshot_seqno, None).rev().count());
            assert_eq!(0, tree.prefix(b"", snapshot_seqno, None).count());
        }
    }

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

        assert_eq!(0, tree.len(SeqNo::MAX, None)?);
        assert_eq!(0, tree.iter(SeqNo::MAX, None).rev().count());

        {
            let snapshot_seqno = seqno.get();
            assert_eq!(0, tree.len(snapshot_seqno, None)?);
            assert_eq!(0, tree.iter(snapshot_seqno, None).rev().count());
            assert_eq!(0, tree.prefix(b"", snapshot_seqno, None).count());
        }
    }

    Ok(())
}
