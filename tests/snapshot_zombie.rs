use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

const ITEM_COUNT: usize = 5;

#[test]
fn snapshot_zombie_memtable() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).block_size(1_024).open()?;

    let seqno = SequenceNumberCounter::default();

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert_eq!(tree.iter().rev().count(), ITEM_COUNT);

    {
        let snapshot = tree.snapshot(seqno.get());
        assert_eq!(ITEM_COUNT, snapshot.len()?);
        assert_eq!(ITEM_COUNT, snapshot.iter().rev().count());
    }

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.remove(key, seqno.next());
    }

    assert_eq!(tree.len()?, 0);
    assert_eq!(tree.iter().rev().count(), 0);

    {
        let snapshot = tree.snapshot(seqno.get());
        assert_eq!(0, snapshot.len()?);
        assert_eq!(0, snapshot.iter().rev().count());
        assert_eq!(0, snapshot.prefix("".as_bytes()).count());
    }

    Ok(())
}

#[test]
fn snapshot_zombie_segment() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();

    {
        let tree = Config::new(&folder).block_size(1_024).open()?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, "abc".as_bytes(), seqno.next());
        }

        tree.flush_active_memtable(0)?;

        assert_eq!(tree.len()?, ITEM_COUNT);
        assert_eq!(tree.iter().rev().count(), ITEM_COUNT);

        {
            let snapshot = tree.snapshot(seqno.get());
            assert_eq!(ITEM_COUNT, snapshot.len()?);
            assert_eq!(ITEM_COUNT, snapshot.iter().rev().count());
        }

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.remove(key, seqno.next());
        }

        tree.flush_active_memtable(0)?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter().rev().count(), 0);

        {
            let snapshot = tree.snapshot(seqno.get());
            assert_eq!(0, snapshot.len()?);
            assert_eq!(0, snapshot.iter().rev().count());
            assert_eq!(0, snapshot.prefix("".as_bytes()).count());
        }
    }

    {
        let tree = Config::new(&folder).block_size(1_024).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter().rev().count(), 0);

        {
            let snapshot = tree.snapshot(seqno.get());
            assert_eq!(0, snapshot.len()?);
            assert_eq!(0, snapshot.iter().rev().count());
            assert_eq!(0, snapshot.prefix("".as_bytes()).count());
        }
    }

    Ok(())
}
