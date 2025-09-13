use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

const ITEM_COUNT: usize = 5;

#[test]
fn snapshot_zombie_memtable() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).open()?;

    let seqno = SequenceNumberCounter::default();

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert_eq!(tree.iter(SeqNo::MAX, None).rev().count(), ITEM_COUNT);

    {
        let snapshot_seqno = seqno.get();
        assert_eq!(ITEM_COUNT, tree.len(snapshot_seqno, None)?);
        assert_eq!(ITEM_COUNT, tree.iter(snapshot_seqno, None).rev().count());
    }

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.remove(key, seqno.next());
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert_eq!(tree.iter(SeqNo::MAX, None).rev().count(), 0);

    {
        let snapshot_seqno = seqno.get();
        assert_eq!(0, tree.len(snapshot_seqno, None)?);
        assert_eq!(0, tree.iter(snapshot_seqno, None).rev().count());
    }

    Ok(())
}

#[test]
fn snapshot_zombie_segment() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();

    {
        let tree = Config::new(&folder).open()?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, "abc".as_bytes(), seqno.next());
        }

        tree.flush_active_memtable(0)?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
        assert_eq!(tree.iter(SeqNo::MAX, None).rev().count(), ITEM_COUNT);

        {
            let snapshot_seqno = seqno.get();
            assert_eq!(ITEM_COUNT, tree.len(snapshot_seqno, None)?);
            assert_eq!(ITEM_COUNT, tree.iter(snapshot_seqno, None).rev().count());
        }

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.remove(key, seqno.next());
        }

        tree.flush_active_memtable(0)?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
        assert_eq!(tree.iter(SeqNo::MAX, None).rev().count(), 0);

        {
            let snapshot_seqno = seqno.get();
            assert_eq!(0, tree.len(snapshot_seqno, None)?);
            assert_eq!(0, tree.iter(snapshot_seqno, None).rev().count());
            assert_eq!(0, tree.prefix(b"", snapshot_seqno, None).count());
        }
    }

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
        assert_eq!(tree.iter(SeqNo::MAX, None).rev().count(), 0);

        {
            let snapshot_seqno = seqno.get();
            assert_eq!(0, tree.len(snapshot_seqno, None)?);
            assert_eq!(0, tree.iter(snapshot_seqno, None).rev().count());
            assert_eq!(0, tree.prefix(b"", snapshot_seqno, None).count());
        }
    }

    Ok(())
}
