use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn snapshot_after_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).open()?;

    let seqno = SequenceNumberCounter::default();

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    let snapshot_seqno = seqno.get();

    assert_eq!(tree.len(SeqNo::MAX, None)?, tree.len(snapshot_seqno, None)?);

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    assert_eq!(ITEM_COUNT, tree.len(snapshot_seqno, None)?);

    Ok(())
}
