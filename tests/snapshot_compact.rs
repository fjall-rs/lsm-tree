use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
#[ignore = "restore w/o snapshot API"]
fn snapshot_after_compaction() -> lsm_tree::Result<()> {
    // let folder = tempfile::tempdir()?;

    // let tree = Config::new(&folder).open()?;

    // let seqno = SequenceNumberCounter::default();

    // for x in 0..ITEM_COUNT as u64 {
    //     let key = x.to_be_bytes();
    //     tree.insert(key, "abc".as_bytes(), seqno.next());
    // }

    // assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    // let snapshot_seqno = seqno.get();
    // let snapshot = tree.snapshot(snapshot_seqno);

    // assert_eq!(tree.len(SeqNo::MAX, None)?, snapshot.len()?);
    // assert_eq!(tree.len(SeqNo::MAX, None)?, snapshot.iter().rev().count());

    // for x in 0..ITEM_COUNT as u64 {
    //     let key = x.to_be_bytes();
    //     tree.insert(key, "abc".as_bytes(), seqno.next());
    // }

    // tree.flush_active_memtable(0)?;
    // tree.major_compact(u64::MAX, 0)?;

    // assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    // assert_eq!(ITEM_COUNT, snapshot.len()?);
    // assert_eq!(ITEM_COUNT, snapshot.iter().rev().count());

    Ok(())
}
