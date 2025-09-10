use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
#[ignore = "restore w/o snapshot API"]
fn snapshot_basic() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // let tree = Config::new(&folder).open()?;

    // let seqno = SequenceNumberCounter::default();

    // for x in 0..ITEM_COUNT as u64 {
    //     let key = x.to_be_bytes();
    //     tree.insert(key, "abc".as_bytes(), seqno.next());
    // }

    // assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    // for x in 0..ITEM_COUNT as u64 {
    //     let key = x.to_be_bytes();
    //     tree.insert(key, "abc".as_bytes(), seqno.next());
    // }

    // assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    // let snapshot = tree.snapshot(seqno.get());

    // assert_eq!(tree.len(SeqNo::MAX, None)?, snapshot.len()?);
    // assert_eq!(tree.len(SeqNo::MAX, None)?, snapshot.iter().rev().count());

    // for x in (ITEM_COUNT as u64)..((ITEM_COUNT * 2) as u64) {
    //     let key = x.to_be_bytes();
    //     tree.insert(key, "abc".as_bytes(), seqno.next());
    // }

    // assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
    // assert_eq!(ITEM_COUNT, snapshot.len()?);
    // assert_eq!(ITEM_COUNT, snapshot.iter().rev().count());

    Ok(())
}
