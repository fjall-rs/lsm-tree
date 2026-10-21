use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn snapshot_basic_memtable() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    let instant = seqno.get();

    assert_eq!(tree.len(SeqNo::MAX, None)?, tree.len(instant, None)?);

    for x in (ITEM_COUNT as u64)..((ITEM_COUNT * 2) as u64) {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
    assert_eq!(ITEM_COUNT, tree.len(instant, None)?);

    Ok(())
}
