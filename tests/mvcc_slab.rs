use lsm_tree::{Config, SequenceNumberCounter};
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
fn segment_reader_mvcc_slab() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).block_size(1_024).open()?;

    let seqno = SequenceNumberCounter::default();

    for _ in 0..ITEM_COUNT {
        tree.insert("a", "", seqno.next());
    }
    tree.insert("b", "", 0);

    tree.flush_active_memtable()?;

    let level_manifest = tree.levels.read().expect("lock is poisoned");

    let segment = level_manifest
        .levels
        .first()
        .expect("should exist")
        .segments
        .first()
        .expect("should exist");

    let reader = segment.iter();
    assert_eq!(reader.count(), ITEM_COUNT + 1);

    Ok(())
}
