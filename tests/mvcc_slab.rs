use lsm_tree::{config::BlockSizePolicy, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn table_reader_mvcc_slab() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    let seqno = SequenceNumberCounter::default();

    for _ in 0..ITEM_COUNT {
        tree.insert("a", "", seqno.next());
    }
    tree.insert("b", "", 0);

    tree.flush_active_memtable(0)?;

    let version = tree.current_version();

    let table = version
        .level(0)
        .expect("level should exist")
        .first()
        .expect("run should exist")
        .first()
        .expect("table should exist");

    let reader = table.iter();
    assert_eq!(reader.count(), ITEM_COUNT + 1);

    Ok(())
}

#[test]
fn table_reader_mvcc_slab_blob() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 1_000;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder)
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .with_kv_separation(Some(Default::default()))
        .open()?;

    let seqno = SequenceNumberCounter::default();

    for _ in 0..ITEM_COUNT {
        tree.insert("a", "neptune".repeat(10_000), seqno.next());
    }
    tree.insert("b", "", 0);

    tree.flush_active_memtable(0)?;

    let version = tree.current_version();

    let table = version
        .level(0)
        .expect("level should exist")
        .first()
        .expect("run should exist")
        .first()
        .expect("table should exist");

    let reader = table.iter();
    assert_eq!(reader.count(), ITEM_COUNT + 1);

    Ok(())
}
