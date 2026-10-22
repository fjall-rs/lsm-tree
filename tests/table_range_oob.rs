use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn table_range_out_of_bounds_lo() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    let batch_seqno = seqno.next();
    for key in ('h'..='o').map(|c| c.to_string()) {
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), batch_seqno);
    }

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(4, tree.range(..="k", SeqNo::MAX, None).count());
    assert_eq!(4, tree.range(..="k", SeqNo::MAX, None).rev().count());

    assert_eq!(4, tree.range("0"..="k", SeqNo::MAX, None).count());
    assert_eq!(4, tree.range("0"..="k", SeqNo::MAX, None).rev().count());

    Ok(())
}

#[test]
fn table_range_out_of_bounds_hi() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    let batch_seqno = seqno.next();
    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), batch_seqno);
    }

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(
        50,
        tree.range((50u64.to_be_bytes()).., SeqNo::MAX, None)
            .count(),
    );
    assert_eq!(
        50,
        tree.range((50u64.to_be_bytes()).., SeqNo::MAX, None)
            .rev()
            .count(),
    );

    assert_eq!(
        50,
        tree.range(
            (50u64.to_be_bytes())..(150u64.to_be_bytes()),
            SeqNo::MAX,
            None,
        )
        .count()
    );
    assert_eq!(
        50,
        tree.range(
            (50u64.to_be_bytes())..(150u64.to_be_bytes()),
            SeqNo::MAX,
            None,
        )
        .rev()
        .count()
    );

    Ok(())
}
