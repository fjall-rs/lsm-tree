use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter,
};
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn table_range_out_of_bounds_lo() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, SequenceNumberCounter::default())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    for key in ('h'..='o').map(|c| c.to_string()) {
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    assert_eq!(4, tree.range(..="k", SeqNo::MAX, None).count());
    assert_eq!(4, tree.range(..="k", SeqNo::MAX, None).rev().count());

    assert_eq!(4, tree.range("0"..="k", SeqNo::MAX, None).count());
    assert_eq!(4, tree.range("0"..="k", SeqNo::MAX, None).rev().count());

    Ok(())
}

#[test]
fn table_range_out_of_bounds_hi() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, SequenceNumberCounter::default())
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    assert_eq!(
        50,
        tree.range((50u64.to_be_bytes()).., SeqNo::MAX, None)
            .count()
    );
    assert_eq!(
        50,
        tree.range((50u64.to_be_bytes()).., SeqNo::MAX, None)
            .rev()
            .count()
    );

    assert_eq!(
        50,
        tree.range(
            (50u64.to_be_bytes())..(150u64.to_be_bytes()),
            SeqNo::MAX,
            None
        )
        .count()
    );
    assert_eq!(
        50,
        tree.range(
            (50u64.to_be_bytes())..(150u64.to_be_bytes()),
            SeqNo::MAX,
            None
        )
        .rev()
        .count()
    );

    Ok(())
}
