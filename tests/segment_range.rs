use lsm_tree::{config::BlockSizePolicy, AbstractTree, Config, Guard, SeqNo};
use test_log::test;

const ITEM_COUNT: usize = 1_000_000;

#[test]
fn segment_ranges() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder)
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    let iter = tree.range(
        1_000u64.to_be_bytes()..11_000u64.to_be_bytes(),
        SeqNo::MAX,
        None,
    );
    assert_eq!(10_000, iter.count());

    let iter = tree.range(
        1_000u64.to_be_bytes()..11_000u64.to_be_bytes(),
        SeqNo::MAX,
        None,
    );
    assert_eq!(10_000, iter.rev().count());

    let mut iter = tree.range(
        1_000u64.to_be_bytes()..11_000u64.to_be_bytes(),
        SeqNo::MAX,
        None,
    );
    let mut count = 0;

    for x in 0.. {
        if x % 2 == 0 {
            let Some(_) = iter.next() else {
                break;
            };

            count += 1;
        } else {
            let Some(_) = iter.next_back() else {
                break;
            };

            count += 1;
        }
    }

    assert_eq!(10_000, count);

    Ok(())
}

#[test]
fn segment_range_last_back() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder)
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    let value = (0..2_000).map(|_| 0).collect::<Vec<u8>>();

    for x in 0..10_u64 {
        let key = x.to_be_bytes();
        tree.insert(key, &value, 0);
    }
    tree.flush_active_memtable(0)?;

    let iter = tree.range(0u64.to_be_bytes()..10u64.to_be_bytes(), SeqNo::MAX, None);
    assert_eq!(10, iter.count());

    let iter = tree.range(0u64.to_be_bytes()..10u64.to_be_bytes(), SeqNo::MAX, None);
    assert_eq!(10, iter.rev().count());

    let mut iter = tree.range(0u64.to_be_bytes()..5u64.to_be_bytes(), SeqNo::MAX, None);

    assert_eq!(0u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(1u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(2u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(3u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(4u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert!(iter.next_back().is_none());

    Ok(())
}

#[test]
fn segment_range_last_back_2() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder)
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    let value = (0..2_000).map(|_| 0).collect::<Vec<u8>>();

    for x in 0..10_u64 {
        let key = x.to_be_bytes();
        tree.insert(key, &value, 0);
    }
    tree.insert(10u64.to_be_bytes(), [], 0);
    tree.insert(11u64.to_be_bytes(), [], 0);
    tree.flush_active_memtable(0)?;

    let iter = tree.range(0u64.to_be_bytes()..10u64.to_be_bytes(), SeqNo::MAX, None);
    assert_eq!(10, iter.count());

    let iter = tree.range(0u64.to_be_bytes()..10u64.to_be_bytes(), SeqNo::MAX, None);
    assert_eq!(10, iter.rev().count());

    let mut iter = tree.range(0u64.to_be_bytes()..12u64.to_be_bytes(), SeqNo::MAX, None);

    assert_eq!(0u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(1u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(2u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(3u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(4u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(5u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(6u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(7u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(8u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(9u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(10u64.to_be_bytes(), &*iter.next().unwrap().key()?);
    assert_eq!(11u64.to_be_bytes(), &*iter.next_back().unwrap().key()?);
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());

    Ok(())
}
