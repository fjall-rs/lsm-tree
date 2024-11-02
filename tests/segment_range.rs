use lsm_tree::{AbstractTree, Config};
use test_log::test;

const ITEM_COUNT: usize = 1_000_000;

#[test]
fn segment_ranges() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder)
        .data_block_size(1_024)
        .index_block_size(1_024)
        .open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    let iter = tree.range(1_000u64.to_be_bytes()..11_000u64.to_be_bytes());
    assert_eq!(10_000, iter.count());

    let iter = tree.range(1_000u64.to_be_bytes()..11_000u64.to_be_bytes());
    assert_eq!(10_000, iter.rev().count());

    let mut iter = tree.range(1_000u64.to_be_bytes()..11_000u64.to_be_bytes());
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
