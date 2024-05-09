use lsm_tree::Config;
use test_log::test;

const ITEM_COUNT: usize = 1_000;

#[test]
fn segment_point_reads() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        assert!(tree.contains_key(key)?, "{key:?} not found");
    }

    Ok(())
}

// TODO: MVCC (get latest)
