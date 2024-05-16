use lsm_tree::{Config, SequenceNumberCounter};
use std::fs::File;
use test_log::test;

const ITEM_COUNT: usize = 100_000;

#[test]
fn tree_reload_empty() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter().flatten().count(), 0);
        assert_eq!(tree.iter().rev().flatten().count(), 0);
    }

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter().flatten().count(), 0);
        assert_eq!(tree.iter().rev().flatten().count(), 0);
    }

    Ok(())
}

#[test]
fn tree_reload() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();

    {
        let tree = Config::new(&folder).open()?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), seqno.next());
        }

        tree.flush_active_memtable()?;

        for x in 0..ITEM_COUNT as u64 {
            let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), seqno.next());
        }

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
        assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);

        tree.flush_active_memtable()?;
    }

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
        assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
    }

    std::thread::sleep(std::time::Duration::from_secs(2));

    Ok(())
}

#[test]
fn tree_remove_unfinished_segments() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let file0 = path.join("segments").join("63364");
    let file1 = path.join("segments").join("tmp_633244");

    File::create(&file0)?;
    File::create(&file1)?;

    // Setup tree
    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter().flatten().count(), 0);
        assert_eq!(tree.iter().rev().flatten().count(), 0);
    }

    // Recover tree
    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter().flatten().count(), 0);
        assert_eq!(tree.iter().rev().flatten().count(), 0);
    }

    assert!(!file0.try_exists()?);
    assert!(!file1.try_exists()?);

    Ok(())
}
