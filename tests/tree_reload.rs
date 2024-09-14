use lsm_tree::{AbstractTree, Config, SequenceNumberCounter, TreeType};
use std::fs::File;
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
fn tree_reload_empty() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter().flatten().count(), 0);
        assert_eq!(tree.iter().rev().flatten().count(), 0);
        assert_eq!(tree.tree_type(), TreeType::Standard);
    }

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter().flatten().count(), 0);
        assert_eq!(tree.iter().rev().flatten().count(), 0);
        assert_eq!(tree.tree_type(), TreeType::Standard);

        tree.flush_active_memtable(0)?;
    }

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter().flatten().count(), 0);
        assert_eq!(tree.iter().rev().flatten().count(), 0);
        assert_eq!(tree.tree_type(), TreeType::Standard);
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

        tree.flush_active_memtable(0)?;

        for x in 0..ITEM_COUNT as u64 {
            let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), seqno.next());
        }

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
        assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);

        tree.flush_active_memtable(0)?;
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

    let segments_folder = path.join("segments");
    let file0 = segments_folder.join("63364");
    let file1 = segments_folder.join("tmp_633244");

    std::fs::create_dir_all(segments_folder)?;
    File::create(&file0)?;
    File::create(&file1)?;

    assert!(file0.try_exists()?);
    assert!(file1.try_exists()?);

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
