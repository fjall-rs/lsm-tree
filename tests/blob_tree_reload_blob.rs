use lsm_tree::{AbstractTree, Config, SequenceNumberCounter, TreeType};
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
#[ignore]
fn blob_tree_reload_empty() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).open_as_blob_tree()?;

        assert_eq!(tree.len(None, None)?, 0);
        assert_eq!(tree.iter(None, None).flatten().count(), 0);
        assert_eq!(tree.iter(None, None).rev().flatten().count(), 0);
        assert_eq!(tree.tree_type(), TreeType::Blob);
    }

    {
        let tree = Config::new(&folder).open_as_blob_tree()?;

        assert_eq!(tree.len(None, None)?, 0);
        assert_eq!(tree.iter(None, None).flatten().count(), 0);
        assert_eq!(tree.iter(None, None).rev().flatten().count(), 0);
        assert_eq!(tree.tree_type(), TreeType::Blob);

        tree.flush_active_memtable(0)?;
    }

    {
        let tree = Config::new(&folder).open_as_blob_tree()?;

        assert_eq!(tree.len(None, None)?, 0);
        assert_eq!(tree.iter(None, None).flatten().count(), 0);
        assert_eq!(tree.iter(None, None).rev().flatten().count(), 0);
        assert_eq!(tree.tree_type(), TreeType::Blob);
    }

    Ok(())
}

#[test]
#[ignore]
fn blob_tree_reload() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();

    {
        let tree = Config::new(&folder).open_as_blob_tree()?;

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

        assert_eq!(tree.len(None, None)?, ITEM_COUNT * 2);
        assert_eq!(tree.iter(None, None).flatten().count(), ITEM_COUNT * 2);
        assert_eq!(
            tree.iter(None, None).rev().flatten().count(),
            ITEM_COUNT * 2
        );

        tree.flush_active_memtable(0)?;
    }

    {
        let tree = Config::new(&folder).open_as_blob_tree()?;

        assert_eq!(tree.len(None, None)?, ITEM_COUNT * 2);
        assert_eq!(tree.iter(None, None).flatten().count(), ITEM_COUNT * 2);
        assert_eq!(
            tree.iter(None, None).rev().flatten().count(),
            ITEM_COUNT * 2
        );
    }

    Ok(())
}
