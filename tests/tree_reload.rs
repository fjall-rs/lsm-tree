use lsm_tree::{AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter, TreeType};
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
fn tree_reload_smoke_test() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).open()?;
        assert_eq!(0, tree.segment_count());

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(1, tree.segment_count());
        assert!(tree.contains_key("a", SeqNo::MAX)?);
    }

    {
        let tree = Config::new(&folder).open()?;
        assert_eq!(1, tree.segment_count());
        assert!(tree.contains_key("a", SeqNo::MAX)?);
    }

    Ok(())
}

#[test]
fn tree_reload_smoke_test_blob() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let large_value = "a".repeat(10_000);

    {
        let tree = Config::new(&folder)
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert_eq!(0, tree.segment_count());

        tree.insert("a", &large_value, 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(1, tree.segment_count());
        assert!(tree.contains_key("a", SeqNo::MAX)?);
    }

    {
        let tree = Config::new(&folder)
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert_eq!(1, tree.segment_count());
        assert_eq!(large_value.as_bytes(), tree.get("a", SeqNo::MAX)?.unwrap());
    }

    Ok(())
}

#[test]
fn tree_reload_empty() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
        assert_eq!(tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(), 0);
        assert_eq!(
            tree.iter(SeqNo::MAX, None)
                .rev()
                .flat_map(|x| x.key())
                .count(),
            0
        );
        assert_eq!(tree.tree_type(), TreeType::Standard);
    }

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
        assert_eq!(tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(), 0);
        assert_eq!(
            tree.iter(SeqNo::MAX, None)
                .rev()
                .flat_map(|x| x.key())
                .count(),
            0
        );
        assert_eq!(tree.tree_type(), TreeType::Standard);

        tree.flush_active_memtable(0)?;
    }

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
        assert_eq!(tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(), 0);
        assert_eq!(
            tree.iter(SeqNo::MAX, None)
                .rev()
                .flat_map(|x| x.key())
                .count(),
            0
        );
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

        assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
        assert_eq!(
            tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2
        );
        assert_eq!(
            tree.iter(SeqNo::MAX, None)
                .rev()
                .flat_map(|x| x.key())
                .count(),
            ITEM_COUNT * 2
        );

        tree.flush_active_memtable(0)?;
    }

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
        assert_eq!(
            tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2
        );
        assert_eq!(
            tree.iter(SeqNo::MAX, None)
                .rev()
                .flat_map(|x| x.key())
                .count(),
            ITEM_COUNT * 2
        );
    }

    std::thread::sleep(std::time::Duration::from_secs(2));

    Ok(())
}
