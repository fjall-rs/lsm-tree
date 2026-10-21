use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter, TreeType,
};
use std::sync::Arc;
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
fn tree_reload_smoke_test() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;
        assert_eq!(0, tree.table_count());

        tree.insert("a", "a", 0);
        version_set_provider.flush_active_memtable(&tree, 1, 0)?;

        assert_eq!(1, tree.table_count());
        assert!(tree.contains_key("a", SeqNo::MAX)?);
    }

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;
        assert_eq!(1, tree.table_count());
        assert!(tree.contains_key("a", SeqNo::MAX)?);
    }

    Ok(())
}

#[test]
fn tree_reload_smoke_test_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let large_value = "a".repeat(10_000);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert_eq!(0, tree.table_count());

        tree.insert("a", &large_value, 0);
        version_set_provider.flush_active_memtable(&tree, 1, 0)?;

        assert_eq!(1, tree.table_count());
        assert!(tree.contains_key("a", SeqNo::MAX)?);
    }

    {
        let tree = Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert_eq!(1, tree.table_count());
        assert_eq!(large_value.as_bytes(), tree.get("a", SeqNo::MAX)?.unwrap());
    }

    Ok(())
}

#[test]
#[ignore = "v4 fix"]
fn tree_reload_blob_again_without_opts() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert_eq!(0, tree.table_count());
        assert_eq!(0, tree.blob_file_count());
    }

    {
        let tree = Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(None)
            .open();

        assert!(matches!(tree, Err(lsm_tree::Error::Unrecoverable)));
    }

    Ok(())
}

#[test]
fn tree_reload_empty() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

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
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

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

        version_set_provider.flush_active_memtable(&tree, 1, 0)?;
    }

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

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

        version_set_provider.flush_active_memtable(&tree, 2, 0)?;
    }

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

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
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), seqno.next());
        }

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

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

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    }

    {
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

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
