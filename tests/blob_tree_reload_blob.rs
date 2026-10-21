use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter, TreeType,
};
use std::sync::Arc;
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
fn blob_tree_reload_empty() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
        assert_eq!(tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(), 0);
        assert_eq!(
            tree.iter(SeqNo::MAX, None)
                .map(|x| x.key())
                .rev()
                .flatten()
                .count(),
            0
        );
        assert_eq!(tree.tree_type(), TreeType::Blob);
    }

    {
        let tree = Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
        assert_eq!(tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(), 0);
        assert_eq!(
            tree.iter(SeqNo::MAX, None)
                .map(|x| x.key())
                .rev()
                .flatten()
                .count(),
            0
        );
        assert_eq!(tree.tree_type(), TreeType::Blob);

        version_set_provider.flush_active_memtable(&tree, 0, 0)?;
    }

    {
        let tree = Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
        assert_eq!(tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(), 0);
        assert_eq!(
            tree.iter(SeqNo::MAX, None)
                .map(|x| x.key())
                .rev()
                .flatten()
                .count(),
            0
        );
        assert_eq!(tree.tree_type(), TreeType::Blob);
    }

    Ok(())
}

#[test]
fn blob_tree_reload() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

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
            ITEM_COUNT * 2,
        );
        assert_eq!(
            tree.iter(SeqNo::MAX, None)
                .rev()
                .flat_map(|x| x.key())
                .count(),
            ITEM_COUNT * 2,
        );

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    }

    {
        let tree = Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
        assert_eq!(
            tree.iter(SeqNo::MAX, None).flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2,
        );
        assert_eq!(
            tree.iter(SeqNo::MAX, None)
                .rev()
                .flat_map(|x| x.key())
                .count(),
            ITEM_COUNT * 2,
        );
    }

    Ok(())
}
