use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter, TreeType,
};
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
fn blob_tree_reload_empty() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
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
        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
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

        tree.flush_active_memtable(0)?;
    }

    {
        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
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

    {
        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(&folder, seqno.clone(), SequenceNumberCounter::default())
            .with_kv_separation(Some(Default::default()))
            .open()?;

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
        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(&folder, seqno.clone(), SequenceNumberCounter::default())
            .with_kv_separation(Some(Default::default()))
            .open()?;

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

    Ok(())
}
