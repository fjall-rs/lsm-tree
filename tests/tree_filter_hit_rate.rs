#[test_log::test]
#[cfg(feature = "metrics")]
fn tree_filter_hit_rate() -> lsm_tree::Result<()> {
    use lsm_tree::{
        get_tmp_folder, AbstractTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter,
    };

    let a = {
        let folder = get_tmp_folder();
        let path = folder.path();

        let seqno = SequenceNumberCounter::default();

        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(path, seqno.clone(), SequenceNumberCounter::default()).open()?;

        for k in 0u64..10_000 {
            tree.insert(k.to_be_bytes(), "abc", seqno.next());
        }
        tree.flush_active_memtable(0)?;
        tree.major_compact(u64::MAX, 0)?;

        for k in 0u64..10_000 {
            tree.get(k.to_be_bytes(), SeqNo::MAX).unwrap().unwrap();
        }

        tree.metrics().filter_efficiency()
    };

    {
        let folder = get_tmp_folder();
        let path = folder.path();

        let seqno = SequenceNumberCounter::default();

        let tree = Config::<lsm_tree::fs::StdFileSystem>::new(path, seqno.clone(), SequenceNumberCounter::default())
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
            .open()?;

        for k in 0u64..10_000 {
            tree.insert(k.to_be_bytes(), "abc", seqno.next());
        }
        tree.flush_active_memtable(0)?;
        tree.major_compact(u64::MAX, 0)?;

        for k in 0u64..10_000 {
            tree.get(k.to_be_bytes(), SeqNo::MAX).unwrap().unwrap();
        }

        assert_eq!(a, tree.metrics().filter_efficiency());
    }

    Ok(())
}
