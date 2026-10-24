use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, KvSeparationOptions, MemoryVersionSetProvider,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
#[ignore = "v4"]
fn tree_recovery_cleanup_orphans() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();

    {
        let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
        let tree = Config::new(&folder, version_set_provider.clone()).open()?;
        tree.insert("a", "a", seqno.next());
        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert!(folder.path().join("tables").join("0").try_exists()?);

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
            seqno.next(),
            0,
        )?;

        assert!(folder.path().join("tables").join("1").try_exists()?);
    }

    std::fs::File::create(folder.path().join("tables").join("0"))?;

    {
        let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
        let _tree = Config::new(&folder, version_set_provider).open()?;

        assert!(!folder.path().join("tables").join("0").try_exists()?);
        assert!(folder.path().join("tables").join("1").try_exists()?);
    }

    Ok(())
}

#[test]
#[ignore = "v4"]
fn tree_recovery_cleanup_orphans_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    {
        let tree = Config::new(&folder, version_set_provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default()
                    .age_cutoff(1.0)
                    .staleness_threshold(0.01),
            ))
            .open()?;

        tree.insert("a", "a".repeat(10_000), seqno.next());
        tree.insert("a", "a".repeat(10_000), seqno.next());
        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert!(folder.path().join("blobs").join("0").try_exists()?);

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
            seqno.next(),
            5,
        )?;

        assert!(folder.path().join("blobs").join("0").try_exists()?);

        version_set_provider.compact(
            &tree,
            Arc::new(lsm_tree::compaction::Major::new(u64::MAX)),
            seqno.next(),
            10,
        )?;

        assert!(folder.path().join("blobs").join("1").try_exists()?);
    }

    std::fs::File::create(folder.path().join("blobs").join("0"))?;

    {
        let _tree = Config::new(&folder, version_set_provider)
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(!folder.path().join("blobs").join("0").try_exists()?);
        assert!(folder.path().join("blobs").join("1").try_exists()?);
    }

    Ok(())
}
