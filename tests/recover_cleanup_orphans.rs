use lsm_tree::{AbstractTree, Config, KvSeparationOptions};
use test_log::test;

#[test]
fn tree_recovery_cleanup_orphans() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).open()?;
        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert!(folder.path().join("tables").join("0").try_exists()?);

        tree.major_compact(u64::MAX, 0)?;

        assert!(folder.path().join("tables").join("1").try_exists()?);
    }

    std::fs::File::create(folder.path().join("tables").join("0"))?;

    {
        let _tree = Config::new(&folder).open()?;

        assert!(!folder.path().join("tables").join("0").try_exists()?);
        assert!(folder.path().join("tables").join("1").try_exists()?);
    }

    Ok(())
}

#[test]
fn tree_recovery_cleanup_orphans_blob() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder)
            .with_kv_separation(Some(
                KvSeparationOptions::default()
                    .age_cutoff(1.0)
                    .staleness_threshold(0.01),
            ))
            .open()?;

        tree.insert("a", "a".repeat(10_000), 0);
        tree.insert("a", "a".repeat(10_000), 1);
        tree.flush_active_memtable(0)?;

        assert!(folder.path().join("blobs").join("0").try_exists()?);

        tree.major_compact(u64::MAX, 5)?;

        assert!(folder.path().join("blobs").join("0").try_exists()?);

        tree.major_compact(u64::MAX, 10)?;

        assert!(folder.path().join("blobs").join("1").try_exists()?);
    }

    std::fs::File::create(folder.path().join("blobs").join("0"))?;

    {
        let _tree = Config::new(&folder)
            .with_kv_separation(Default::default())
            .open()?;

        assert!(!folder.path().join("blobs").join("0").try_exists()?);
        assert!(folder.path().join("blobs").join("1").try_exists()?);
    }

    Ok(())
}
