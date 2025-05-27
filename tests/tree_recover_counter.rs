use lsm_tree::{AbstractTree, Config};
use test_log::test;

#[test]
fn tree_recover_segment_counter() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(
            0,
            tree.0
                .segment_id_counter
                .load(std::sync::atomic::Ordering::Relaxed)
        );

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(
            1,
            tree.0
                .segment_id_counter
                .load(std::sync::atomic::Ordering::Relaxed)
        );

        {
            assert_eq!(
                0,
                tree.manifest
                    .read()
                    .expect("lock is poisoned")
                    .current_version()
                    .level(0)
                    .expect("should exist")
                    .first()
                    .expect("should have exactly 1 run")
                    .first()
                    .expect("should have one segment")
                    .id()
            );
        }

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(
            2,
            tree.0
                .segment_id_counter
                .load(std::sync::atomic::Ordering::Relaxed)
        );

        assert_eq!(
            1,
            tree.manifest
                .read()
                .expect("lock is poisoned")
                .current_version()
                .level(0)
                .expect("should exist")
                .first()
                .expect("should have at least 1 run")
                .first()
                .expect("should have one segment")
                .id()
        );

        assert_eq!(
            0,
            tree.manifest
                .read()
                .expect("lock is poisoned")
                .current_version()
                .level(0)
                .expect("should exist")
                .get(1)
                .expect("should have at least 1 run")
                .first()
                .expect("should have one segment")
                .id()
        );
    }

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(
            2,
            tree.0
                .segment_id_counter
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    Ok(())
}
