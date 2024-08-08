use lsm_tree::{AbstractTree, Config};

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
        tree.flush_active_memtable()?;

        assert_eq!(
            1,
            tree.0
                .segment_id_counter
                .load(std::sync::atomic::Ordering::Relaxed)
        );

        {
            let first_level = &tree.levels.read().expect("lock is poisoned").levels[0];
            assert_eq!(0, first_level.segments[0].metadata.id);
        }

        tree.insert("b", "b", 0);
        tree.flush_active_memtable()?;

        assert_eq!(
            2,
            tree.0
                .segment_id_counter
                .load(std::sync::atomic::Ordering::Relaxed)
        );

        {
            let first_level = &tree.levels.read().expect("lock is poisoned").levels[0];
            assert_eq!(1, first_level.segments[1].metadata.id);
        }
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
