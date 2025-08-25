use lsm_tree::{AbstractTree, Config, KeyRange, UserKey};
use test_log::test;

#[test]
fn tree_drop_range() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).open()?;

        for key in 'a'..='e' {
            tree.insert([key as u8], "", 0);
            tree.flush_active_memtable(0)?;
        }

        assert_eq!(1, tree.l0_run_count());
        assert_eq!(5, tree.segment_count());

        tree.drop_range(KeyRange::new((UserKey::from("a"), UserKey::from("c"))))?;

        assert!(!tree.contains_key("a", None)?);
        assert!(!tree.contains_key("b", None)?);
        assert!(!tree.contains_key("c", None)?);
        assert!(tree.contains_key("d", None)?);
        assert!(tree.contains_key("e", None)?);

        assert_eq!(1, tree.l0_run_count());
        assert_eq!(2, tree.segment_count());
    }

    Ok(())
}
