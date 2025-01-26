use lsm_tree::{AbstractTree, Config};
use test_log::test;

#[test]
fn tree_first_last_kv() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).open()?;

        assert!(tree.is_empty(None, None)?);
        assert_eq!(tree.first_key_value(None, None)?, None);
        assert_eq!(tree.last_key_value(None, None)?, None);

        tree.insert("b", "b", 0);
        assert_eq!(b"b", &*tree.first_key_value(None, None)?.unwrap().0);
        assert_eq!(b"b", &*tree.last_key_value(None, None)?.unwrap().0);

        tree.flush_active_memtable(0)?;

        assert_eq!(b"b", &*tree.first_key_value(None, None)?.unwrap().0);
        assert_eq!(b"b", &*tree.last_key_value(None, None)?.unwrap().0);
    }

    {
        let tree = Config::new(&folder).open()?;
        assert_eq!(1, tree.len(None, None)?);

        assert_eq!(b"b", &*tree.first_key_value(None, None)?.unwrap().0);
        assert_eq!(b"b", &*tree.last_key_value(None, None)?.unwrap().0);

        tree.insert("a", "a", 0);
        assert_eq!(2, tree.len(None, None)?);

        assert_eq!(b"a", &*tree.first_key_value(None, None)?.unwrap().0);
        assert_eq!(b"b", &*tree.last_key_value(None, None)?.unwrap().0);

        tree.insert("c", "c", 0);
        assert_eq!(3, tree.len(None, None)?);

        assert_eq!(b"a", &*tree.first_key_value(None, None)?.unwrap().0);
        assert_eq!(b"c", &*tree.last_key_value(None, None)?.unwrap().0);

        tree.flush_active_memtable(0)?;

        assert_eq!(b"a", &*tree.first_key_value(None, None)?.unwrap().0);
        assert_eq!(b"c", &*tree.last_key_value(None, None)?.unwrap().0);
    }

    {
        let tree = Config::new(&folder).open()?;
        assert_eq!(3, tree.len(None, None)?);

        assert_eq!(b"a", &*tree.first_key_value(None, None)?.unwrap().0);
        assert_eq!(b"c", &*tree.last_key_value(None, None)?.unwrap().0);
    }

    Ok(())
}
