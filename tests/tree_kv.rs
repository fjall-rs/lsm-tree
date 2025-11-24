use lsm_tree::{get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_first_last_kv() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;

        assert!(tree.is_empty(SeqNo::MAX, None)?);
        assert_eq!(tree.first_key_value(SeqNo::MAX, None)?, None);
        assert_eq!(tree.last_key_value(SeqNo::MAX, None)?, None);

        tree.insert("b", "b", 0);
        assert_eq!(b"b", &*tree.first_key_value(SeqNo::MAX, None)?.unwrap().0);
        assert_eq!(b"b", &*tree.last_key_value(SeqNo::MAX, None)?.unwrap().0);

        tree.flush_active_memtable(0)?;

        assert_eq!(b"b", &*tree.first_key_value(SeqNo::MAX, None)?.unwrap().0);
        assert_eq!(b"b", &*tree.last_key_value(SeqNo::MAX, None)?.unwrap().0);
    }

    {
        let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;
        assert_eq!(1, tree.len(SeqNo::MAX, None)?);

        assert_eq!(b"b", &*tree.first_key_value(SeqNo::MAX, None)?.unwrap().0);
        assert_eq!(b"b", &*tree.last_key_value(SeqNo::MAX, None)?.unwrap().0);

        tree.insert("a", "a", 0);
        assert_eq!(2, tree.len(SeqNo::MAX, None)?);

        assert_eq!(b"a", &*tree.first_key_value(SeqNo::MAX, None)?.unwrap().0);
        assert_eq!(b"b", &*tree.last_key_value(SeqNo::MAX, None)?.unwrap().0);

        tree.insert("c", "c", 0);
        assert_eq!(3, tree.len(SeqNo::MAX, None)?);

        assert_eq!(b"a", &*tree.first_key_value(SeqNo::MAX, None)?.unwrap().0);
        assert_eq!(b"c", &*tree.last_key_value(SeqNo::MAX, None)?.unwrap().0);

        tree.flush_active_memtable(0)?;

        assert_eq!(b"a", &*tree.first_key_value(SeqNo::MAX, None)?.unwrap().0);
        assert_eq!(b"c", &*tree.last_key_value(SeqNo::MAX, None)?.unwrap().0);
    }

    {
        let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;
        assert_eq!(3, tree.len(SeqNo::MAX, None)?);

        assert_eq!(b"a", &*tree.first_key_value(SeqNo::MAX, None)?.unwrap().0);
        assert_eq!(b"c", &*tree.last_key_value(SeqNo::MAX, None)?.unwrap().0);
    }

    Ok(())
}
