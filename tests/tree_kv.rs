use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
#[ignore = "restore recovery"]
fn tree_first_last_kv() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    {
        let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

        assert!(tree.is_empty(SeqNo::MAX, None)?);
        assert_eq!(
            tree.first_key_value(SeqNo::MAX, None)
                .map(|x| x.into_inner().unwrap()),
            None
        );
        assert_eq!(
            tree.last_key_value(SeqNo::MAX, None)
                .map(|x| x.into_inner().unwrap()),
            None
        );

        tree.insert("b", "b", seqno.next());
        assert_eq!(
            b"b",
            &*tree
                .first_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );
        assert_eq!(
            b"b",
            &*tree
                .last_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(
            b"b",
            &*tree
                .first_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );
        assert_eq!(
            b"b",
            &*tree
                .last_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );
    }

    {
        let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

        assert_eq!(1, tree.len(SeqNo::MAX, None)?);
        assert_eq!(
            b"b",
            &*tree
                .first_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );
        assert_eq!(
            b"b",
            &*tree
                .last_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );

        tree.insert("a", "a", seqno.next());

        assert_eq!(2, tree.len(SeqNo::MAX, None)?);
        assert_eq!(
            b"a",
            &*tree
                .first_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );
        assert_eq!(
            b"b",
            &*tree
                .last_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );

        tree.insert("c", "c", seqno.next());

        assert_eq!(3, tree.len(SeqNo::MAX, None)?);
        assert_eq!(
            b"a",
            &*tree
                .first_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );
        assert_eq!(
            b"c",
            &*tree
                .last_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert_eq!(
            b"a",
            &*tree
                .first_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );
        assert_eq!(
            b"c",
            &*tree
                .last_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );
    }

    {
        let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

        let tree = Config::new(&folder, version_set_provider.clone()).open()?;

        assert_eq!(3, tree.len(SeqNo::MAX, None)?);
        assert_eq!(
            b"a",
            &*tree
                .first_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );
        assert_eq!(
            b"c",
            &*tree
                .last_key_value(SeqNo::MAX, None)
                .map(|x| x.key().unwrap())
                .unwrap()
        );
    }

    Ok(())
}
