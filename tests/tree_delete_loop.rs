use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_delete_by_prefix() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let seqno = SequenceNumberCounter::default();

    for x in 0..ITEM_COUNT as u64 {
        let value = "old".as_bytes();
        let batch_seqno = seqno.next();

        tree.insert(format!("a:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("b:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("c:{x}").as_bytes(), value, batch_seqno);
    }

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 3);
    assert_eq!(
        tree.prefix("a:".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.prefix("b:".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.prefix("c:".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );

    for item in tree.prefix("b:".as_bytes(), SeqNo::MAX, None) {
        let key = item.key()?;
        tree.remove(key, seqno.next());
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("a:".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );
    assert_eq!(tree.prefix("b:".as_bytes(), SeqNo::MAX, None).count(), 0);
    assert_eq!(
        tree.prefix("c:".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );

    Ok(())
}

#[test]
fn tree_delete_by_range() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let value = "old".as_bytes();
    let batch_seqno = seqno.next();
    tree.insert("a".as_bytes(), value, batch_seqno);
    tree.insert("b".as_bytes(), value, batch_seqno);
    tree.insert("c".as_bytes(), value, batch_seqno);
    tree.insert("d".as_bytes(), value, batch_seqno);
    tree.insert("e".as_bytes(), value, batch_seqno);
    tree.insert("f".as_bytes(), value, batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 6);

    let batch_seqno = seqno.next();
    for item in tree.range("c"..="e", SeqNo::MAX, None) {
        let key = item.key()?;
        tree.remove(key, batch_seqno);
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, 3);

    Ok(())
}
