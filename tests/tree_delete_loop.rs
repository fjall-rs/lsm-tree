use lsm_tree::{get_tmp_folder, AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_delete_by_prefix() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let seqno = SequenceNumberCounter::default();

    for x in 0..ITEM_COUNT as u64 {
        let value = "old".as_bytes();
        let batch_seqno = seqno.next();

        tree.insert(format!("a:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("b:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("c:{x}").as_bytes(), value, batch_seqno);
    }

    tree.flush_active_memtable(0)?;

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

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let value = "old".as_bytes();
    tree.insert("a".as_bytes(), value, 0);
    tree.insert("b".as_bytes(), value, 0);
    tree.insert("c".as_bytes(), value, 0);
    tree.insert("d".as_bytes(), value, 0);
    tree.insert("e".as_bytes(), value, 0);
    tree.insert("f".as_bytes(), value, 0);

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 6);

    for item in tree.range("c"..="e", SeqNo::MAX, None) {
        let key = item.key()?;
        tree.remove(key, 1);
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, 3);

    Ok(())
}
