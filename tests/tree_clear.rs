use lsm_tree::{get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_clear() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    assert_eq!(0, tree.len(visible_seqno.get(), None)?);

    {
        let seqno = seqno.next();
        tree.insert("a", "a", seqno);
        visible_seqno.fetch_max(seqno + 1);
    }

    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert_eq!(1, tree.len(visible_seqno.get(), None)?);

    tree.clear()?;
    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert_eq!(0, tree.len(visible_seqno.get(), None)?);

    {
        let seqno = seqno.next();
        tree.insert("a", "a", seqno);
        visible_seqno.fetch_max(seqno + 1);
    }

    tree.flush_active_memtable(0)?;
    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert_eq!(1, tree.len(visible_seqno.get(), None)?);

    tree.clear()?;
    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert_eq!(0, tree.len(visible_seqno.get(), None)?);

    Ok(())
}

#[test]
fn tree_clear_at_seqno() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();
    let external_seqno = SequenceNumberCounter::default();

    let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    assert!(tree.get_highest_persisted_seqno().is_none());
    let seqno = external_seqno.next();
    tree.clear_at_seqno(Some(seqno))?;
    assert_eq!(tree.get_highest_persisted_seqno(), Some(seqno));
    let insert_seqno = external_seqno.next();
    tree.insert(b"foo", b"bar", insert_seqno);
    tree.flush_active_memtable(insert_seqno)
        .expect("should flush");
    assert_eq!(tree.get_highest_persisted_seqno(), Some(insert_seqno));

    Ok(())
}
