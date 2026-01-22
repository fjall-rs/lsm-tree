use lsm_tree::{get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_clear() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

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
