use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_clear_snapshot() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    assert_eq!(0, tree.len(visible_seqno.get(), None)?);

    for c in ["a", "b", "c"] {
        let seqno = seqno.next();
        tree.insert(c, c, seqno);
        visible_seqno.fetch_max(seqno + 1);
    }
    assert_eq!(3, tree.len(visible_seqno.get(), None)?);

    let snapshot = visible_seqno.get();

    tree.clear()?;
    assert_eq!(0, tree.len(visible_seqno.get(), None)?);

    assert_eq!(3, tree.len(snapshot, None)?);

    Ok(())
}
