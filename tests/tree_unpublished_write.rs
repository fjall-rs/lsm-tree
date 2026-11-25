use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

// NOTE: Flushes and compactions used to publish their own seqno, which made
// a write that took an earlier seqno visible before it was fully applied

#[test]
fn tree_flush_published_when_caught_up() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    visible_seqno.fetch_max(batch_seqno + 1);

    tree.flush_active_memtable(0)?;
    assert_eq!(seqno.get(), visible_seqno.get());

    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(seqno.get(), visible_seqno.get());
    assert_eq!(b"a", &*tree.get("a", visible_seqno.get())?.unwrap());

    Ok(())
}

#[test]
fn tree_flush_unpublished_write() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    visible_seqno.fetch_max(batch_seqno + 1);

    let pending_seqno = seqno.next();
    tree.insert("b", "b", pending_seqno);

    tree.flush_active_memtable(0)?;
    assert_eq!(pending_seqno, visible_seqno.get());
    assert_eq!(b"a", &*tree.get("a", visible_seqno.get())?.unwrap());
    assert!(tree.get("b", visible_seqno.get())?.is_none());

    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(pending_seqno, visible_seqno.get());
    assert_eq!(b"a", &*tree.get("a", visible_seqno.get())?.unwrap());
    assert!(tree.get("b", visible_seqno.get())?.is_none());

    visible_seqno.fetch_max(pending_seqno + 1);
    assert_eq!(b"b", &*tree.get("b", visible_seqno.get())?.unwrap());

    let next_seqno = seqno.next();
    tree.insert("c", "c", next_seqno);
    visible_seqno.fetch_max(next_seqno + 1);
    assert_eq!(3, tree.len(visible_seqno.get(), None)?);

    Ok(())
}

#[test]
fn tree_rotate_unpublished_write() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    visible_seqno.fetch_max(batch_seqno + 1);

    let first_seqno = seqno.next();
    let second_seqno = seqno.next();

    // NOTE: The flushed version is not published yet,
    // so writes landing after the rotation have to be visible from the published one
    tree.flush_active_memtable(0)?;
    tree.insert("b", "b", first_seqno);
    assert!(tree.rotate_memtable().is_some());
    tree.insert("c", "c", second_seqno);
    visible_seqno.fetch_max(second_seqno + 1);

    assert_eq!(b"a", &*tree.get("a", visible_seqno.get())?.unwrap());
    assert_eq!(b"b", &*tree.get("b", visible_seqno.get())?.unwrap());
    assert_eq!(b"c", &*tree.get("c", visible_seqno.get())?.unwrap());
    assert_eq!(3, tree.len(visible_seqno.get(), None)?);

    Ok(())
}
