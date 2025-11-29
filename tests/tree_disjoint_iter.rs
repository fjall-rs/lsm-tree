use lsm_tree::{get_tmp_folder, AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter, Slice};
use test_log::test;

macro_rules! iter_closed {
    ($iter:expr) => {
        assert!($iter.next().is_none(), "iterator should be closed (done)");
        assert!(
            $iter.next_back().is_none(),
            "iterator should be closed (done)"
        );
    };
}

#[test]
fn tree_disjoint_iter() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = crate::Config::new(&folder, SequenceNumberCounter::default(), SequenceNumberCounter::default()).open()?;

    // IMPORTANT: Purposefully mangle the order of IDs
    // to make sure stuff is still getting read in the correct order
    // even if written out of order
    let ids = [["f", "e", "d"], ["a", "b", "c"]];

    for batch in ids {
        for id in batch {
            tree.insert(id, vec![], 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // NOTE: Forwards

    let mut iter = tree.iter(SeqNo::MAX, None);

    assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key()?);
    iter_closed!(iter);

    // NOTE: Reverse

    let mut iter = tree.iter(SeqNo::MAX, None).rev();

    assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key()?);
    iter_closed!(iter);

    // NOTE: Ping Pong

    let mut iter = tree.iter(SeqNo::MAX, None);

    assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"f"), iter.next_back().unwrap().key()?);
    assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"e"), iter.next_back().unwrap().key()?);
    assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"d"), iter.next_back().unwrap().key()?);
    iter_closed!(iter);

    Ok(())
}
