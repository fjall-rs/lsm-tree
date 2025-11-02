use lsm_tree::{AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter, Slice};
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
fn tree_disjoint_prefix() -> lsm_tree::Result<()> {
    let tempdir = tempfile::tempdir()?;
    let tree = crate::Config::new(&tempdir, SequenceNumberCounter::default()).open()?;

    // IMPORTANT: Purposefully mangle the order of IDs
    // to make sure stuff is still getting read in the correct order
    // even if written out of order
    let ids = [
        ["cc", "ca", "cb"],
        ["aa", "ab", "ac"],
        ["dc", "da", "db"],
        ["ba", "bb", "bc"],
    ];

    for batch in ids {
        for id in batch {
            tree.insert(id, vec![], 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // NOTE: Forwards

    let mut iter = tree.prefix("d", SeqNo::MAX, None);

    assert_eq!(Slice::from(*b"da"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"db"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"dc"), iter.next().unwrap().key()?);
    iter_closed!(iter);

    // NOTE: Reverse

    let mut iter = tree.prefix("d", SeqNo::MAX, None).rev();

    assert_eq!(Slice::from(*b"dc"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"db"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"da"), iter.next().unwrap().key()?);
    iter_closed!(iter);

    // NOTE: Ping Pong

    let mut iter = tree.prefix("d", SeqNo::MAX, None);

    assert_eq!(Slice::from(*b"da"), iter.next().unwrap().key()?);
    assert_eq!(Slice::from(*b"dc"), iter.next_back().unwrap().key()?);
    assert_eq!(Slice::from(*b"db"), iter.next().unwrap().key()?);
    iter_closed!(iter);

    Ok(())
}
