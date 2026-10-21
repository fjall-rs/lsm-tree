use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter, Slice,
};
use std::sync::Arc;
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
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = crate::Config::new(&folder, version_set_provider.clone()).open()?;

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
        let batch_seqno = seqno.next();
        for id in batch {
            tree.insert(id, vec![], batch_seqno);
        }

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
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
