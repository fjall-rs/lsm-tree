use lsm_tree::Config;
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
fn tree_disjoint_range() -> lsm_tree::Result<()> {
    let tempdir = tempfile::tempdir()?;
    let tree = crate::Config::new(&tempdir).open()?;

    // IMPORTANT: Purposefully mangle the order of IDs
    // to make sure stuff is still getting read in the correct order
    // even if written out of order
    let ids = [
        ["d", "e", "f"],
        ["g", "h", "i"],
        ["a", "b", "c"],
        ["j", "k", "l"],
    ];

    for batch in ids {
        for id in batch {
            tree.insert(id, vec![], 0);
        }
        tree.flush_active_memtable()?;
    }

    // NOTE: Forwards

    let iter = tree.range("e".."i");
    let mut iter = iter.into_iter();

    assert_eq!(Arc::from(*b"e"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"f"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"g"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"h"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Forwards inclusive

    let iter = tree.range("e"..="i");
    let mut iter = iter.into_iter();

    assert_eq!(Arc::from(*b"e"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"f"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"g"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"h"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"i"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Reverse

    let iter = tree.range("e".."i");
    let mut iter = iter.into_iter().rev();

    assert_eq!(Arc::from(*b"h"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"g"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"f"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"e"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Reverse inclusive

    let iter = tree.range("e"..="i");
    let mut iter = iter.into_iter().rev();

    assert_eq!(Arc::from(*b"i"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"h"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"g"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"f"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"e"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Ping Pong

    let iter = tree.range("e".."i");
    let mut iter = iter.into_iter();

    assert_eq!(Arc::from(*b"e"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"h"), iter.next_back().unwrap()?.0);
    assert_eq!(Arc::from(*b"f"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"g"), iter.next_back().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Ping Pong inclusive

    let iter = tree.range("e"..="i");
    let mut iter = iter.into_iter();

    assert_eq!(Arc::from(*b"e"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"i"), iter.next_back().unwrap()?.0);
    assert_eq!(Arc::from(*b"f"), iter.next().unwrap()?.0);
    assert_eq!(Arc::from(*b"h"), iter.next_back().unwrap()?.0);
    assert_eq!(Arc::from(*b"g"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    Ok(())
}
