mod common;

use common::TestPrefixExtractor;
use std::sync::Arc;

use lsm_tree::{AbstractTree, Config, Slice};
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
fn tree_disjoint_prefix_with_extractor() -> lsm_tree::Result<()> {
    let tempdir = tempfile::tempdir()?;
    let tree = crate::Config::new(&tempdir)
        .prefix_extractor(Arc::new(TestPrefixExtractor::new(2)))
        .open()?;

    // IMPORTANT: Purposefully mangle the order of IDs
    // to make sure stuff is still getting read in the correct order
    // even if written out of order
    let ids = [
        ["cc", "ca", "cb"],
        ["aa", "ab", "ac"],
        ["dc", "da", "db"],
        ["daa", "baa", "bda"],
        ["ba", "bb", "bc"],
    ];

    for batch in ids {
        for id in batch {
            tree.insert(id, vec![], 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // NOTE: Forwards

    let mut iter = tree.prefix("d");

    assert_eq!(Slice::from(*b"da"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"daa"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"db"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"dc"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Reverse

    let mut iter = tree.prefix("d").rev();

    assert_eq!(Slice::from(*b"dc"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"db"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"daa"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"da"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Ping Pong

    let mut iter = tree.prefix("d");

    assert_eq!(Slice::from(*b"da"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"dc"), iter.next_back().unwrap()?.0);
    assert_eq!(Slice::from(*b"daa"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"db"), iter.next_back().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Forwards with prefix in domain

    let mut iter = tree.prefix("da");

    assert_eq!(Slice::from(*b"da"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"daa"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Reverse with prefix in domain

    let mut iter = tree.prefix("da").rev();

    assert_eq!(Slice::from(*b"daa"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"da"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Ping Pong with prefix in domain

    let mut iter = tree.prefix("da");

    assert_eq!(Slice::from(*b"da"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"daa"), iter.next_back().unwrap()?.0);
    iter_closed!(iter);

    Ok(())
}

#[test]
fn tree_disjoint_prefix() -> lsm_tree::Result<()> {
    let tempdir = tempfile::tempdir()?;
    let tree = crate::Config::new(&tempdir).open()?;

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

    let mut iter = tree.prefix("d");

    assert_eq!(Slice::from(*b"da"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"db"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"dc"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Reverse

    let mut iter = tree.prefix("d").rev();

    assert_eq!(Slice::from(*b"dc"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"db"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"da"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    // NOTE: Ping Pong

    let mut iter = tree.prefix("d");

    assert_eq!(Slice::from(*b"da"), iter.next().unwrap()?.0);
    assert_eq!(Slice::from(*b"dc"), iter.next_back().unwrap()?.0);
    assert_eq!(Slice::from(*b"db"), iter.next().unwrap()?.0);
    iter_closed!(iter);

    Ok(())
}
