use lsm_tree::{AbstractTree, Config, Guard as _, SharedComparator, UserComparator};
use std::cmp::Ordering;
use std::sync::Arc;

/// Comparator that reverses the default lexicographic byte ordering.
struct ReverseComparator;

impl UserComparator for ReverseComparator {
    fn name(&self) -> &'static str {
        "reverse-lexicographic"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a) // reversed
    }
}

/// Comparator that orders u64 keys stored as big-endian bytes.
struct U64BigEndianComparator;

impl UserComparator for U64BigEndianComparator {
    fn name(&self) -> &'static str {
        "u64-big-endian"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if a.len() == 8 && b.len() == 8 {
            let a_u64 = u64::from_be_bytes(a.try_into().unwrap());
            let b_u64 = u64::from_be_bytes(b.try_into().unwrap());
            a_u64.cmp(&b_u64)
        } else {
            // Non-8-byte keys: fall back to lexicographic ordering
            // to preserve the bytewise-equality invariant.
            a.cmp(b)
        }
    }
}

#[test]
fn reverse_comparator_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);

    // Point reads should work regardless of comparator
    assert_eq!(tree.get("a", 3)?, Some("val_a".as_bytes().into()));
    assert_eq!(tree.get("b", 3)?, Some("val_b".as_bytes().into()));
    assert_eq!(tree.get("c", 3)?, Some("val_c".as_bytes().into()));

    Ok(())
}

#[test]
fn reverse_comparator_iteration_order() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);

    // With reverse comparator, iteration order should be c, b, a
    let items: Vec<_> = tree
        .iter(3, None)
        .map(|g| {
            let (k, v) = g.into_inner().unwrap();
            (
                String::from_utf8(k.to_vec()).unwrap(),
                String::from_utf8(v.to_vec()).unwrap(),
            )
        })
        .collect();

    assert_eq!(
        items,
        vec![
            ("c".into(), "val_c".into()),
            ("b".into(), "val_b".into()),
            ("a".into(), "val_a".into()),
        ]
    );

    Ok(())
}

#[test]
fn reverse_comparator_after_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);

    // Flush to disk
    tree.flush_active_memtable(3)?;

    // Point reads after flush
    assert_eq!(tree.get("a", 4)?, Some("val_a".as_bytes().into()));
    assert_eq!(tree.get("b", 4)?, Some("val_b".as_bytes().into()));
    assert_eq!(tree.get("c", 4)?, Some("val_c".as_bytes().into()));

    // Iteration order should still be reversed after flush
    let items: Vec<_> = tree
        .iter(4, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["c", "b", "a"]);

    Ok(())
}

#[test]
fn u64_comparator_point_read_and_order() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    // Insert u64 keys as big-endian bytes
    let keys = [1u64, 100, 50, 1000, 500];
    for (i, &key) in keys.iter().enumerate() {
        tree.insert(key.to_be_bytes(), format!("val_{key}"), i as u64);
    }

    // Point reads
    assert_eq!(
        tree.get(1u64.to_be_bytes().as_ref(), 5)?,
        Some("val_1".as_bytes().into())
    );
    assert_eq!(
        tree.get(1000u64.to_be_bytes().as_ref(), 5)?,
        Some("val_1000".as_bytes().into())
    );

    // Iteration should be in numeric order: 1, 50, 100, 500, 1000
    let items: Vec<u64> = tree
        .iter(5, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![1, 50, 100, 500, 1000]);

    Ok(())
}

#[test]
fn u64_comparator_after_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    let keys = [1u64, 100, 50, 1000, 500];
    for (i, &key) in keys.iter().enumerate() {
        tree.insert(key.to_be_bytes(), format!("val_{key}"), i as u64);
    }

    tree.flush_active_memtable(5)?;

    // Point reads after flush
    assert_eq!(
        tree.get(50u64.to_be_bytes().as_ref(), 6)?,
        Some("val_50".as_bytes().into())
    );

    // Iteration should still be in numeric order
    let items: Vec<u64> = tree
        .iter(6, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![1, 50, 100, 500, 1000]);

    Ok(())
}

#[test]
fn default_comparator_unchanged_behavior() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // No custom comparator — default lexicographic should work as before
    let tree = Config::new(folder, Default::default(), Default::default()).open()?;

    tree.insert("banana", "b", 0);
    tree.insert("apple", "a", 1);
    tree.insert("cherry", "c", 2);

    let items: Vec<_> = tree
        .iter(3, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["apple", "banana", "cherry"]);

    Ok(())
}

#[test]
fn reverse_comparator_bounded_range_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "1", 0);
    tree.insert("b", "2", 1);
    tree.insert("c", "3", 2);
    tree.insert("d", "4", 3);
    tree.insert("e", "5", 4);

    // Reverse order: e, d, c, b, a
    // Range "d"..="b" in reverse comparator means: items where cmp says key >= "d" && key <= "b"
    // In reverse: "d" < "c" < "b" (reversed), so range "d"..="b" should yield d, c, b
    let items: Vec<_> = tree
        .range("d"..="b", 5, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["d", "c", "b"]);

    Ok(())
}

#[test]
fn u64_comparator_bounded_range_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    for &key in &[10u64, 50, 100, 500, 1000] {
        tree.insert(key.to_be_bytes(), format!("v{key}"), key);
    }

    // Range scan: 50..=500 should yield 50, 100, 500
    let lo = 50u64.to_be_bytes();
    let hi = 500u64.to_be_bytes();
    let items: Vec<u64> = tree
        .range(lo..=hi, 1001, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![50, 100, 500]);

    Ok(())
}

#[test]
fn reopen_with_same_comparator_succeeds() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with reverse comparator
    {
        let cmp: SharedComparator = Arc::new(ReverseComparator);
        let tree = Config::new(&folder, Default::default(), Default::default())
            .comparator(cmp)
            .open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(1)?;
    }

    // Reopen with the same comparator — must succeed
    let cmp: SharedComparator = Arc::new(ReverseComparator);
    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    assert_eq!(tree.get("a", 2)?, Some("1".as_bytes().into()));

    Ok(())
}

#[test]
fn reopen_with_different_comparator_fails() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with reverse comparator
    {
        let cmp: SharedComparator = Arc::new(ReverseComparator);
        let tree = Config::new(&folder, Default::default(), Default::default())
            .comparator(cmp)
            .open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(1)?;
    }

    // Reopen with u64 comparator — must fail
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);
    let result = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open();

    match result {
        Err(lsm_tree::Error::ComparatorMismatch { stored, supplied }) => {
            assert_eq!(stored, "reverse-lexicographic");
            assert_eq!(supplied, "u64-big-endian");
        }
        Ok(_) => panic!("expected ComparatorMismatch, got Ok"),
        Err(e) => panic!("expected ComparatorMismatch, got {e:?}"),
    }

    Ok(())
}

#[test]
fn reopen_custom_with_default_comparator_fails() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with reverse comparator
    {
        let cmp: SharedComparator = Arc::new(ReverseComparator);
        let tree = Config::new(&folder, Default::default(), Default::default())
            .comparator(cmp)
            .open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(1)?;
    }

    // Reopen without specifying a comparator (uses default) — must fail
    let result = Config::new(&folder, Default::default(), Default::default()).open();

    match result {
        Err(lsm_tree::Error::ComparatorMismatch { stored, supplied }) => {
            assert_eq!(stored, "reverse-lexicographic");
            assert_eq!(supplied, "default");
        }
        Ok(_) => panic!("expected ComparatorMismatch, got Ok"),
        Err(e) => panic!("expected ComparatorMismatch, got {e:?}"),
    }

    Ok(())
}

#[test]
fn reopen_default_tree_with_default_comparator_succeeds() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with default comparator
    {
        let tree = Config::new(&folder, Default::default(), Default::default()).open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(1)?;
    }

    // Reopen with default comparator — must succeed
    let tree = Config::new(&folder, Default::default(), Default::default()).open()?;
    assert_eq!(tree.get("a", 2)?, Some("1".as_bytes().into()));

    Ok(())
}

/// Comparator with a name exceeding the 256-byte limit.
struct OversizedNameComparator;

impl UserComparator for OversizedNameComparator {
    fn name(&self) -> &'static str {
        // 300 chars — exceeds MAX_COMPARATOR_NAME_BYTES (256)
        "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

#[test]
fn oversized_comparator_name_rejected_on_create() {
    let folder = tempfile::tempdir().unwrap();
    let cmp: SharedComparator = Arc::new(OversizedNameComparator);

    let result = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open();

    match result {
        Err(lsm_tree::Error::Io(e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
        }
        Ok(_) => panic!("expected InvalidInput error for oversized comparator name"),
        Err(e) => panic!("expected InvalidInput Io error, got {e:?}"),
    }
}
