use super::*;
use crate::{version_set::MemoryVersionSetProvider, AbstractTree, Config, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn leveled_empty_levels() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(dir.path(), version_set_provider.clone()).open()?;

    let strategy = Arc::new(Strategy::default());
    version_set_provider.compact(&tree, strategy, 3, 0)?;

    assert_eq!(0, tree.table_count());
    Ok(())
}

#[test]
fn leveled_l0_below_limit() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(dir.path(), version_set_provider.clone()).open()?;

    for i in 0..3u8 {
        tree.insert([b'k', i].as_slice(), "v", 0);

        version_set_provider.flush_active_memtable(&tree, 1, 0)?;
    }

    let before = tree.table_count();
    assert_eq!(3, before);

    let strategy = Arc::new(Strategy::default());
    version_set_provider.compact(&tree, strategy, 3, 0)?;

    assert_eq!(before, tree.table_count());

    Ok(())
}

#[test]
fn leveled_l0_reached_limit() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(dir.path(), version_set_provider.clone()).open()?;

    for i in 0..4u8 {
        // NOTE: Tables need to overlap
        tree.insert("a", "v", 0);
        tree.insert([b'k', i].as_slice(), "v", 0);
        tree.insert("z", "v", 0);

        version_set_provider.flush_active_memtable(&tree, 1, 0)?;
    }

    assert_eq!(4, tree.table_count());

    let strategy = Arc::new(Strategy::default());
    version_set_provider.compact(&tree, strategy, 3, 0)?;

    assert_eq!(1, tree.table_count());

    Ok(())
}

#[test]
fn leveled_l0_reached_limit_disjoint() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(dir.path(), version_set_provider.clone()).open()?;

    for i in 0..4u8 {
        tree.insert([b'k', i].as_slice(), "v", 0);

        version_set_provider.flush_active_memtable(&tree, 1, 0)?;
    }

    assert_eq!(4, tree.table_count());

    let strategy = Arc::new(Strategy::default());
    version_set_provider.compact(&tree, strategy, 3, 0)?;

    assert_eq!(4, tree.table_count());

    Ok(())
}

#[test]
fn leveled_l0_reached_limit_disjoint_l1() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(dir.path(), version_set_provider.clone()).open()?;

    for _ in 0..4 {
        let batch_seqno = seqno.next();

        // NOTE: Tables need to overlap
        tree.insert("a", "v", batch_seqno);
        tree.insert("b", "v", batch_seqno);

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    }

    let fifo = Arc::new(Strategy::default());
    version_set_provider.compact(&tree, fifo, seqno.next(), 0)?;

    assert_eq!(1, tree.table_count());

    for i in 0..4u8 {
        tree.insert([b'k', i].as_slice(), "v", seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    }

    assert_eq!(5, tree.table_count());

    let strategy = Arc::new(Strategy::default());
    version_set_provider.compact(&tree, strategy, seqno.next(), 0)?;

    assert_eq!(5, tree.table_count());

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn leveled_sequential_inserts() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(dir.path(), version_set_provider.clone()).open()?;

    let strategy = Arc::new(Strategy {
        target_size: 1,
        ..Default::default()
    });

    let mut table_count = 0;

    for k in 0u64..100 {
        table_count += 1;

        tree.insert(k.to_be_bytes(), "", 0);

        version_set_provider.flush_active_memtable(&tree, 1, 0)?;

        assert_eq!(table_count, tree.table_count());

        version_set_provider.compact(&tree, strategy.clone(), 3, 0)?;

        assert_eq!(table_count, tree.table_count());

        for idx in 1..=5 {
            assert_eq!(
                0,
                tree.current_version().level(idx).unwrap().len(),
                "no tables should be in intermediary level (L{idx})",
            );
        }
    }

    Ok(())
}
