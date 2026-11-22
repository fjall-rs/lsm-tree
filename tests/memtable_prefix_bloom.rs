// The memtable prefix bloom filter must skip memtables that lack a prefix
// and never hide a key that was written.

use lsm_tree::{
    prefix::FixedPrefixExtractor, AbstractTree, AnyTree, Config, Guard, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;

fn open(folder: &std::path::Path, bloom_bytes: usize) -> lsm_tree::Result<AnyTree> {
    open_with(folder, bloom_bytes, SequenceNumberCounter::default())
}

fn open_with(
    folder: &std::path::Path,
    bloom_bytes: usize,
    visible_seqno: SequenceNumberCounter,
) -> lsm_tree::Result<AnyTree> {
    Config::new(folder, SequenceNumberCounter::default(), visible_seqno)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
        .memtable_prefix_bloom_bytes(bloom_bytes)
        .open()
}

/// Reads of an absent prefix skip the active and the sealed memtable.
#[test]
#[cfg(feature = "metrics")]
fn absent_prefix_skips_active_and_sealed_memtables() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), 1_024)?;

    tree.insert("aaaa1", "v", 0);
    tree.rotate_memtable().expect("memtable should rotate");
    tree.insert("bbbb1", "v", 1);

    let metrics = tree.metrics().clone();
    let skipped = metrics.memtable_reads_skipped_by_prefix_bloom();

    assert!(tree.get("zzzz1", SeqNo::MAX)?.is_none());
    assert_eq!(
        skipped + 2,
        metrics.memtable_reads_skipped_by_prefix_bloom()
    );

    assert_eq!(0, tree.prefix("zzzz", SeqNo::MAX, None).count());
    assert_eq!(
        skipped + 4,
        metrics.memtable_reads_skipped_by_prefix_bloom()
    );

    // Each key is in one memtable, which its read searches
    assert!(tree.get("aaaa1", SeqNo::MAX)?.is_some());
    assert!(tree.get("bbbb1", SeqNo::MAX)?.is_some());
    assert_eq!(1, tree.prefix("aaaa", SeqNo::MAX, None).count());
    assert_eq!(1, tree.prefix("bbbb", SeqNo::MAX, None).count());

    Ok(())
}

/// A reopened tree's first memtable has the bloom too.
#[test]
#[cfg(feature = "metrics")]
fn reopened_tree_has_bloom() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    drop(open(folder.path(), 1_024)?);

    let tree = open(folder.path(), 1_024)?;
    tree.insert("aaaa1", "v", 0);

    assert!(tree.get("zzzz1", SeqNo::MAX)?.is_none());
    assert_eq!(1, tree.metrics().memtable_reads_skipped_by_prefix_bloom());

    Ok(())
}

/// Without the option, memtables have no bloom to probe.
#[test]
#[cfg(feature = "metrics")]
fn off_by_default() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), 0)?;

    tree.insert("aaaa1", "v", 0);
    assert!(tree.get("zzzz1", SeqNo::MAX)?.is_none());
    assert_eq!(0, tree.prefix("zzzz", SeqNo::MAX, None).count());
    assert_eq!(0, tree.metrics().memtable_prefix_bloom_queries());

    Ok(())
}

/// An undersized bloom only costs false positives: every key stays readable.
#[test]
fn undersized_bloom_keeps_every_key() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), 1)?;

    for i in 0..5_000u64 {
        tree.insert(format!("{i:04}_x"), "v", i);
    }

    for i in 0..5_000u64 {
        let key = format!("{i:04}_x");
        assert!(tree.get(&key, SeqNo::MAX)?.is_some(), "lost {key}");
        assert_eq!(
            1,
            tree.prefix(&key[..4], SeqNo::MAX, None).count(),
            "lost {key}"
        );
    }

    Ok(())
}

/// A key a reader can see by its seqno is visible through the bloom while
/// other writers keep adding prefixes to it.
#[test]
fn concurrent_reads_see_every_published_key() -> lsm_tree::Result<()> {
    const WRITERS: u64 = 4;
    const KEYS: u64 = 5_000;

    fn key(seqno: SeqNo) -> String {
        format!("{:04}_{seqno}", seqno % 1_000)
    }

    let folder = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let tree = open_with(folder.path(), 64 * 1_024, visible.clone())?;
    let seqno = SequenceNumberCounter::default();

    std::thread::scope(|scope| {
        for _ in 0..WRITERS {
            scope.spawn(|| {
                for _ in 0..KEYS {
                    let s = seqno.next();
                    tree.insert(key(s), "v", s);
                    visible.fetch_max(s + 1);
                }
            });
        }

        for _ in 0..2 {
            scope.spawn(|| {
                for _ in 0..WRITERS * KEYS {
                    // The writer that published `snapshot` inserted
                    // `snapshot - 1` before publishing it; other writers
                    // may still be inserting smaller seqnos
                    let snapshot = visible.get();

                    if let Some(s) = snapshot.checked_sub(1) {
                        assert!(
                            tree.get(key(s), snapshot).unwrap().is_some(),
                            "key with seqno {s} is hidden at snapshot {snapshot}",
                        );
                    }
                }
            });
        }
    });

    for s in 0..WRITERS * KEYS {
        assert!(tree.get(key(s), SeqNo::MAX)?.is_some(), "lost seqno {s}");
    }

    Ok(())
}

/// The bloom lives with its memtable: flushed keys are read from tables.
#[test]
fn flushed_keys_stay_readable() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), 1_024)?;

    tree.insert("aaaa1", "v", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("bbbb1", "v", 1);

    assert!(tree.get("aaaa1", SeqNo::MAX)?.is_some());
    assert_eq!(
        vec![b"aaaa1".to_vec(), b"bbbb1".to_vec()],
        tree.range::<&str, _>(.., SeqNo::MAX, None)
            .map(|g| g.key().map(|k| k.to_vec()))
            .collect::<Result<Vec<_>, _>>()?,
    );

    Ok(())
}
