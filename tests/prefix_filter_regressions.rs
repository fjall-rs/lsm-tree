// Regression tests for prefix filter bugs; each test names the bug it pins.

use lsm_tree::{
    config::{
        BloomConstructionPolicy, CompressionPolicy, FilterPolicy, FilterPolicyEntry, PinningPolicy,
    },
    prefix::{FixedPrefixExtractor, PrefixExtractor},
    AbstractTree, AnyTree, Config, Guard, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;

fn config(folder: &std::path::Path, extractor: Arc<dyn PrefixExtractor>) -> Config {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(extractor)
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(20.0),
    )))
    // Table counts after compaction must not depend on the lz4 feature
    .data_block_compression_policy(CompressionPolicy::disabled())
}

fn keys<I: DoubleEndedIterator<Item = lsm_tree::IterGuardImpl>>(iter: I) -> Vec<Vec<u8>> {
    iter.map(|g| g.key().unwrap().to_vec()).collect()
}

/// Two bytes if the third is below `m`, else three: "ab1" has prefix "ab",
/// "abz1" has "abz". The keys with each prefix are contiguous, so this
/// extractor follows the contract, yet "ab" is not a prefix of every key
/// starting with "ab".
struct SplitWidth;

impl PrefixExtractor for SplitWidth {
    fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
        match key.get(2) {
            None if key.len() == 2 => emit(2),
            None => {}
            Some(&b) if b < b'm' => emit(2),
            Some(_) => emit(3),
        }
    }

    fn name(&self) -> &'static str {
        "test::SplitWidth"
    }
}

/// Bug: `prefix(p)` probed the filter with p's own prefix, assuming every key
/// starting with p has it. "abz1" starts with "ab" but has prefix "abz", so
/// the table was skipped and the key lost.
#[test]
fn prefix_scan_keeps_keys_with_a_longer_prefix() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = config(folder.path(), Arc::new(SplitWidth)).open()?;

    tree.insert("abz1", "v", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        keys(tree.prefix("ab", SeqNo::MAX, None)),
        [b"abz1".to_vec()]
    );
    assert_eq!(
        keys(tree.range("ab".."ac", SeqNo::MAX, None)),
        [b"abz1".to_vec()]
    );

    Ok(())
}

/// Bug: a range like `mmm..mmn` holds only keys with prefix "mmm", but its
/// bounds have different prefixes, so the filter was never probed.
#[test]
#[cfg(feature = "metrics")]
fn prefix_successor_range_uses_the_filter() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = config(folder.path(), Arc::new(FixedPrefixExtractor::new(3))).open()?;

    for key in ["aaa1", "aaa2", "zzz1"] {
        tree.insert(key, "v", 0);
    }
    tree.flush_active_memtable(0)?;

    let skipped = tree.metrics().io_skipped_by_prefix_filter();
    assert!(keys(tree.range("mmm".."mmn", SeqNo::MAX, None)).is_empty());
    assert_eq!(skipped + 1, tree.metrics().io_skipped_by_prefix_filter());

    // The same holds for a longer scan of one prefix
    let skipped = tree.metrics().io_skipped_by_prefix_filter();
    assert!(keys(tree.range("mmm5".."mmn", SeqNo::MAX, None)).is_empty());
    assert_eq!(skipped + 1, tree.metrics().io_skipped_by_prefix_filter());

    Ok(())
}

/// The domain-of-a-flipped-email example from the old docs, done right: the
/// prefix includes the `@`, so the keys with prefix "a@" are exactly those
/// starting with it.
struct Domain;

impl PrefixExtractor for Domain {
    fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
        if let Some(at) = key.iter().position(|&b| b == b'@') {
            emit(at + 1);
        }
    }

    fn same_result_when_appended(&self, prefix: &[u8]) -> bool {
        prefix.ends_with(b"@")
    }

    fn name(&self) -> &'static str {
        "test::Domain"
    }
}

/// Bug: the documented example extracted "a" from "a@x" and the whole key
/// "a" from "a", so `a..=a@z` probed "a", which "a!" between them lacks,
/// and lost "a!".
#[test]
fn domain_prefix_with_delimiter_keeps_keys_between() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = config(folder.path(), Arc::new(Domain)).open()?;

    tree.insert("a!", "v", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        keys(tree.range("a"..="a@z", SeqNo::MAX, None)),
        [b"a!".to_vec()]
    );
    assert!(keys(tree.prefix("a@", SeqNo::MAX, None)).is_empty());

    Ok(())
}

/// The old documented domain example, which breaks contiguity.
struct DomainAndWholeKey;

impl PrefixExtractor for DomainAndWholeKey {
    fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
        if let Some(at) = key.iter().position(|&b| b == b'@') {
            emit(at);
        }
        emit(key.len());
    }

    fn name(&self) -> &'static str {
        "test::DomainAndWholeKey"
    }
}

/// Debug builds catch an extractor that breaks contiguity when its table is
/// written, instead of losing keys on later reads.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "broke contiguity")]
fn non_contiguous_extractor_is_caught_on_flush() {
    let folder = tempfile::tempdir().unwrap();
    let tree = config(folder.path(), Arc::new(DomainAndWholeKey))
        .open()
        .unwrap();

    for key in ["a", "a!", "a@x"] {
        tree.insert(key, "v", 0);
    }
    tree.flush_active_memtable(0).unwrap();
}

/// 4 tables in one run, each holding a single prefix of 50 keys.
fn four_table_run(folder: &std::path::Path) -> lsm_tree::Result<AnyTree> {
    let tree = config(folder, Arc::new(FixedPrefixExtractor::new(4))).open()?;

    for prefix in ["aaaa", "cccc", "eeee", "gggg"] {
        for i in 0..50 {
            tree.insert(format!("{prefix}{i:04}"), "v", 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // The disjoint tables move down as one run
    tree.compact(Arc::new(lsm_tree::compaction::Leveled::default()), 0)?;
    assert!(tree
        .current_version()
        .iter_levels()
        .any(|level| level.run_count() == 1 && level.table_count() == 4));

    Ok(tree)
}

/// Bug (waste): a scan over several tables of a run probed their filters,
/// but each of those tables holds keys of the range, so if the range has one
/// prefix every probe finds it.
#[test]
#[cfg(feature = "metrics")]
fn scan_over_several_tables_of_a_run_probes_none() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = config(folder.path(), Arc::new(FixedPrefixExtractor::new(4))).open()?;

    for i in 0..2_000 {
        tree.insert(format!("aaaa{i:06}"), "v", 0);
    }
    tree.insert("zzzz000000", "v", 0);
    tree.flush_active_memtable(0)?;
    tree.major_compact(4_096, SeqNo::MAX)?;
    assert!(tree
        .current_version()
        .iter_levels()
        .any(|level| level.run_count() == 1 && level.table_count() >= 3));

    let queries = tree.metrics().prefix_filter_queries();
    assert_eq!(2_000, keys(tree.prefix("aaaa", SeqNo::MAX, None)).len());
    assert_eq!(queries, tree.metrics().prefix_filter_queries());

    Ok(())
}

/// A scan within one table of a run probes just that table.
#[test]
#[cfg(feature = "metrics")]
fn scan_within_one_table_of_a_run_probes_it() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = four_table_run(folder.path())?;

    let queries = tree.metrics().prefix_filter_queries();
    assert_eq!(
        100,
        keys(tree.range("cccc".."eeee9999", SeqNo::MAX, None)).len()
    );
    assert_eq!(queries, tree.metrics().prefix_filter_queries());

    assert_eq!(50, keys(tree.prefix("cccc", SeqNo::MAX, None)).len());
    assert_eq!(queries + 1, tree.metrics().prefix_filter_queries());

    Ok(())
}

/// Bug: after the configured extractor changed, tables written with the old
/// one stopped using their prefix filter: scans read them all, and without
/// whole-key hashes point reads read every table.
#[test]
#[cfg(feature = "metrics")]
fn old_tables_keep_their_extractor_after_config_change() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = config(folder.path(), Arc::new(FixedPrefixExtractor::new(4)))
            .whole_key_filtering(false)
            .open()?;

        for key in ["aaaa1", "aaaa2", "zzzz1"] {
            tree.insert(key, "v", 0);
        }
        tree.flush_active_memtable(0)?;
    }

    let tree = config(folder.path(), Arc::new(FixedPrefixExtractor::new(8)))
        .whole_key_filtering(false)
        .open()?;

    // The old table's prefix "mmmm" is absent
    let skipped = tree.metrics().io_skipped_by_prefix_filter();
    assert!(keys(tree.prefix("mmmm", SeqNo::MAX, None)).is_empty());
    assert!(tree.get("mmmm1", SeqNo::MAX)?.is_none());
    assert_eq!(skipped + 2, tree.metrics().io_skipped_by_prefix_filter());

    for key in ["aaaa1", "aaaa2", "zzzz1"] {
        assert!(tree.get(key, SeqNo::MAX)?.is_some(), "lost {key}");
    }
    assert_eq!(2, keys(tree.prefix("aaaa", SeqNo::MAX, None)).len());

    Ok(())
}

/// One prefix spread over many filter partitions: each partition must hold
/// the prefix, or a scan starting in a later partition skips the table.
#[test]
fn prefix_spanning_filter_partitions_keeps_every_key() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Whole-key hashes fill the 4 KiB partitions, ~1,600 keys each
    let tree = config(folder.path(), Arc::new(FixedPrefixExtractor::new(3)))
        .filter_block_partitioning_policy(PinningPolicy::all(true))
        .open()?;

    for i in 0..10_000 {
        tree.insert(format!("aaa{i:05}"), "v", 0);
    }
    tree.insert("bbb00000", "v", 0);
    tree.flush_active_memtable(0)?;

    for start in (0..10_000).step_by(1_000) {
        let (lo, hi) = (format!("aaa{start:05}"), format!("aaa{:05}", start + 100));
        assert_eq!(
            100,
            keys(tree.range(lo.as_str()..hi.as_str(), SeqNo::MAX, None)).len(),
            "{lo}..{hi}",
        );
    }

    assert_eq!(10_000, keys(tree.prefix("aaa", SeqNo::MAX, None)).len());
    assert_eq!(1, keys(tree.prefix("bbb", SeqNo::MAX, None)).len());

    Ok(())
}

/// Filter size depends on the number of distinct prefixes, not on how many
/// keys share each one.
#[test]
fn filter_size_counts_distinct_prefixes() -> lsm_tree::Result<()> {
    fn filter_size(keys_per_prefix: usize, partitioned: bool) -> lsm_tree::Result<u64> {
        let folder = tempfile::tempdir()?;
        let mut config = config(folder.path(), Arc::new(FixedPrefixExtractor::new(6)))
            .whole_key_filtering(false);
        if partitioned {
            config = config.filter_block_partitioning_policy(PinningPolicy::all(true));
        }
        let tree = config.open()?;

        for p in 0..2_000 {
            for k in 0..keys_per_prefix {
                tree.insert(format!("{p:06}{k:04}"), "v", 0);
            }
        }
        tree.flush_active_memtable(0)?;
        Ok(tree.filter_size())
    }

    assert_eq!(filter_size(1, false)?, filter_size(10, false)?);

    // Partitions split differently, and a prefix spanning a partition
    // boundary is in both partitions
    let one = filter_size(1, true)?;
    let ten = filter_size(10, true)?;
    assert!(
        ten <= one + one / 10,
        "1 key per prefix: {one}B, 10: {ten}B"
    );

    Ok(())
}

/// Two bytes if the third is at least 0x80, else three: "ab\x01" has prefix
/// "ab\x01", the next key "ab\x90" has "ab".
struct HighSplit;

impl PrefixExtractor for HighSplit {
    fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
        match key.get(2) {
            None => {}
            Some(&byte) if byte >= 0x80 => emit(2),
            Some(_) => emit(3),
        }
    }

    fn name(&self) -> &'static str {
        "test::HighSplit"
    }
}

/// A key's prefix is new to the filter when the previous key starts with the
/// same bytes but lacks that prefix; skipping it as a repeat loses the key on
/// point reads that probe the prefix.
#[test]
fn prefix_the_previous_key_lacked_reaches_the_filter() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = config(folder.path(), Arc::new(HighSplit))
        .whole_key_filtering(false)
        .open()?;

    tree.insert(b"ab\x01", "v", 0);
    tree.insert(b"ab\x90", "v", 0);
    tree.flush_active_memtable(0)?;

    assert!(tree.get(b"ab\x01", SeqNo::MAX)?.is_some());
    assert!(tree.get(b"ab\x90", SeqNo::MAX)?.is_some());

    Ok(())
}

/// A filter that cannot be read fails the range read: skipping the table
/// would drop its keys without a trace.
#[test]
fn range_surfaces_filter_read_errors() -> lsm_tree::Result<()> {
    use std::io::{Read, Seek, SeekFrom, Write};

    let folder = tempfile::tempdir()?;
    let config = || {
        config(folder.path(), Arc::new(FixedPrefixExtractor::new(3)))
            .filter_block_pinning_policy(PinningPolicy::all(false))
    };

    let path = {
        let tree = config().open()?;
        tree.insert("aaa0", "v", 0);
        tree.insert("ccc0", "v", 0);
        tree.flush_active_memtable(0)?;

        let version = tree.current_version();
        let table = version.iter_tables().next().unwrap();
        (*table.path).clone()
    };

    // Flip a byte inside the filter block
    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        let trailer = sfa::Reader::from_reader(&mut file).unwrap();
        let filter = trailer.toc().section(b"filter").unwrap();

        let pos = filter.pos() + filter.len() / 2;
        let mut byte = [0];
        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(&mut byte)?;
        file.seek(SeekFrom::Start(pos))?;
        file.write_all(&[!byte[0]])?;
    }

    let tree = config().open()?;
    let items: Vec<_> = tree
        .prefix("bbb", SeqNo::MAX, None)
        .map(Guard::into_inner)
        .collect();
    assert!(
        matches!(&*items, [Err(_)]),
        "expected the read error, got {:?}",
        items.iter().map(Result::is_ok).collect::<Vec<_>>(),
    );

    Ok(())
}
