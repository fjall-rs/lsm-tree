use crate::config::BenchConfig;
use crate::db::make_sequential_key;
use crate::reporter::Reporter;
use crate::workloads::{run_threaded, Workload};
use lsm_tree::{
    config::{BlockSizePolicy, CompressionPolicy},
    AbstractTree, AnyTree, Cache, Config, MergeOperator, SequenceNumberCounter, UserValue,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Counter merge operator: sums i64 operands.
/// Used by the `mergerandom` benchmark to exercise real merge resolution.
struct CounterMerge;

impl MergeOperator for CounterMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut counter: i64 = match base_value {
            Some(bytes) if bytes.len() == 8 => {
                i64::from_le_bytes(bytes.try_into().unwrap_or_default())
            }
            Some(_) => return Err(lsm_tree::Error::MergeOperator),
            None => 0,
        };

        for operand in operands {
            if operand.len() != 8 {
                return Err(lsm_tree::Error::MergeOperator);
            }
            counter += i64::from_le_bytes((*operand).try_into().unwrap_or_default());
        }

        Ok(counter.to_le_bytes().to_vec().into())
    }
}

/// Writes merge operands to a small set of "hot" keys, flushing periodically
/// to create overlapping SSTs. Exercises the full merge path: operand storage,
/// lazy resolution during reads, and merge-aware compaction.
///
/// This is the lsm-tree equivalent of RocksDB's `mergerandom` benchmark.
pub struct MergeRandom;

impl Workload for MergeRandom {
    fn run(
        &self,
        _tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        let hot_keys: u64 = 1024;
        let flush_interval: u64 = 5_000;

        if config.key_size < 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "mergerandom requires --key-size >= 2 to preserve hot-key distinctness",
            )
            .into());
        }

        // Create a dedicated tree with a merge operator — the shared tree
        // from main doesn't have one configured.
        let tmpdir =
            tempfile::tempdir().map_err(|e| std::io::Error::other(format!("tmpdir: {e}")))?;
        let cache_bytes = config.cache_mb.checked_mul(1024 * 1024).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "cache size overflows u64")
        })?;
        let cache = Arc::new(Cache::with_capacity_bytes(cache_bytes));
        let mut builder = Config::new(
            tmpdir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_size_policy(BlockSizePolicy::all(config.block_size))
        .data_block_compression_policy(CompressionPolicy::all(config.compression.to_lsm()))
        .use_cache(cache)
        .with_merge_operator(Some(Arc::new(CounterMerge)));
        if config.use_blob_tree {
            builder = builder.with_kv_separation(Some(Default::default()));
        }
        let tree = builder.open()?;

        // All threads merge on shared hot keys — high contention is intentional.
        // Each thread processes its partition of the global op range, preserving
        // the same key distribution as single-threaded (key = global_index % hot_keys).
        run_threaded(config, reporter, |_t, my_ops, start| {
            let mut local = Reporter::new();

            for i in start..(start + my_ops) {
                let key_idx = i % hot_keys;
                let key = make_sequential_key(key_idx, config.key_size);
                // Each operand adds 1 to the counter for this key.
                let operand = 1_i64.to_le_bytes();
                let seq = seqno.fetch_add(1, Ordering::Relaxed);

                let t = Instant::now();
                tree.merge(key, operand.as_slice(), seq);
                local.record_duration(t.elapsed());

                if (i + 1) % flush_interval == 0 {
                    tree.flush_active_memtable(0)?;
                }
            }

            Ok(local)
        })?;

        // Final flush + compaction to exercise merge resolution.
        // Included in wall-clock timing (reporter.stop after compact).
        tree.flush_active_memtable(0)?;
        let compact_seqno = seqno.load(Ordering::Relaxed);
        tree.major_compact(64 * 1024 * 1024, compact_seqno)?;

        reporter.stop();

        // Verify merged counter: key 0 should have received exactly
        // (num / hot_keys) + (1 if num % hot_keys > 0) merge operands.
        let base = config.num / hot_keys;
        let remainder = config.num % hot_keys;
        let expected = (base + if remainder > 0 { 1 } else { 0 }) as i64;
        let read_seqno = seqno.load(Ordering::Relaxed);
        let sample_key = make_sequential_key(0, config.key_size);
        match tree.get(&sample_key, read_seqno)? {
            Some(val) => {
                if val.len() < 8 {
                    return Err(std::io::Error::other(format!(
                        "merge result too short: {} bytes (expected 8)",
                        val.len()
                    ))
                    .into());
                }
                let mut buf = [0_u8; 8];
                buf.copy_from_slice(&val[..8]);
                let actual = i64::from_le_bytes(buf);

                if actual != expected {
                    return Err(std::io::Error::other(format!(
                        "merge result mismatch: got {actual}, expected {expected}"
                    ))
                    .into());
                }

                eprintln!(
                    "Merged {} operands over {} hot keys, counter verified: {actual} (expected {expected}), {} tables",
                    config.num, hot_keys, tree.table_count(),
                );
            }
            None => {
                return Err(
                    std::io::Error::other("sample key missing after merge/compaction").into(),
                );
            }
        }

        Ok(())
    }
}
