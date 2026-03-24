use crate::config::BenchConfig;
use crate::db::{prefill_prefix_keys, read_seqno};
use crate::reporter::Reporter;
use crate::workloads::{run_threaded, Workload};
use lsm_tree::{
    config::{BlockSizePolicy, CompressionPolicy},
    AbstractTree, AnyTree, Cache, Config, Guard, PrefixExtractor, SequenceNumberCounter,
};
use rand::Rng;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

pub struct PrefixScan;

const NUM_PREFIXES: u16 = 256;
const SCAN_LIMIT: usize = 10;
/// Prefix length in bytes — matches the u16 BE prefix in prefill_prefix_keys.
const PREFIX_LEN: usize = 2;

/// Fixed-length prefix extractor: first 2 bytes of each key.
/// Enables prefix bloom filters to skip tables without matching prefixes.
struct FixedPrefixExtractor;

impl PrefixExtractor for FixedPrefixExtractor {
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        if let Some(prefix) = key.get(..PREFIX_LEN) {
            Box::new(std::iter::once(prefix))
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn is_valid_scan_boundary(&self, prefix: &[u8]) -> bool {
        prefix.len() == PREFIX_LEN
    }
}

impl Workload for PrefixScan {
    fn run(
        &self,
        _tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Prefix keys: 2-byte u16 prefix + 2-byte u16 suffix = 4 bytes minimum.
        if config.key_size < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "prefixscan requires --key-size >= 4 (2-byte prefix + 2-byte suffix)",
            )
            .into());
        }

        // Reject num values that exceed the u16 prefix x u16 suffix space.
        let max_keys = u64::from(NUM_PREFIXES) * (u64::from(u16::MAX) + 1);
        if config.num > max_keys {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "prefixscan --num {} exceeds prefix corpus capacity ({max_keys})",
                    config.num,
                ),
            )
            .into());
        }

        // Create a dedicated tree with prefix extractor for bloom-based
        // table skipping — the shared tree has no extractor configured.
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
        .prefix_extractor(Arc::new(FixedPrefixExtractor));
        if config.use_blob_tree {
            builder = builder.with_kv_separation(Some(Default::default()));
        }
        let tree = builder.open()?;

        // Prefill with structured prefix keys.
        prefill_prefix_keys(&tree, config, seqno, NUM_PREFIXES)?;

        // All threads scan random prefixes from shared prefilled data.
        run_threaded(config, reporter, |_t, my_ops, _start| {
            let mut local = Reporter::new();
            let read_seq = read_seqno(seqno);
            let mut rng = rand::rng();

            for _ in 0..my_ops {
                let prefix_idx: u16 = rng.random_range(0..NUM_PREFIXES);
                let prefix_bytes = prefix_idx.to_be_bytes();

                let t = Instant::now();
                let mut iter = tree.prefix(prefix_bytes, read_seq, None);
                for _ in 0..SCAN_LIMIT {
                    let Some(item) = iter.next() else { break };
                    // Force full value read including blob payload (BlobTree).
                    let _ = item.value()?;
                }
                local.record_duration(t.elapsed());
            }

            Ok(local)
        })?;

        reporter.stop();
        Ok(())
    }
}
