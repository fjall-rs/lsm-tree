use crate::config::BenchConfig;
use crate::db::{make_sequential_key, prefill_sequential, read_seqno};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use lsm_tree::{AbstractTree, AnyTree, Guard}; // Guard trait required for .value()
use rand::Rng;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

pub struct SeekRandom;

impl Workload for SeekRandom {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Prefill the tree with sequential keys.
        prefill_sequential(tree, config, seqno)?;

        let read_seq = read_seqno(seqno);
        let mut rng = rand::rng();

        reporter.start();

        for _ in 0..config.num {
            let idx: u64 = rng.random_range(0..config.num);
            let key = make_sequential_key(idx, config.key_size);

            let t = Instant::now();
            // Seek to key and read the next entry.
            let mut iter = tree.range(key.., read_seq, None);
            // Force full value read including blob payload (BlobTree).
            // Guard::value() reads the full blob payload.
            if let Some(next) = iter.next() {
                let _ = next.value()?;
            }
            reporter.record_duration(t.elapsed());
        }

        reporter.stop();
        Ok(())
    }
}
