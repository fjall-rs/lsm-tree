use crate::config::BenchConfig;
use crate::db::{fill_sequential_key, prefill_sequential, read_seqno};
use crate::reporter::Reporter;
use crate::workloads::{run_threaded, Workload};
use lsm_tree::{AbstractTree, AnyTree, Guard};
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

        // All threads seek random keys from shared prefilled data.
        run_threaded(config, reporter, |_t, my_ops, _start| {
            let mut local = Reporter::new();
            let read_seq = read_seqno(seqno);
            let mut rng = rand::rng();

            // Reusable buffer — fill_sequential_key writes in-place, no alloc per op.
            let mut key_buf = vec![0u8; config.key_size];

            for _ in 0..my_ops {
                let idx: u64 = rng.random_range(0..config.num);
                fill_sequential_key(&mut key_buf, idx);

                let t = Instant::now();
                // Seek to key and read the next entry.
                let mut iter = tree.range(key_buf.as_slice().., read_seq, None);
                // Force full value read including blob payload (BlobTree).
                if let Some(next) = iter.next() {
                    let _ = next.value()?;
                }
                local.record_duration(t.elapsed());
            }

            Ok(local)
        })?;

        reporter.stop();
        Ok(())
    }
}
