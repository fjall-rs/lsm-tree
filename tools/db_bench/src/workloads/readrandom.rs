use crate::config::BenchConfig;
use crate::db::{fill_sequential_key, prefill_sequential, read_seqno};
use crate::reporter::Reporter;
use crate::workloads::{run_threaded, Workload};
use lsm_tree::{AbstractTree, AnyTree};
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct ReadRandom;

impl Workload for ReadRandom {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Prefill the tree with sequential keys.
        prefill_sequential(tree, config, seqno)?;

        let found = AtomicU64::new(0);

        // All threads read random keys from shared prefilled data.
        run_threaded(config, reporter, |_t, my_ops, _start| {
            let mut local = Reporter::new();
            let read_seq = read_seqno(seqno);
            let mut rng = rand::rng();
            let mut local_found = 0u64;

            // Reusable buffer — fill_sequential_key writes in-place, no alloc per op.
            let mut key_buf = vec![0u8; config.key_size];

            for _ in 0..my_ops {
                let idx: u64 = rng.random_range(0..config.num);
                fill_sequential_key(&mut key_buf, idx);

                let t = Instant::now();
                let result = tree.get(&key_buf, read_seq)?;
                local.record_duration(t.elapsed());

                if result.is_some() {
                    local_found += 1;
                }
            }

            found.fetch_add(local_found, Ordering::Relaxed);
            Ok(local)
        })?;

        reporter.stop();

        if config.num > 0 {
            let total_found = found.load(Ordering::Relaxed);
            let hit_rate = total_found as f64 / config.num as f64 * 100.0;
            eprintln!("Hit rate: {total_found}/{} ({hit_rate:.1}%)", config.num);
        }

        Ok(())
    }
}
