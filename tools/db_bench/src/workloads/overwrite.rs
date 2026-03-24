use crate::config::BenchConfig;
use crate::db::{make_sequential_key, make_value, prefill_sequential};
use crate::reporter::Reporter;
use crate::workloads::{run_threaded, Workload};
use lsm_tree::{AbstractTree, AnyTree};
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct Overwrite;

impl Workload for Overwrite {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Prefill the tree with sequential keys.
        prefill_sequential(tree, config, seqno)?;

        // All threads overwrite random existing keys — contention is intentional.
        run_threaded(config, reporter, |_t, my_ops, _start| {
            let mut local = Reporter::new();
            let mut rng = rand::rng();

            for _ in 0..my_ops {
                let idx: u64 = rng.random_range(0..config.num);
                let key = make_sequential_key(idx, config.key_size);
                let value = make_value(config.value_size);
                let seq = seqno.fetch_add(1, Ordering::Relaxed);

                let t = Instant::now();
                tree.insert(key, value, seq);
                local.record_duration(t.elapsed());
            }

            Ok(local)
        })?;

        reporter.stop();
        Ok(())
    }
}
