use crate::config::BenchConfig;
use crate::db::{make_random_key, make_value};
use crate::reporter::Reporter;
use crate::workloads::{run_threaded, Workload};
use lsm_tree::{AbstractTree, AnyTree};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct FillRandom;

impl Workload for FillRandom {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // All threads insert random keys — memtable contention is intentional.
        run_threaded(config, reporter, |_t, my_ops, _start| {
            let mut local = Reporter::new();

            for _ in 0..my_ops {
                // Key/value allocation is outside the timed region (before Instant::now).
                let key = make_random_key(config.key_size);
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
