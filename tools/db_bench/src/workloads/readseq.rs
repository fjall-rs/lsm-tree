use crate::config::BenchConfig;
use crate::db::{make_sequential_key, prefill_sequential, read_seqno};
use crate::reporter::Reporter;
use crate::workloads::{run_threaded, Workload};
use lsm_tree::{AbstractTree, AnyTree, Guard};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct ReadSeq;

impl Workload for ReadSeq {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Prefill the tree with sequential keys.
        prefill_sequential(tree, config, seqno)?;

        let total_scanned = AtomicU64::new(0);

        // Each thread scans its own key range partition.
        run_threaded(config, reporter, |_t, my_ops, start| {
            let mut local = Reporter::new();
            if my_ops == 0 {
                return Ok(local);
            }

            let read_seq = read_seqno(seqno);
            let mut count = 0u64;

            let start_key = make_sequential_key(start, config.key_size);
            let mut iter = tree.range(start_key.., read_seq, None);

            while count < my_ops {
                let t = Instant::now();
                match iter.next() {
                    Some(item) => {
                        // Force full value read including blob payload (BlobTree).
                        let _ = item.value()?;
                        local.record_duration(t.elapsed());
                        count += 1;
                    }
                    None => {
                        break;
                    }
                }
            }

            total_scanned.fetch_add(count, Ordering::Relaxed);
            Ok(local)
        })?;

        reporter.stop();

        eprintln!("Scanned {} entries", total_scanned.load(Ordering::Relaxed));

        Ok(())
    }
}
