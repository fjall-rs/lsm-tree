use crate::config::BenchConfig;
use crate::db::{make_random_key, make_sequential_key, make_value, prefill_sequential, read_seqno};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use lsm_tree::{AbstractTree, AnyTree};
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Barrier;
use std::time::Instant;

pub struct ReadWhileWriting;

impl Workload for ReadWhileWriting {
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Prefill the tree with sequential keys.
        prefill_sequential(tree, config, seqno)?;

        // Minimum 2 threads (1 reader + 1 writer). Cap readers so each
        // spawned reader does useful work (no empty-loop threads).
        let mut threads = config.threads.max(2);
        // Benchmark tool targets 64-bit; on 32-bit this caps at usize::MAX readers.
        let max_readers = usize::try_from(config.num.max(1)).unwrap_or(usize::MAX);
        let reader_count = std::cmp::min(threads - 1, max_readers);
        threads = reader_count + 1; // recompute for barrier
                                    // Distribute ops across readers, giving remainder to the last reader.
                                    // reader_count is always small (< --threads), safe to cast on all targets.
        let base_ops = config.num / reader_count as u64;
        let remainder = config.num % reader_count as u64;
        let barrier = Barrier::new(threads);

        // Timer starts before thread spawn — spawn overhead is negligible
        // (<1ms) compared to benchmark duration. Moving start() inside the
        // barrier would require sharing the reporter across threads.
        reporter.start();

        let scope_result: lsm_tree::Result<()> = std::thread::scope(|s| {
            // Spawn reader threads — borrow barrier by reference.
            let reader_handles: Vec<_> = (0..reader_count)
                .enumerate()
                .map(|(i, _)| {
                    let my_ops = base_ops + if (i as u64) < remainder { 1 } else { 0 };
                    let barrier = &barrier;
                    s.spawn(move || -> lsm_tree::Result<Reporter> {
                        let mut local_reporter = Reporter::new();
                        let mut rng = rand::rng();
                        barrier.wait();

                        for _ in 0..my_ops {
                            let read_seq = read_seqno(seqno);
                            let idx: u64 = rng.random_range(0..config.num);
                            let key = make_sequential_key(idx, config.key_size);

                            let t = Instant::now();
                            tree.get(&key, read_seq)?;
                            local_reporter.record_duration(t.elapsed());
                        }

                        Ok(local_reporter)
                    })
                })
                .collect();

            // Writer thread — also borrows barrier by reference.
            let writer_handle = s.spawn(|| {
                barrier.wait();

                // Writer inserts a fixed config.num keys — it may finish before
                // readers, which is intentional (fixed write volume, measured read
                // throughput). This matches RocksDB db_bench readwhilewriting.
                for _ in 0..config.num {
                    let key = make_random_key(config.key_size);
                    let value = make_value(config.value_size);
                    let seq = seqno.fetch_add(1, Ordering::Relaxed);
                    tree.insert(key, value, seq);
                }
            });

            // Collect reader results. Only reader ops are counted in ops_total —
            // this is a read throughput benchmark with concurrent write pressure,
            // matching RocksDB db_bench semantics.
            for handle in reader_handles {
                #[expect(clippy::expect_used, reason = "reader panic is unrecoverable")]
                let local_reporter = handle.join().expect("reader thread panicked")?;
                reporter.merge(&local_reporter);
            }

            // Stop timing once readers have finished; writer may still be running.
            reporter.stop();

            #[expect(clippy::expect_used, reason = "writer panic is unrecoverable")]
            writer_handle.join().expect("writer thread panicked");

            Ok(())
        });

        scope_result?;

        Ok(())
    }
}
