pub mod fillrandom;
pub mod fillseq;
pub mod mergerandom;
pub mod overwrite;
pub mod prefixscan;
pub mod readrandom;
pub mod readseq;
pub mod readwhilewriting;
pub mod seekrandom;

use crate::config::BenchConfig;
use crate::reporter::Reporter;
use lsm_tree::AnyTree;
use std::sync::atomic::AtomicU64;
use std::sync::Barrier;

/// All benchmark workloads implement this trait.
pub trait Workload {
    /// Run the benchmark, recording latencies into the reporter.
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()>;
}

/// Split `total` operations across `threads`, capping so no thread gets zero ops.
/// Returns `(actual_threads, base_ops, remainder)`.
///
/// Thread `t` gets `base_ops + if t < remainder { 1 } else { 0 }` ops.
/// Its global starting index is `t * base_ops + min(t, remainder)`.
pub(crate) fn distribute_ops(total: u64, threads: usize) -> (usize, u64, u64) {
    if total == 0 {
        return (1, 0, 0);
    }
    // Benchmark tool targets 64-bit; on 32-bit this caps at usize::MAX threads.
    let threads = std::cmp::min(threads.max(1), usize::try_from(total).unwrap_or(usize::MAX));
    let base = total / threads as u64;
    let rem = total % threads as u64;
    (threads, base, rem)
}

/// Run a multi-threaded benchmark. Each thread calls `thread_fn(thread_index, my_ops, start_op)`
/// and returns a local [`Reporter`]. Results are merged into the caller's `reporter`.
///
/// `start_op` is the global op index for thread `t` — useful for partitioned workloads
/// (e.g. fillseq where thread `t` writes keys `[start_op, start_op + my_ops)`).
/// Random-access workloads may ignore it.
///
/// The caller's reporter is started before threads launch but **not stopped** — the
/// caller must call `reporter.stop()` after any post-thread work (e.g. flush, compaction).
pub(crate) fn run_threaded<F>(
    config: &BenchConfig,
    reporter: &mut Reporter,
    thread_fn: F,
) -> lsm_tree::Result<()>
where
    F: Fn(usize, u64, u64) -> lsm_tree::Result<Reporter> + Sync,
{
    let (threads, base_ops, remainder) = distribute_ops(config.num, config.threads);

    reporter.start();

    // Fast-path: avoid thread::scope + Barrier overhead for the default
    // single-thread case so --threads 1 stays comparable to the prior
    // non-threaded implementation.
    if threads == 1 {
        let local = thread_fn(0, config.num, 0)?;
        reporter.merge(&local);
        return Ok(());
    }

    let barrier = Barrier::new(threads);

    let scope_result: lsm_tree::Result<()> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let my_ops = base_ops + if (t as u64) < remainder { 1 } else { 0 };
                let start = t as u64 * base_ops + std::cmp::min(t as u64, remainder);
                let barrier = &barrier;
                let thread_fn = &thread_fn;
                s.spawn(move || -> lsm_tree::Result<Reporter> {
                    barrier.wait();
                    thread_fn(t, my_ops, start)
                })
            })
            .collect();

        for handle in handles {
            #[expect(clippy::expect_used, reason = "thread panic is unrecoverable")]
            let local = handle.join().expect("thread panicked")?;
            reporter.merge(&local);
        }

        Ok(())
    });

    scope_result
}

/// Single source of truth for workload name → type mapping.
macro_rules! define_workloads {
    ( $( $name:expr => $ty:path ),+ $(,)? ) => {
        /// Create a workload by name.
        pub fn create_workload(name: &str) -> Option<Box<dyn Workload>> {
            match name {
                $( $name => Some(Box::new($ty)), )+
                _ => None,
            }
        }

        /// List all available benchmark names.
        pub fn available_benchmarks() -> &'static [&'static str] {
            &[ $( $name, )+ ]
        }
    };
}

define_workloads! {
    "fillseq" => fillseq::FillSeq,
    "fillrandom" => fillrandom::FillRandom,
    "readrandom" => readrandom::ReadRandom,
    "readseq" => readseq::ReadSeq,
    "seekrandom" => seekrandom::SeekRandom,
    "prefixscan" => prefixscan::PrefixScan,
    "overwrite" => overwrite::Overwrite,
    "mergerandom" => mergerandom::MergeRandom,
    "readwhilewriting" => readwhilewriting::ReadWhileWriting,
}
