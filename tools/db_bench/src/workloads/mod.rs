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
