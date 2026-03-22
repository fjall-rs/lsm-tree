mod config;
mod db;
mod reporter;
mod workloads;

use crate::config::{BenchConfig, Compression};
use crate::reporter::{JsonConfig, Reporter};
use crate::workloads::{available_benchmarks, create_workload};
use clap::Parser;
use lsm_tree::AbstractTree; // for get_highest_seqno
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;

#[derive(Parser, Debug)]
#[command(
    name = "db_bench",
    about = "LSM-tree benchmark suite (RocksDB db_bench compatible)"
)]
struct Cli {
    /// Benchmark workload to run. Use "all" to run every workload.
    #[arg(long, value_parser = parse_benchmark)]
    benchmark: String,

    /// Number of operations.
    #[arg(long, default_value = "1000000")]
    num: u64,

    /// Key size in bytes.
    #[arg(long, default_value = "16")]
    key_size: usize,

    /// Value size in bytes.
    #[arg(long, default_value = "100")]
    value_size: usize,

    /// Number of concurrent threads.
    #[arg(long, default_value = "1")]
    threads: usize,

    /// Block cache size in MB.
    #[arg(long, default_value = "64")]
    cache_mb: u64,

    /// Compression type: none, lz4, zstd.
    #[arg(long, default_value = "none")]
    compression: Compression,

    /// Data block size in bytes.
    #[arg(long, default_value = "4096")]
    block_size: u32,

    /// Use BlobTree (key-value separation) instead of standard Tree.
    #[arg(long)]
    use_blob_tree: bool,

    /// Output results as JSON.
    #[arg(long)]
    json: bool,

    /// Output results in github-action-benchmark format (customBiggerIsBetter).
    /// Implies running all benchmarks if --benchmark is "all".
    #[arg(long)]
    github_json: bool,

    /// Database directory path. If not set, a temporary directory is used.
    /// Note: some workloads (e.g. `prefixscan`, `mergerandom`) create their
    /// own temporary database (they require special tree configuration) and
    /// will not reuse this path.
    #[arg(long)]
    db: Option<PathBuf>,
}

fn parse_benchmark(s: &str) -> Result<String, String> {
    if s == "all" {
        return Ok(s.to_string());
    }
    let available = available_benchmarks();
    if available.contains(&s) {
        Ok(s.to_string())
    } else {
        Err(format!(
            "unknown benchmark '{}'. Available: all, {}",
            s,
            available.join(", ")
        ))
    }
}

fn main() {
    let cli = Cli::parse();

    let bench_config = BenchConfig {
        num: cli.num,
        key_size: cli.key_size,
        value_size: cli.value_size,
        threads: cli.threads,
        cache_mb: cli.cache_mb,
        compression: cli.compression,
        block_size: cli.block_size,
        use_blob_tree: cli.use_blob_tree,
    };

    if bench_config.num == 0 {
        eprintln!("Error: --num must be > 0");
        std::process::exit(1);
    }

    if bench_config.key_size == 0 {
        eprintln!("Error: --key-size must be > 0");
        std::process::exit(1);
    }

    // --json emits one JSON object per workload; with "all" that produces
    // concatenated objects which is not valid JSON.  Use --github-json instead.
    if cli.json && cli.benchmark == "all" {
        eprintln!("Error: --json does not support --benchmark all; use --github-json");
        std::process::exit(1);
    }

    // Warn if key space is smaller than num ops (causes silent overwrites).
    if bench_config.key_size < 8 {
        let max_keys = 1u64 << (bench_config.key_size * 8);
        if bench_config.num > max_keys {
            eprintln!(
                "Warning: --key-size {} supports only {} distinct keys, \
                 but --num {} was requested. Keys will repeat (overwrites).",
                bench_config.key_size, max_keys, bench_config.num,
            );
        }
    }

    let benchmarks: Vec<&str> = if cli.benchmark == "all" {
        available_benchmarks().to_vec()
    } else {
        vec![&cli.benchmark]
    };

    // Collect github-action-benchmark entries when --github-json is set.
    let mut github_entries: Vec<serde_json::Value> = Vec::new();
    let mut failures = 0u32;

    for benchmark_name in &benchmarks {
        if let Err(e) = run_single(benchmark_name, &bench_config, &cli, &mut github_entries) {
            eprintln!("Error: {benchmark_name} failed: {e}");
            failures += 1;
        }
    }

    if cli.github_json {
        let array = serde_json::Value::Array(github_entries);
        match serde_json::to_string_pretty(&array) {
            Ok(json) => println!("{json}"),
            Err(e) => {
                eprintln!("Error: failed to serialize GitHub JSON: {e}");
                failures += 1;
            }
        }
    }

    if failures > 0 {
        eprintln!("{failures} benchmark(s) failed");
        std::process::exit(1);
    }
}

fn run_single(
    benchmark_name: &str,
    bench_config: &BenchConfig,
    cli: &Cli,
    github_entries: &mut Vec<serde_json::Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Each benchmark gets a fresh temp directory by default.
    // With --db, all workloads share the same directory so data persists
    // across runs (e.g. fill first, then benchmark reads in a second invocation).
    // Note: within a single run each workload still prefills its own data.
    let _tmpdir;
    let db_path = match &cli.db {
        Some(p) => p.clone(),
        None => {
            _tmpdir = tempfile::tempdir()?;
            _tmpdir.path().to_path_buf()
        }
    };

    eprintln!("=== db_bench: {benchmark_name} ===");
    eprintln!(
        "num={} key_size={} value_size={} threads={} cache={}MB",
        cli.num, cli.key_size, cli.value_size, cli.threads, cli.cache_mb,
    );

    let tree = config::create_tree(&db_path, bench_config)?;
    // When --db reuses a directory, start from existing highest seqno to
    // avoid duplicate/non-monotonic sequence numbers.
    let initial_seqno = tree.get_highest_seqno().map_or(1, |s| s.saturating_add(1));
    let seqno = AtomicU64::new(initial_seqno);
    let mut reporter = Reporter::new();

    let workload = create_workload(benchmark_name)
        .ok_or_else(|| format!("unknown benchmark '{benchmark_name}'"))?;

    workload.run(&tree, bench_config, &seqno, &mut reporter)?;

    let entry_size = bench_config.entry_size();

    if cli.github_json {
        let s = reporter.summary(entry_size);
        github_entries.push(serde_json::json!({
            "name": benchmark_name,
            "value": s.ops_per_sec,
            "unit": "ops/sec",
            "extra": format!(
                "P50: {:.1}us | P99: {:.1}us | P99.9: {:.1}us\nthreads: {} | elapsed: {:.2}s | num: {}",
                s.p50, s.p99, s.p999, cli.threads, s.secs, cli.num,
            ),
        }));
    } else if cli.json {
        let json_config = JsonConfig {
            num: cli.num,
            key_size: cli.key_size,
            value_size: cli.value_size,
            entry_size,
            threads: cli.threads,
            compression: cli.compression.to_string(),
        };
        println!("{}", reporter.to_json(benchmark_name, &json_config));
    } else {
        reporter.print_human(benchmark_name, entry_size);
    }

    Ok(())
}
