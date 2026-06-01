// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Benchmark mirroring the `readwhilewriting` workload from RocksDB PR #14743 to
//! quantify the impact of `use_direct_io_for_compaction_reads` and
//! `use_direct_io_for_flush_and_compaction`.
//!
//! Build + run:
//!   cargo run --release --features lz4 --example direct_io_bench
//!
//! Tunable knobs are taken from environment variables so each invocation is
//! reproducible from CI:
//!   - LSMT_DIO_NUM        Hot key subset (default 7500). Readers pick from [0, NUM).
//!   - LSMT_DIO_TOTAL      Total keys in the source DB (default 200_000).
//!   - LSMT_DIO_VALUE_SIZE Value size in bytes (default 4096).
//!   - LSMT_DIO_THREADS    Reader threads (default 4).
//!   - LSMT_DIO_DURATION   Benchmark duration per config in seconds (default 30).
//!   - LSMT_DIO_WARMUP     Warmup duration in seconds (default 3).
//!   - LSMT_DIO_TARGET_SIZE Compaction target_size in bytes (default 16 MiB).
//!   - LSMT_DIO_WRITE_BPS  Writer throttle in bytes/sec (default 50 MiB/s).

use lsm_tree::{AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc as StdArc;
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[derive(Copy, Clone)]
struct Settings {
    /// Hot key range readers select from.
    num: u32,
    /// Total keys pre-populated and writer's target range.
    total_keys: u32,
    value_size: usize,
    reader_threads: usize,
    duration_secs: u64,
    warmup_secs: u64,
    target_size: u64,
    write_bps: u64,
}

impl Settings {
    fn from_env() -> Self {
        Self {
            num: env_usize("LSMT_DIO_NUM", 7_500) as u32,
            total_keys: env_usize("LSMT_DIO_TOTAL", 200_000) as u32,
            value_size: env_usize("LSMT_DIO_VALUE_SIZE", 4_096),
            reader_threads: env_usize("LSMT_DIO_THREADS", 4),
            duration_secs: env_usize("LSMT_DIO_DURATION", 30) as u64,
            warmup_secs: env_usize("LSMT_DIO_WARMUP", 3) as u64,
            target_size: env_usize("LSMT_DIO_TARGET_SIZE", 16 * 1_024 * 1_024) as u64,
            write_bps: env_usize("LSMT_DIO_WRITE_BPS", 50 * 1_024 * 1_024) as u64,
        }
    }
}

fn fmt_key(i: u32) -> [u8; 16] {
    // 16-byte fixed-width keys, padded to keep ordering by integer value.
    let mut k = [b'0'; 16];
    let s = format!("{i:016}");
    k.copy_from_slice(s.as_bytes());
    k
}

/// Fills a value with deterministic pseudo-random bytes.
///
/// The seed is part of the payload, so callers can make each key or write
/// operation unique while keeping runs reproducible.
pub(crate) fn fill_incompressible_value(value: &mut [u8], seed: u64) {
    let mut state = seed
        .wrapping_add(0xCAFE_BABE_DEAD_BEEFu64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15u64);
    if state == 0 {
        state = 0xA076_1D64_78BD_642Fu64;
    }

    for chunk in value.chunks_mut(8) {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let bytes = state.wrapping_mul(0x2545_F491_4F6C_DD1Du64).to_le_bytes();
        let n = chunk.len();
        chunk.copy_from_slice(&bytes[..n]);
    }
}

/// Pre-populates the source DB once. Reused (via copy) for every config below
/// so the per-config measurement starts from an identical on-disk state.
///
/// Values are filled with deterministic pseudo-random bytes so compression doesn't
/// shrink the on-disk footprint below the user-data size — otherwise the whole DB
/// fits in RAM and the direct-I/O benefit is invisible.
fn populate_source(
    settings: &Settings,
    source_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if source_dir.join("manifest").exists() || source_dir.join("tables").exists() {
        eprintln!("source DB already populated at {}", source_dir.display());
        return Ok(());
    }
    eprintln!(
        "Populating source DB: {} keys × {} B = {:.1} MiB user data",
        settings.total_keys,
        settings.value_size,
        (settings.total_keys as f64 * settings.value_size as f64) / 1_048_576.0,
    );
    let t0 = Instant::now();
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(source_dir, seqno.clone(), SequenceNumberCounter::default()).open()?;
    let mut value = vec![0u8; settings.value_size];
    for i in 0..settings.total_keys {
        fill_incompressible_value(&mut value, u64::from(i));
        tree.insert(fmt_key(i), value.as_slice(), seqno.next());
        if i.is_multiple_of(50_000) {
            tree.flush_active_memtable(0)?;
        }
    }
    tree.flush_active_memtable(0)?;
    // Compact to a stable shape so each config below starts from the same layout.
    tree.major_compact(settings.target_size, 0)?;
    drop(tree);
    eprintln!(
        "  populated + compacted in {:.1}s, on-disk size {:.1} MiB",
        t0.elapsed().as_secs_f64(),
        dir_size_bytes(source_dir)? as f64 / 1_048_576.0,
    );
    Ok(())
}

fn dir_size_bytes(p: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    for e in walkdir(p)? {
        let meta = e.metadata()?;
        if meta.is_file() {
            total += meta.len();
        }
    }
    Ok(total)
}

fn walkdir(p: &Path) -> std::io::Result<Vec<std::fs::DirEntry>> {
    let mut out = vec![];
    let mut stack = vec![p.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d)? {
            let e = e?;
            if e.file_type()?.is_dir() {
                stack.push(e.path());
            } else {
                out.push(e);
            }
        }
    }
    Ok(out)
}

fn copy_dir(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

#[derive(Copy, Clone, Debug)]
struct ConfigPermutation {
    label: &'static str,
    direct_reads: bool,
    direct_writes: bool,
}

const PERMUTATIONS: &[ConfigPermutation] = &[
    ConfigPermutation {
        label: "buffered",
        direct_reads: false,
        direct_writes: false,
    },
    ConfigPermutation {
        label: "writes_only",
        direct_reads: false,
        direct_writes: true,
    },
    ConfigPermutation {
        label: "reads_only",
        direct_reads: true,
        direct_writes: false,
    },
    ConfigPermutation {
        label: "both",
        direct_reads: true,
        direct_writes: true,
    },
];

struct RunResult {
    label: &'static str,
    read_ops: u64,
    write_ops: u64,
    elapsed: Duration,
    latencies_ns: Vec<u32>,
}

impl RunResult {
    fn throughput(&self) -> f64 {
        self.read_ops as f64 / self.elapsed.as_secs_f64()
    }

    /// Percentile in microseconds.
    fn percentile_us(&self, p: f64) -> f64 {
        if self.latencies_ns.is_empty() {
            return f64::NAN;
        }
        let mut sorted = self.latencies_ns.clone();
        sorted.sort_unstable();
        #[expect(clippy::cast_precision_loss, reason = "bench math")]
        let idx_f = (p / 100.0) * (sorted.len() - 1) as f64;
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "bench math"
        )]
        let idx = idx_f.round() as usize;
        sorted[idx.min(sorted.len() - 1)] as f64 / 1_000.0
    }
}

fn run_one(
    settings: &Settings,
    perm: ConfigPermutation,
    _source_dir: &Path,
    scratch_base: &Path,
) -> Result<RunResult, Box<dyn std::error::Error>> {
    // The scratch dir was prepared during populate (outside the memory cgroup) so
    // the per-config run is dominated by the workload, not by `cp`'s page-cache
    // footprint.
    let scratch = scratch_base.join(perm.label);
    if !scratch.exists() {
        return Err(format!(
            "scratch dir missing at {}; populate phase did not run",
            scratch.display()
        )
        .into());
    }
    eprintln!(
        "\n[{}] opening scratch dir at {}",
        perm.label,
        scratch.display()
    );

    let seqno = SequenceNumberCounter::default();
    // Keep the block cache small (4 MiB instead of 16 MiB default) so the cgroup
    // headroom is consumed by the kernel page cache — where direct I/O actually
    // matters — rather than by lsm-tree's own block cache.
    let block_cache = StdArc::new(Cache::with_capacity_bytes(4 * 1024 * 1024));
    let tree = Config::new(&scratch, seqno.clone(), SequenceNumberCounter::default())
        .use_cache(block_cache)
        .use_direct_io_for_compaction_reads(perm.direct_reads)
        .use_direct_io_for_flush_and_compaction(perm.direct_writes)
        .open()?;
    let tree = Arc::new(tree);

    let stop = Arc::new(AtomicBool::new(false));
    let read_ops = Arc::new(AtomicU64::new(0));
    let write_ops = Arc::new(AtomicU64::new(0));

    // Latency samples: bounded reservoir per reader thread so memory stays flat
    // even at high throughput. SAMPLE_CAP=2M × 4 threads × 4 B per sample = 32 MiB.
    // u32 ns covers up to ~4 s which comfortably bounds any read latency we expect.
    const SAMPLE_CAP: usize = 2_000_000;
    let mut reader_handles = vec![];
    let mut latency_bufs = vec![];

    // Spawn reader threads.
    for thread_id in 0..settings.reader_threads {
        let tree = Arc::clone(&tree);
        let stop = Arc::clone(&stop);
        let read_ops = Arc::clone(&read_ops);
        let num = settings.num;
        let warmup = Duration::from_secs(settings.warmup_secs);
        let h = thread::spawn(move || {
            let mut rng_state = 0x9E37_79B9_7F4A_7C15u64.wrapping_add(thread_id as u64);
            let mut latencies: Vec<u32> = Vec::with_capacity(SAMPLE_CAP);
            let mut seen: u64 = 0;
            let start = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                // xorshift64* — fast, deterministic, decent distribution for a key index.
                rng_state ^= rng_state >> 12;
                rng_state ^= rng_state << 25;
                rng_state ^= rng_state >> 27;
                let key_idx =
                    ((rng_state.wrapping_mul(0x2545_F491_4F6C_DD1Du64)) >> 32) as u32 % num.max(1);

                let key = fmt_key(key_idx);
                let t0 = Instant::now();
                let r = tree.get(key, SeqNo::MAX);
                #[expect(clippy::cast_possible_truncation, reason = "u32 ns covers ~4s")]
                let dt = t0.elapsed().as_nanos().min(u128::from(u32::MAX)) as u32;

                if start.elapsed() >= warmup && r.is_ok() {
                    read_ops.fetch_add(1, Ordering::Relaxed);
                    // Algorithm R reservoir sampling: increment `seen` first so
                    // P(replace) = SAMPLE_CAP / seen, which goes to 0 as the stream
                    // grows (rather than 1, which would over-replace the earliest
                    // samples — the bug in an earlier revision).
                    seen += 1;
                    if latencies.len() < SAMPLE_CAP {
                        latencies.push(dt);
                    } else {
                        rng_state ^= rng_state >> 12;
                        rng_state ^= rng_state << 25;
                        rng_state ^= rng_state >> 27;
                        #[expect(
                            clippy::cast_possible_truncation,
                            reason = "seen is u64; usize on 64-bit hosts"
                        )]
                        let idx = (rng_state as usize) % (seen as usize);
                        if idx < SAMPLE_CAP {
                            #[expect(
                                clippy::indexing_slicing,
                                reason = "idx bounded by SAMPLE_CAP"
                            )]
                            {
                                latencies[idx] = dt;
                            }
                        }
                    }
                }
            }
            latencies
        });
        reader_handles.push(h);
    }

    // Spawn writer thread.
    let writer_handle = {
        let tree = Arc::clone(&tree);
        let stop = Arc::clone(&stop);
        let write_ops = Arc::clone(&write_ops);
        let seqno = seqno.clone();
        let total_keys = settings.total_keys;
        let value_size = settings.value_size;
        let target_bps = settings.write_bps;
        thread::spawn(move || {
            let mut rng_state = 0xDEAD_BEEF_CAFE_BABEu64;
            let mut value = vec![0u8; value_size];
            let mut local_writes = 0u64;
            let bytes_per_op = (16 + value_size) as u64;
            // Token-bucket throttle.
            let mut tokens: i64 = 0;
            let mut last_tick = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                // Replenish tokens proportional to elapsed time.
                let now = Instant::now();
                let dt = now.duration_since(last_tick);
                last_tick = now;
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_possible_wrap,
                    reason = "throttle math"
                )]
                let new_tokens = (target_bps as f64 * dt.as_secs_f64()) as i64;
                tokens = tokens
                    .saturating_add(new_tokens)
                    .min((target_bps * 2) as i64);
                if tokens < bytes_per_op as i64 {
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }

                rng_state ^= rng_state >> 12;
                rng_state ^= rng_state << 25;
                rng_state ^= rng_state >> 27;
                let key_idx = ((rng_state.wrapping_mul(0x2545_F491_4F6C_DD1Du64)) >> 32) as u32
                    % total_keys.max(1);
                let key = fmt_key(key_idx);
                fill_incompressible_value(
                    &mut value,
                    u64::from(key_idx) ^ local_writes.wrapping_mul(0x9E37_79B9_7F4A_7C15u64),
                );
                tree.insert(key, value.as_slice(), seqno.next());
                local_writes = local_writes.wrapping_add(1);
                tokens -= bytes_per_op as i64;
                write_ops.fetch_add(1, Ordering::Relaxed);

                // Sealed-memtable flush is triggered by AbstractTree::insert size only when
                // a memtable rotation occurs; for sustained writes we explicitly flush
                // periodically to drive the compactor.
                if write_ops.load(Ordering::Relaxed).is_multiple_of(2_000) {
                    let _ = tree.flush_active_memtable(0);
                }
            }
        })
    };

    // Compaction driver: rolls up L0 every 2s so the workload exercises the
    // compaction-read path continuously without letting L0 grow large enough to
    // make each compaction expensive (which causes memory pressure inside a
    // cgroup and starves the readers).
    let compaction_handle = {
        let tree = Arc::clone(&tree);
        let stop = Arc::clone(&stop);
        let target_size = settings.target_size;
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..20 {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
                let _ = tree.major_compact(target_size, 0);
            }
        })
    };

    let total = Duration::from_secs(settings.warmup_secs + settings.duration_secs);
    thread::sleep(total);
    stop.store(true, Ordering::Relaxed);

    for h in reader_handles {
        latency_bufs.push(h.join().expect("reader thread"));
    }
    writer_handle.join().expect("writer thread");
    compaction_handle.join().expect("compaction thread");

    // Measurement window is the requested duration (warmup excluded). The
    // wall-clock wait for the compaction thread to finish after `stop` does not
    // contribute additional read ops, so it must not inflate the divisor.
    let elapsed = Duration::from_secs(settings.duration_secs);
    let mut all_latencies = vec![];
    for buf in latency_bufs {
        all_latencies.extend(buf);
    }

    eprintln!(
        "[{}] read_ops={} write_ops={} samples={} elapsed={:.2}s",
        perm.label,
        read_ops.load(Ordering::Relaxed),
        write_ops.load(Ordering::Relaxed),
        all_latencies.len(),
        elapsed.as_secs_f64(),
    );

    Ok(RunResult {
        label: perm.label,
        read_ops: read_ops.load(Ordering::Relaxed),
        write_ops: write_ops.load(Ordering::Relaxed),
        elapsed,
        latencies_ns: all_latencies,
    })
}

fn print_results_table(results: &[RunResult]) {
    println!("\nResults ({} configs):", results.len());
    println!(
        "{:<14}  {:>11}  {:>9}  {:>9}  {:>10}  {:>11}",
        "config", "throughput", "P50 (µs)", "P99 (µs)", "P99.9 (µs)", "P99.99 (µs)",
    );
    println!("{}", "-".repeat(78));
    // Resolve the baseline by label so single-config "run" mode (which passes a
    // one-element slice) doesn't compare a non-buffered result against itself.
    let baseline = results.iter().find(|r| r.label == "buffered");
    let baseline_thru = baseline.map(RunResult::throughput).unwrap_or(0.0);
    let baseline_p50 = baseline.map(|r| r.percentile_us(50.0)).unwrap_or(0.0);
    let baseline_p99 = baseline.map(|r| r.percentile_us(99.0)).unwrap_or(0.0);
    let baseline_p999 = baseline.map(|r| r.percentile_us(99.9)).unwrap_or(0.0);
    let baseline_p9999 = baseline.map(|r| r.percentile_us(99.99)).unwrap_or(0.0);

    for r in results {
        let pct = |new: f64, base: f64| -> String {
            if base == 0.0 || base.is_nan() {
                return String::new();
            }
            let delta = (new - base) / base * 100.0;
            format!("{delta:+.1}%")
        };

        println!(
            "{:<14}  {:>11.0}  {:>9.2}  {:>9.2}  {:>10.1}  {:>11.1}",
            r.label,
            r.throughput(),
            r.percentile_us(50.0),
            r.percentile_us(99.0),
            r.percentile_us(99.9),
            r.percentile_us(99.99),
        );

        if r.label != "buffered" && baseline.is_some() {
            println!(
                "{:<14}  {:>11}  {:>9}  {:>9}  {:>10}  {:>11}",
                "  vs buffered",
                pct(r.throughput(), baseline_thru),
                pct(r.percentile_us(50.0), baseline_p50),
                pct(r.percentile_us(99.0), baseline_p99),
                pct(r.percentile_us(99.9), baseline_p999),
                pct(r.percentile_us(99.99), baseline_p9999),
            );
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Settings::from_env();
    let mode = std::env::var("LSMT_DIO_MODE").unwrap_or_else(|_| "all".into());
    eprintln!(
        "Settings: mode={mode} num={} (hot) total={} value_size={} threads={} duration={}s warmup={}s target_size={} write_bps={}",
        settings.num,
        settings.total_keys,
        settings.value_size,
        settings.reader_threads,
        settings.duration_secs,
        settings.warmup_secs,
        settings.target_size,
        settings.write_bps,
    );

    // Workdir layout:
    //   /work/data/source/   — pre-populated DB, written once, mounted read-only
    //                          for the per-config runs.
    //   /work/data/scratch/  — per-config working copy.
    //
    // LSMT_DIO_WORKDIR overrides the default tmp location so the populate step
    // can write to a host-mounted volume.
    let workdir: PathBuf = std::env::var("LSMT_DIO_WORKDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("lsm_tree_direct_io_bench"));
    std::fs::create_dir_all(&workdir)?;
    let source_dir = workdir.join("source");
    let scratch_base = workdir.join("scratch");
    std::fs::create_dir_all(&scratch_base)?;

    match mode.as_str() {
        "populate" => {
            std::fs::create_dir_all(&source_dir)?;
            populate_source(&settings, &source_dir)?;
            // Pre-copy a fresh scratch dir per config here, while we still have
            // unrestricted memory. The run phase only opens these.
            for perm in PERMUTATIONS {
                let dst = scratch_base.join(perm.label);
                if dst.exists() {
                    std::fs::remove_dir_all(&dst)?;
                }
                eprintln!("  copying source -> scratch/{}", perm.label);
                copy_dir(&source_dir, &dst)?;
            }
            eprintln!("populate complete; source + scratch dirs ready");
        }
        "run" => {
            // Single config per container so the kernel page cache is released
            // between configs (the container exit returns its cgroup memory to
            // the host kernel). LSMT_DIO_CONFIG selects which permutation to run.
            let want = std::env::var("LSMT_DIO_CONFIG").unwrap_or_else(|_| "buffered".into());
            let perm = PERMUTATIONS
                .iter()
                .find(|p| p.label == want)
                .ok_or_else(|| format!("unknown config: {want}"))?;
            let r = run_one(&settings, *perm, &source_dir, &scratch_base)?;
            print_results_table(std::slice::from_ref(&r));
            eprintln!("\nJSON: {}", serialize_results(std::slice::from_ref(&r)));
        }
        // "all" or any unrecognized mode: populate once, run every permutation.
        _ => {
            std::fs::create_dir_all(&source_dir)?;
            populate_source(&settings, &source_dir)?;
            for perm in PERMUTATIONS {
                let dst = scratch_base.join(perm.label);
                if dst.exists() {
                    std::fs::remove_dir_all(&dst)?;
                }
                copy_dir(&source_dir, &dst)?;
            }
            let mut results = vec![];
            for perm in PERMUTATIONS {
                let r = run_one(&settings, *perm, &source_dir, &scratch_base)?;
                results.push(r);
            }
            print_results_table(&results);
            eprintln!("\nJSON: {}", serialize_results(&results));
        }
    }

    Ok(())
}

fn serialize_results(results: &[RunResult]) -> String {
    let mut s = String::from("[");
    for (i, r) in results.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "{{\"config\":\"{}\",\"throughput\":{:.1},\"p50_us\":{:.2},\"p99_us\":{:.2},\"p999_us\":{:.2},\"p9999_us\":{:.2},\"read_ops\":{},\"write_ops\":{}}}",
            r.label,
            r.throughput(),
            r.percentile_us(50.0),
            r.percentile_us(99.0),
            r.percentile_us(99.9),
            r.percentile_us(99.99),
            r.read_ops,
            r.write_ops,
        ));
    }
    s.push(']');
    s
}
