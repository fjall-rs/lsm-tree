use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use lsm_tree::prefix::FixedPrefixExtractor;
use lsm_tree::{AbstractTree, Config};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

fn create_tree_with_segments(
    segment_count: usize,
    with_prefix_extractor: bool,
) -> (TempDir, lsm_tree::Tree) {
    let tempdir = tempfile::tempdir().unwrap();

    let mut config = Config::new(&tempdir);
    if with_prefix_extractor {
        config = config.prefix_extractor(Arc::new(FixedPrefixExtractor::new(8)));
    }

    let tree = config.open().unwrap();

    // Create segments with distinct prefixes
    for segment_idx in 0..segment_count {
        let prefix = format!("seg{:04}", segment_idx);

        // Add 100 keys per segment
        for key_idx in 0..100 {
            let key = format!("{}_{:04}", prefix, key_idx);
            tree.insert(key.as_bytes(), vec![0u8; 100], 0);
        }

        // Flush to create a segment
        tree.flush_active_memtable(0).unwrap();
    }

    (tempdir, tree)
}

fn benchmark_range_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query");

    // Test different segment counts
    for segment_count in [10, 100, 500, 1000] {
        // Benchmark without prefix extractor
        group.bench_with_input(
            BenchmarkId::new("no_prefix", segment_count),
            &segment_count,
            |b, &count| {
                let (_tempdir, tree) = create_tree_with_segments(count, false);

                b.iter(|| {
                    // Query for a range that doesn't exist
                    let start: &[u8] = b"zzz_0000";
                    let end: &[u8] = b"zzz_9999";
                    let iter = tree.range(start..=end, 0, None);
                    // Force evaluation by counting
                    let count = iter.count();
                    black_box(count);
                });
            },
        );

        // Benchmark with prefix extractor
        group.bench_with_input(
            BenchmarkId::new("with_prefix", segment_count),
            &segment_count,
            |b, &count| {
                let (_tempdir, tree) = create_tree_with_segments(count, true);

                b.iter(|| {
                    // Query for a range that doesn't exist (will check filters)
                    let start: &[u8] = b"zzz_0000";
                    let end: &[u8] = b"zzz_9999";
                    let iter = tree.range(start..=end, 0, None);
                    // Force evaluation by counting
                    let count = iter.count();
                    black_box(count);
                });
            },
        );

        // Benchmark with prefix extractor - existing prefix
        group.bench_with_input(
            BenchmarkId::new("with_prefix_exists", segment_count),
            &segment_count,
            |b, &count| {
                let (_tempdir, tree) = create_tree_with_segments(count, true);

                b.iter(|| {
                    // Query for a range that exists in the middle
                    let mid = count / 2;
                    let prefix = format!("seg{:04}", mid);
                    let start_str = format!("{}_0000", prefix);
                    let end_str = format!("{}_0099", prefix);
                    let start: &[u8] = start_str.as_bytes();
                    let end: &[u8] = end_str.as_bytes();
                    let iter = tree.range(start..=end, 0, None);
                    // Force evaluation by counting
                    let count = iter.count();
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

fn benchmark_timing_comparison(_c: &mut Criterion) {
    println!("\n=== RunReader Performance Benchmark ===");
    println!("Testing impact of prefix filter checks on large runs\n");

    for segment_count in [100, 500, 1000] {
        println!("\n--- Testing with {} segments ---", segment_count);

        // Test without prefix extractor
        let (_tempdir_no_prefix, tree_no_prefix) = create_tree_with_segments(segment_count, false);

        let start = Instant::now();
        for _ in 0..100 {
            let start_key: &[u8] = b"zzz_0000";
            let end_key: &[u8] = b"zzz_9999";
            let iter = tree_no_prefix.range(start_key..=end_key, 0, None);
            let _ = iter.count();
        }
        let no_prefix_time = start.elapsed();
        let avg_no_prefix = no_prefix_time.as_nanos() / 100;

        println!("  Without prefix extractor: {:>8} ns/query", avg_no_prefix);

        // Test with prefix extractor
        let (_tempdir_with_prefix, tree_with_prefix) =
            create_tree_with_segments(segment_count, true);

        let start = Instant::now();
        for _ in 0..100 {
            let start_key: &[u8] = b"zzz_0000";
            let end_key: &[u8] = b"zzz_9999";
            let iter = tree_with_prefix.range(start_key..=end_key, 0, None);
            let _ = iter.count();
        }
        let with_prefix_time = start.elapsed();
        let avg_with_prefix = with_prefix_time.as_nanos() / 100;

        println!(
            "  With prefix extractor:    {:>8} ns/query",
            avg_with_prefix
        );

        if avg_with_prefix > avg_no_prefix {
            let overhead = avg_with_prefix - avg_no_prefix;
            println!(
                "  Overhead: {} ns ({:.1}%)",
                overhead,
                (overhead as f64 / avg_no_prefix as f64) * 100.0
            );
        } else {
            let savings = avg_no_prefix - avg_with_prefix;
            println!(
                "  Savings: {} ns ({:.1}%)",
                savings,
                (savings as f64 / avg_no_prefix as f64) * 100.0
            );
        }

        // Check CPU cost per segment
        if segment_count > 0 {
            let per_segment_overhead = if avg_with_prefix > avg_no_prefix {
                (avg_with_prefix - avg_no_prefix) / segment_count as u128
            } else {
                0
            };
            println!("  Per-segment overhead: ~{} ns", per_segment_overhead);
        }
    }

    println!("\n=== Summary ===");
    println!("MAX_UPFRONT_CHECKS optimization limits overhead to checking at most 10 segments.");
    println!(
        "For runs with >10 segments, remaining segments are filtered lazily during iteration.\n"
    );
}

fn run_timing_benchmark() {
    println!("\n=== RunReader Performance Benchmark ===");
    println!("Testing impact of prefix filter checks on large runs\n");

    for segment_count in [100, 500, 1000] {
        println!("\n--- Testing with {} segments ---", segment_count);

        // Test without prefix extractor
        let (_tempdir_no_prefix, tree_no_prefix) = create_tree_with_segments(segment_count, false);

        let start = Instant::now();
        for _ in 0..100 {
            let start_key: &[u8] = b"zzz_0000";
            let end_key: &[u8] = b"zzz_9999";
            let iter = tree_no_prefix.range(start_key..=end_key, 0, None);
            let _ = iter.count();
        }
        let no_prefix_time = start.elapsed();
        let avg_no_prefix = no_prefix_time.as_nanos() / 100;

        println!("  Without prefix extractor: {:>8} ns/query", avg_no_prefix);

        // Test with prefix extractor
        let (_tempdir_with_prefix, tree_with_prefix) =
            create_tree_with_segments(segment_count, true);

        let start = Instant::now();
        for _ in 0..100 {
            let start_key: &[u8] = b"zzz_0000";
            let end_key: &[u8] = b"zzz_9999";
            let iter = tree_with_prefix.range(start_key..=end_key, 0, None);
            let _ = iter.count();
        }
        let with_prefix_time = start.elapsed();
        let avg_with_prefix = with_prefix_time.as_nanos() / 100;

        println!(
            "  With prefix extractor:    {:>8} ns/query",
            avg_with_prefix
        );

        if avg_with_prefix > avg_no_prefix {
            let overhead = avg_with_prefix - avg_no_prefix;
            println!(
                "  Overhead: {} ns ({:.1}%)",
                overhead,
                (overhead as f64 / avg_no_prefix as f64) * 100.0
            );
        } else {
            let savings = avg_no_prefix - avg_with_prefix;
            println!(
                "  Savings: {} ns ({:.1}%)",
                savings,
                (savings as f64 / avg_no_prefix as f64) * 100.0
            );
        }

        // Check CPU cost per segment
        if segment_count > 0 {
            let per_segment_overhead = if avg_with_prefix > avg_no_prefix {
                (avg_with_prefix - avg_no_prefix) / segment_count as u128
            } else {
                0
            };
            println!("  Per-segment overhead: ~{} ns", per_segment_overhead);
        }
    }

    println!("\n=== Summary ===");
    println!("MAX_UPFRONT_CHECKS optimization limits overhead to checking at most 10 segments.");
    println!(
        "For runs with >10 segments, remaining segments are filtered lazily during iteration.\n"
    );
}

fn benchmark_all(c: &mut Criterion) {
    // Run standard benchmarks
    benchmark_range_query(c);

    // Run the detailed timing comparison
    run_timing_benchmark();
}

criterion_group!(benches, benchmark_range_query);
criterion_main!(benches);
