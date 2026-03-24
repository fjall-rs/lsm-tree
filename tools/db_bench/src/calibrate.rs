//! Runner calibration — measures raw hardware capabilities so benchmark
//! results can be normalized across different CI runners.
//!
//! The calibration workload runs ~5 seconds of IO + CPU microbenchmarks and
//! produces a composite score.  Actual benchmark ops/sec are divided by the
//! composite and multiplied by a fixed reference constant, yielding results
//! that are comparable across hardware.

use std::io::{Read, Seek, SeekFrom, Write};
use std::time::Instant;

/// Fixed reference composite.  All normalized results are expressed relative
/// to this constant so they stay in a human-readable ops/sec range.
///
/// Calibrated so that `factor ≈ 1.0` on a typical `ubuntu-latest` GitHub
/// runner (~23K composite), keeping normalized values in the same ballpark
/// as pre-normalization CI results.  Derived by comparing local raw ops/sec
/// against historical CI results and scaling by the local composite.
///
/// Changing this constant rescales *all* historical results (reset the
/// dashboard).
pub const REFERENCE_COMPOSITE: f64 = 23_000.0;

/// Raw calibration measurements for the current runner.
#[derive(Debug, Clone)]
pub struct CalibrationScore {
    /// Sequential 4 KiB write IOPS.
    pub seq_write_iops: f64,
    /// Random 4 KiB read IOPS.
    pub rand_read_iops: f64,
    /// CPU throughput (CRC32 MB/s over in-memory buffer).
    pub cpu_score: f64,
    /// Weighted geometric mean of the three sub-scores.
    pub composite: f64,
}

impl CalibrationScore {
    /// Normalization factor: `raw_ops * factor = normalized_ops`.
    pub fn factor(&self) -> f64 {
        if self.composite > 0.0 {
            REFERENCE_COMPOSITE / self.composite
        } else {
            1.0
        }
    }
}

impl std::fmt::Display for CalibrationScore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "seq_wr={:.0} rand_rd={:.0} cpu={:.0} composite={:.1}",
            self.seq_write_iops, self.rand_read_iops, self.cpu_score, self.composite,
        )
    }
}

/// Run the calibration workload (~5 s) and return the score.
///
/// Uses a temporary file for IO tests — no external dependencies beyond std.
///
/// IO tests intentionally run on the system temp filesystem (not `--db` path)
/// because calibration measures runner-level capability for cross-runner
/// normalization, not the performance of a specific mount point.
///
/// Random reads hit page cache (the file was just written), which matches
/// how LSM-tree benchmarks behave — block cache and OS page cache are the
/// hot path.  The goal is a stable, reproducible score per runner, not
/// absolute storage latency.
pub fn run_calibration() -> std::io::Result<CalibrationScore> {
    eprintln!("=== calibration ===");

    let seq_write_iops = calibrate_seq_write()?;
    let rand_read_iops = calibrate_rand_read()?;
    let cpu_score = calibrate_cpu();

    // Weighted geometric mean.  Weights reflect LSM-tree workload profile:
    // random IO dominates (0.4), sequential IO matters (0.3), CPU is
    // secondary (0.3).
    let composite = weighted_geometric_mean(&[
        (seq_write_iops, 0.3),
        (rand_read_iops, 0.4),
        (cpu_score, 0.3),
    ]);

    let score = CalibrationScore {
        seq_write_iops,
        rand_read_iops,
        cpu_score,
        composite,
    };

    eprintln!("calibration: {score}");

    Ok(score)
}

/// Sequential write: 64 MiB in 4 KiB blocks → IOPS.
fn calibrate_seq_write() -> std::io::Result<f64> {
    const BLOCK: usize = 4096;
    const TOTAL: usize = 64 * 1024 * 1024; // 64 MiB
    let blocks = TOTAL / BLOCK;
    let buf = vec![0xABu8; BLOCK];

    let mut file = tempfile::tempfile()?;

    let start = Instant::now();
    for _ in 0..blocks {
        file.write_all(&buf)?;
    }
    file.sync_all()?;
    let elapsed = start.elapsed().as_secs_f64();

    let iops = blocks as f64 / elapsed;
    eprintln!("  seq_write: {blocks} x 4K in {elapsed:.3}s = {iops:.0} IOPS");
    Ok(iops)
}

/// Random read: 10 000 random 4 KiB reads from a 64 MiB file → IOPS.
fn calibrate_rand_read() -> std::io::Result<f64> {
    const BLOCK: usize = 4096;
    const FILE_SIZE: u64 = 64 * 1024 * 1024;
    const NUM_READS: usize = 10_000;

    // Write a file to read from.
    let mut file = tempfile::tempfile()?;
    let buf = vec![0xCDu8; BLOCK];
    let blocks = FILE_SIZE as usize / BLOCK;
    for _ in 0..blocks {
        file.write_all(&buf)?;
    }
    file.sync_all()?;

    let max_offset = FILE_SIZE - BLOCK as u64;
    // Simple LCG for deterministic offsets — no need for rand crate here.
    let mut lcg: u64 = 0xDEAD_BEEF;
    let mut read_buf = vec![0u8; BLOCK];

    let start = Instant::now();
    for _ in 0..NUM_READS {
        lcg = lcg.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let offset = (lcg >> 16) % max_offset;
        // Align to block boundary for realistic IO.
        let aligned = offset & !(BLOCK as u64 - 1);
        file.seek(SeekFrom::Start(aligned))?;
        file.read_exact(&mut read_buf)?;
    }
    let elapsed = start.elapsed().as_secs_f64();

    let iops = NUM_READS as f64 / elapsed;
    eprintln!("  rand_read: {NUM_READS} x 4K in {elapsed:.3}s = {iops:.0} IOPS");
    Ok(iops)
}

/// CPU: CRC32 over 64 MiB in-memory buffer → MB/s.
///
/// 64 MiB is large enough to exceed L3 cache on most runners, keeping the
/// measurement meaningful, while completing in <1s even on slow CI hardware.
fn calibrate_cpu() -> f64 {
    const SIZE: usize = 64 * 1024 * 1024;
    // Allocate and fill to ensure pages are faulted in.
    let buf = vec![0x55u8; SIZE];

    let start = Instant::now();
    let checksum = crc32_slice(&buf);
    std::hint::black_box(checksum);
    let elapsed = start.elapsed().as_secs_f64();

    let mb_per_sec = (SIZE as f64 / (1024.0 * 1024.0)) / elapsed;
    eprintln!(
        "  cpu (crc32): {} MiB in {elapsed:.3}s = {mb_per_sec:.0} MB/s",
        SIZE / (1024 * 1024)
    );
    mb_per_sec
}

/// Simple CRC32 (IEEE) — no external crate needed, just a tight compute loop.
/// Not optimized with lookup tables: the point is to measure raw CPU speed
/// with a deterministic workload, not to be the fastest CRC32.
fn crc32_slice(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Weighted geometric mean: exp(Σ wᵢ·ln(xᵢ) / Σ wᵢ).
fn weighted_geometric_mean(values: &[(f64, f64)]) -> f64 {
    let (sum_wln, sum_w) = values
        .iter()
        .filter(|(x, _)| *x > 0.0)
        .fold((0.0, 0.0), |(s, w), (x, weight)| {
            (s + weight * x.ln(), w + weight)
        });

    if sum_w > 0.0 {
        (sum_wln / sum_w).exp()
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_known_value() {
        // CRC32 of "123456789" = 0xCBF43926
        let data = b"123456789";
        assert_eq!(crc32_slice(data), 0xCBF4_3926);
    }

    #[test]
    fn weighted_geometric_mean_equal_weights() {
        // geo_mean(100, 100, 100) = 100
        let vals = [(100.0, 1.0), (100.0, 1.0), (100.0, 1.0)];
        let m = weighted_geometric_mean(&vals);
        assert!((m - 100.0).abs() < 0.01, "expected 100, got {m}");
    }

    #[test]
    fn weighted_geometric_mean_unequal() {
        // geo_mean(10, 1000) with equal weights = sqrt(10*1000) = 100
        let vals = [(10.0, 0.5), (1000.0, 0.5)];
        let m = weighted_geometric_mean(&vals);
        assert!((m - 100.0).abs() < 0.1, "expected 100, got {m}");
    }

    #[test]
    fn factor_with_reference() {
        let score = CalibrationScore {
            seq_write_iops: 100_000.0,
            rand_read_iops: 50_000.0,
            cpu_score: 3000.0,
            composite: 11_500.0,
        };
        // factor = REFERENCE / composite = 23000 / 11500 = 2.0
        assert!((score.factor() - 2.0).abs() < 0.001);
    }

    #[test]
    fn factor_zero_composite_returns_one() {
        let score = CalibrationScore {
            seq_write_iops: 0.0,
            rand_read_iops: 0.0,
            cpu_score: 0.0,
            composite: 0.0,
        };
        assert!((score.factor() - 1.0).abs() < 0.001);
    }

    #[test]
    fn display_format() {
        let score = CalibrationScore {
            seq_write_iops: 150_000.0,
            rand_read_iops: 48_000.0,
            cpu_score: 3200.0,
            composite: 42_123.456,
        };
        let s = format!("{score}");
        assert!(s.contains("seq_wr=150000"));
        assert!(s.contains("rand_rd=48000"));
        assert!(s.contains("cpu=3200"));
        assert!(s.contains("composite=42123.5"));
    }
}
