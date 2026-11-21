use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lsm_tree::{
    config::{BlockSizePolicy, CompressionPolicy, HashRatioPolicy, RestartIntervalPolicy},
    AbstractTree, AnyTree, Cache, Config, SeqNo, SequenceNumberCounter,
};
use std::{hint::black_box, path::Path, sync::Arc, time::Duration};

const MIB: usize = 1024 * 1024;
const VALUE_LEN: usize = 1024;

fn set_key(key: &mut [u8], id: usize) {
    // Groups of 256 keys share the long middle prefix; lexicographic order is
    // identical to ID order. Only these 16 bytes change during a lookup.
    key[..8].copy_from_slice(&((id / 256) as u64).to_be_bytes());
    let end = key.len();
    key[end - 8..].copy_from_slice(&(id as u64).to_be_bytes());
}

fn open(path: &Path, cache_bytes: usize) -> AnyTree {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::new(Cache::with_capacity_bytes(cache_bytes as u64)))
    .data_block_size_policy(BlockSizePolicy::all(4096))
    .data_block_restart_interval_policy(RestartIntervalPolicy::all(16))
    .data_block_hash_ratio_policy(HashRatioPolicy::all(0.0))
    .data_block_compression_policy(CompressionPolicy::disabled())
    .open()
    .unwrap()
}

fn scaled_reads(c: &mut Criterion) {
    let root = std::env::var_os("LSM_SCALE_DATA")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| ".bench/point-read-scale".into());
    std::fs::create_dir_all(&root).unwrap();
    let mut group = c.benchmark_group("point_read_scale");
    group.sample_size(100);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    for payload_mib in [4, 64, 1024] {
        let count = payload_mib * MIB / VALUE_LEN;
        assert!(count.is_power_of_two());
        for key_len in [128, 512] {
            let path = root.join(format!("payload_{payload_mib}MiB_key_{key_len}"));
            let ready = path.join("BENCH_READY");
            if !ready.exists() {
                assert!(
                    !path.exists(),
                    "Incomplete dataset at {path:?}; inspect it before retrying"
                );
                let tree = open(&path, 64 * MIB);
                let mut ingest = tree.ingestion().unwrap();
                let value = vec![0x5a; VALUE_LEN];
                let mut key = vec![b'x'; key_len];
                eprintln!("Building {path:?}: {count} records");
                for id in 0..count {
                    set_key(&mut key, id);
                    ingest.write(key.as_slice(), value.as_slice()).unwrap();
                }
                ingest.finish().unwrap();
                std::fs::write(&ready, format!("{count}\n")).unwrap();
            }

            for (mode, cache_bytes) in [("resident", 2048 * MIB), ("cache64MiB", 64 * MIB)] {
                // Reopen for each cache configuration. Data files are reused by
                // both implementations and never rewritten in the timed loop.
                let tree = open(&path, cache_bytes);
                assert_eq!(tree.approximate_len(), count);
                assert_eq!(tree.sealed_memtable_count(), 0);
                eprintln!("DATASET payload_MiB={payload_mib} key_bytes={key_len} records={count} logical_bytes={} disk_bytes={} tables={} cache_bytes={cache_bytes} mode={mode}",
                    count * (key_len + VALUE_LEN), tree.disk_space(), tree.table_count());
                let mut key = vec![b'x'; key_len];
                // Verify and warm every record. The bounded cache intentionally
                // cannot retain the largest dataset. OS pages are warm; this is
                // not a physical-device cold-read benchmark.
                for id in 0..count {
                    set_key(&mut key, id);
                    let value = tree.get(key.as_slice(), SeqNo::MAX).unwrap().unwrap();
                    assert_eq!(value.len(), VALUE_LEN);
                    assert!(value.iter().all(|byte| *byte == 0x5a));
                }
                group.bench_function(
                    BenchmarkId::new(format!("{mode}/key{key_len}"), format!("{payload_mib}MiB")),
                    |b| {
                        let mut cursor = 0_u64;
                        b.iter(|| {
                            // An odd multiplier permutes the entire power-of-two ID
                            // space. No query allocation or random generator calls.
                            let id = cursor
                                .wrapping_mul(6364136223846793005)
                                .wrapping_add(1442695040888963407)
                                as usize
                                & (count - 1);
                            cursor = cursor.wrapping_add(1);
                            set_key(&mut key, id);
                            black_box(
                                black_box(&tree)
                                    .get(black_box(key.as_slice()), black_box(SeqNo::MAX))
                                    .unwrap()
                                    .unwrap(),
                            );
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, scaled_reads);
criterion_main!(benches);
