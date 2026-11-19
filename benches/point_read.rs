use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lsm_tree::{
    config::{BlockSizePolicy, CompressionPolicy, HashRatioPolicy, RestartIntervalPolicy},
    table::{block::BlockType, Block, DataBlock},
    AbstractTree, Cache, CompressionType, Config, InternalValue, SeqNo, SequenceNumberCounter,
    ValueType,
};
use std::{hint::black_box, sync::Arc};

const ITEM_COUNT: usize = 256;
const RESTART_INTERVAL: usize = 16;
const VALUE: &[u8] = b"a small value";

fn key(key_len: usize, id: usize) -> Vec<u8> {
    let mut key = vec![b'x'; key_len - 8];
    key.extend_from_slice(&(id as u64).to_be_bytes());
    key
}

fn items(key_len: usize) -> Vec<InternalValue> {
    (0..ITEM_COUNT)
        // Even IDs exist; odd IDs are in-range misses.
        .map(|i| InternalValue::from_components(key(key_len, i * 2), VALUE, 0, ValueType::Value))
        .collect()
}

fn block_point_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_read/block");
    group.throughput(Throughput::Elements(1));

    for key_len in [16, 128, 512] {
        let items = items(key_len);
        let encoded = DataBlock::encode_into_vec(&items, RESTART_INTERVAL as u8, 0.0).unwrap();
        let mut serialized = Vec::new();
        Block::write_into(
            &mut serialized,
            &encoded,
            BlockType::Data,
            CompressionType::None,
        )
        .unwrap();
        let block = DataBlock::new(
            Block::from_reader(&mut serialized.as_slice(), CompressionType::None).unwrap(),
        );

        // Confirm that the intended hits really exercise full vs. truncated keys.
        for (i, item) in block.iter().enumerate() {
            assert_eq!(item.prefix.is_some(), i % RESTART_INTERVAL != 0);
        }

        for case in ["restart_hit", "prefix_hit", "miss"] {
            let queries: Vec<_> = (0..ITEM_COUNT / RESTART_INTERVAL)
                .map(|i| {
                    // Visit restart groups in a fixed non-sequential permutation.
                    let start = ((i * 5) % (ITEM_COUNT / RESTART_INTERVAL)) * RESTART_INTERVAL;
                    let index = start + if case == "restart_hit" { 0 } else { 7 };
                    key(key_len, index * 2 + usize::from(case == "miss"))
                })
                .collect();

            // Assertions and allocation are outside the timed loop.
            for query in &queries {
                let result = block.point_read(query, SeqNo::MAX);
                if case == "miss" {
                    assert!(result.is_none());
                } else {
                    let item = result.unwrap();
                    assert_eq!(item.key.user_key.as_ref(), query.as_slice());
                    assert_eq!(item.value.as_ref(), VALUE);
                }
            }

            group.bench_with_input(BenchmarkId::new(case, key_len), &queries, |b, queries| {
                let mut cursor = 0;
                b.iter(|| {
                    let query = &queries[cursor];
                    cursor = (cursor + 1) % queries.len();
                    // Consume just the value, matching the purpose of a point lookup.
                    // After adding a key-free block API, switch this call to that API.
                    black_box(
                        black_box(&block)
                            .point_read(black_box(query.as_slice()), black_box(SeqNo::MAX))
                            .map(|item| item.value),
                    );
                });
            });
        }
    }
    group.finish();
}

fn cached_tree_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_read/cached_tree");
    group.throughput(Throughput::Elements(1));

    for key_len in [16, 128, 512] {
        let folder = tempfile::tempdir().unwrap();
        let tree = Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .use_cache(Arc::new(Cache::with_capacity_bytes(16 * 1024 * 1024)))
        .data_block_size_policy(BlockSizePolicy::all(4096))
        .data_block_restart_interval_policy(RestartIntervalPolicy::all(RESTART_INTERVAL as u8))
        .data_block_hash_ratio_policy(HashRatioPolicy::all(0.0))
        .data_block_compression_policy(CompressionPolicy::disabled())
        .open()
        .unwrap();

        for item in items(key_len) {
            tree.insert(item.key.user_key, item.value, 0);
        }
        tree.flush_active_memtable(0).unwrap();

        // Fixed permutation; query construction, verification and cache warming
        // happen before timing. The memtable is empty after the flush.
        let queries: Vec<_> = (0..ITEM_COUNT)
            .map(|i| key(key_len, ((i * 157) % ITEM_COUNT) * 2))
            .collect();
        for query in &queries {
            assert_eq!(
                tree.get(query, SeqNo::MAX).unwrap().unwrap().as_ref(),
                VALUE
            );
        }

        group.bench_with_input(BenchmarkId::new("hit", key_len), &queries, |b, queries| {
            let mut cursor = 0;
            b.iter(|| {
                let query = &queries[cursor];
                cursor = (cursor + 1) % queries.len();
                black_box(
                    black_box(&tree)
                        .get(black_box(query.as_slice()), black_box(SeqNo::MAX))
                        .unwrap(),
                );
            });
        });
    }
    group.finish();
}

criterion_group!(benches, block_point_read, cached_tree_get);
criterion_main!(benches);
