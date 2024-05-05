use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{
    bloom::BloomFilter, segment::block::ValueBlock, BlockCache, Config, MemTable, Value,
};
use nanoid::nanoid;
use std::{io::Write, sync::Arc};

fn memtable_get_upper_bound(c: &mut Criterion) {
    let memtable = MemTable::default();

    for _ in 0..1_000_000 {
        memtable.insert(Value {
            key: format!("abc_{}", nanoid!()).as_bytes().into(),
            value: vec![].into(),
            seqno: 0,
            value_type: lsm_tree::ValueType::Value,
        });
    }

    c.bench_function("memtable get", |b| {
        b.iter(|| {
            memtable.get("abc", None);
        });
    });
}

fn value_block_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("ValueBlock::size");

    for item_count in [10, 100, 1_000] {
        group.bench_function(format!("{item_count} items"), |b| {
            let items = (0..item_count)
                .map(|_| {
                    Value::new(
                        "a".repeat(16).as_bytes(),
                        "a".repeat(100).as_bytes(),
                        63,
                        lsm_tree::ValueType::Tombstone,
                    )
                })
                .collect();

            let block = ValueBlock { items, crc: 0 };

            b.iter(|| {
                block.size();
            })
        });
    }
}

fn value_block_size_find(c: &mut Criterion) {
    use lsm_tree::segment::{
        block_index::block_handle::BlockHandle, block_index::BlockHandleBlock,
    };

    let mut group = c.benchmark_group("Find item in BlockHandleBlock");

    for item_count in [10, 100, 1_000] {
        group.bench_function(format!("{item_count} items"), |b| {
            let items = (0u64..item_count)
                .map(|x| BlockHandle {
                    start_key: x.to_be_bytes().into(),
                    offset: 56,
                    size: 635,
                })
                .collect();

            let block = BlockHandleBlock { items, crc: 0 };
            let key = &(item_count / 2).to_be_bytes();

            b.iter(|| block.get_lower_bound_block_info(key))
        });
    }
}

fn load_block_from_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("Load block from disk");

    for block_size in [1, 4, 8, 16, 32, 64] {
        group.bench_function(format!("{block_size} KiB"), |b| {
            let block_size = block_size * 1_024;

            let mut size = 0;

            let mut items = vec![];

            for x in 0u64.. {
                let value = Value::new(
                    x.to_be_bytes(),
                    x.to_string().repeat(100).as_bytes(),
                    63,
                    lsm_tree::ValueType::Tombstone,
                );

                size += value.size();

                items.push(value);

                if size >= block_size {
                    break;
                }
            }

            let mut block = ValueBlock {
                items: items.into_boxed_slice(),
                crc: 0,
            };

            // Serialize block
            block.crc = ValueBlock::create_crc(&block.items).unwrap();
            let bytes = ValueBlock::to_bytes_compressed(&block);
            let block_size_on_disk = bytes.len();

            let mut file = tempfile::tempfile().unwrap();
            file.write_all(&bytes).unwrap();

            b.iter(|| {
                let loaded_block =
                    ValueBlock::from_file_compressed(&mut file, 0, block_size_on_disk as u32)
                        .unwrap();

                assert_eq!(loaded_block.items.len(), block.items.len());
                assert_eq!(loaded_block.crc, block.crc);
            });
        });
    }
}

fn file_descriptor(c: &mut Criterion) {
    use std::fs::File;

    let file = tempfile::NamedTempFile::new().unwrap();

    let mut group = c.benchmark_group("Get file descriptor");

    group.bench_function("fopen", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            File::open(file.path()).unwrap();
        });
    });

    let id = (0, 523).into();
    let descriptor_table = lsm_tree::descriptor_table::FileDescriptorTable::new(1, 1);
    descriptor_table.insert(file.path(), id);

    group.bench_function("descriptor table", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            let guard = descriptor_table.access(&id).unwrap().unwrap();
            let _fd = guard.file.lock().unwrap();
        });
    });
}

fn bloom_filter_construction(c: &mut Criterion) {
    let mut filter = BloomFilter::with_fp_rate(1_000_000, 0.001);

    c.bench_function("bloom filter add key", |b| {
        b.iter(|| {
            let key = nanoid::nanoid!();
            filter.set_with_hash(BloomFilter::get_hash(key.as_bytes()));
        });
    });
}

fn bloom_filter_contains(c: &mut Criterion) {
    let mut filter = BloomFilter::with_fp_rate(10, 0.0001);

    for key in [
        b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7", b"item8",
        b"item9",
    ] {
        filter.set_with_hash(BloomFilter::get_hash(key));

        assert!(!filter.contains(nanoid::nanoid!().as_bytes()));
    }

    c.bench_function("bloom filter contains key, true positive", |b| {
        b.iter(|| filter.contains(b"item4"));
    });

    c.bench_function("bloom filter contains key, true negative", |b| {
        b.iter(|| filter.contains(b"sdfafdas"));
    });
}

// TODO: benchmark .prefix().next() and .next_back(), disjoint and non-disjoint

fn tree_get_pairs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Get pairs");
    group.sample_size(10);

    for segment_count in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] {
        {
            let folder = tempfile::tempdir().unwrap();
            let tree = Config::new(folder)
                .block_size(1_024)
                .block_cache(Arc::new(BlockCache::with_capacity_bytes(0)))
                .open()
                .unwrap();

            let mut x = 0_u64;

            for _ in 0..segment_count {
                for _ in 0..10 {
                    let key = x.to_be_bytes();
                    x += 1;
                    tree.insert(key, key, 0);
                }
                tree.flush_active_memtable().unwrap();
            }

            group.bench_function(
                &format!("Tree::first_key_value (disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        assert!(tree.first_key_value().unwrap().is_some());
                    });
                },
            );

            group.bench_function(
                &format!("Tree::last_key_value (disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        assert!(tree.last_key_value().unwrap().is_some());
                    });
                },
            );
        }

        {
            let folder = tempfile::tempdir().unwrap();
            let tree = Config::new(folder)
                .block_size(1_024)
                .block_cache(Arc::new(BlockCache::with_capacity_bytes(0)))
                .open()
                .unwrap();

            let mut x = 0_u64;

            for _ in 0..segment_count {
                for _ in 0..10 {
                    let key = x.to_be_bytes();
                    x += 1;
                    tree.insert(key, key, 0);
                }
                tree.insert("a", vec![], 0);
                tree.insert(u64::MAX.to_be_bytes(), vec![], 0);
                tree.flush_active_memtable().unwrap();
            }

            group.bench_function(
                &format!("Tree::first_key_value (non-disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        assert!(tree.first_key_value().unwrap().is_some());
                    });
                },
            );

            group.bench_function(
                &format!("Tree::last_key_value (non-disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        assert!(tree.last_key_value().unwrap().is_some());
                    });
                },
            );
        }
    }
}

// TODO: benchmark point read disjoint vs non-disjoint level

criterion_group!(
    benches,
    memtable_get_upper_bound,
    value_block_size_find,
    value_block_size,
    load_block_from_disk,
    file_descriptor,
    bloom_filter_construction,
    bloom_filter_contains,
    tree_get_pairs,
);
criterion_main!(benches);
