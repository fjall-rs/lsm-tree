use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{
    segment::{
        block::header::Header as BlockHeader, meta::CompressionType, value_block::ValueBlock,
    },
    serde::Serializable,
    BlockCache, Config, MemTable, Value,
};
use nanoid::nanoid;
use std::{io::Write, sync::Arc};
use tempfile::tempdir;

fn full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan all");
    group.sample_size(10);

    for item_count in [10_000, 100_000, 1_000_000] {
        group.bench_function(format!("scan all uncached, {item_count} items"), |b| {
            let path = tempdir().unwrap();

            let tree = Config::new(path)
                .block_cache(BlockCache::with_capacity_bytes(0).into())
                .open()
                .unwrap();

            for x in 0_u32..item_count {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value, 0);
            }

            tree.flush_active_memtable().unwrap();

            b.iter(|| {
                assert_eq!(tree.len().unwrap(), item_count as usize);
            })
        });

        group.bench_function(format!("scan all cached, {item_count} items"), |b| {
            let path = tempdir().unwrap();

            let tree = Config::new(path).open().unwrap();

            for x in 0_u32..item_count {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value, 0);
            }

            tree.flush_active_memtable().unwrap();
            assert_eq!(tree.len().unwrap(), item_count as usize);

            b.iter(|| {
                assert_eq!(tree.len().unwrap(), item_count as usize);
            })
        });
    }
}

fn scan_vs_query(c: &mut Criterion) {
    use std::ops::Bound::*;

    let mut group = c.benchmark_group("scan vs query");

    for size in [100_000, 1_000_000] {
        let path = tempdir().unwrap();

        let tree = Config::new(path)
            .block_cache(BlockCache::with_capacity_bytes(0).into())
            .open()
            .unwrap();

        for x in 0..size as u64 {
            let key = x.to_be_bytes().to_vec();
            let value = nanoid::nanoid!().as_bytes().to_vec();
            tree.insert(key, value, 0);
        }

        tree.flush_active_memtable().unwrap();
        assert_eq!(tree.len().unwrap(), size);

        group.sample_size(10);
        group.bench_function(format!("scan {} (uncached)", size), |b| {
            b.iter(|| {
                let iter = tree.iter();
                let iter = iter.into_iter();
                let count = iter
                    .filter(|x| match x {
                        Ok((key, _)) => {
                            let buf = &key[..8];
                            let (int_bytes, _rest) = buf.split_at(std::mem::size_of::<u64>());
                            let num = u64::from_be_bytes(int_bytes.try_into().unwrap());
                            (60000..61000).contains(&num)
                        }
                        Err(_) => false,
                    })
                    .count();
                assert_eq!(count, 1000);
            })
        });
        group.bench_function(format!("query {} (uncached)", size), |b| {
            b.iter(|| {
                let iter = tree.range((
                    Included(60000_u64.to_be_bytes().to_vec()),
                    Excluded(61000_u64.to_be_bytes().to_vec()),
                ));
                let iter = iter.into_iter();
                assert_eq!(iter.count(), 1000);
            })
        });
        group.bench_function(format!("query rev {}", size), |b| {
            b.iter(|| {
                let iter = tree.range((
                    Included(60000_u64.to_be_bytes().to_vec()),
                    Excluded(61000_u64.to_be_bytes().to_vec()),
                ));
                let iter = iter.into_iter();
                assert_eq!(iter.rev().count(), 1000);
            })
        });
    }
}

fn scan_vs_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan vs prefix");

    for size in [10_000, 100_000, 1_000_000] {
        let path = tempdir().unwrap();

        let tree = Config::new(path)
            .block_cache(BlockCache::with_capacity_bytes(0).into())
            .open()
            .unwrap();

        for _ in 0..size {
            let key = nanoid::nanoid!();
            let value = nanoid::nanoid!();
            tree.insert(key, value, 0);
        }

        let prefix = "hello$$$";

        for _ in 0..1000_u64 {
            let key = format!("{}:{}", prefix, nanoid::nanoid!());
            let value = nanoid::nanoid!();
            tree.insert(key, value, 0);
        }

        tree.flush_active_memtable().unwrap();
        assert_eq!(tree.len().unwrap() as u64, size + 1000);

        group.sample_size(10);
        group.bench_function(format!("scan {} (uncached)", size), |b| {
            b.iter(|| {
                let iter = tree.iter();
                let iter = iter.into_iter().filter(|x| match x {
                    Ok((key, _)) => key.starts_with(prefix.as_bytes()),
                    Err(_) => false,
                });
                assert_eq!(iter.count(), 1000);
            });
        });
        group.bench_function(format!("prefix {} (uncached)", size), |b| {
            b.iter(|| {
                let iter = tree.prefix(prefix);
                let iter = iter.into_iter();
                assert_eq!(iter.count(), 1000);
            });
        });
        group.bench_function(format!("prefix rev {} (uncached)", size), |b| {
            b.iter(|| {
                let iter = tree.prefix(prefix);
                let iter = iter.into_iter();
                assert_eq!(iter.rev().count(), 1000);
            });
        });
    }
}

fn memtable_get_upper_bound(c: &mut Criterion) {
    c.bench_function("memtable get", |b| {
        let memtable = MemTable::default();

        for _ in 0..1_000_000 {
            memtable.insert(Value {
                key: format!("abc_{}", nanoid!()).as_bytes().into(),
                value: vec![].into(),
                seqno: 0,
                value_type: lsm_tree::ValueType::Value,
            });
        }

        b.iter(|| {
            memtable.get("abc", None);
        });
    });
}

fn tli_find_item(c: &mut Criterion) {
    use lsm_tree::segment::block_index::{
        block_handle::KeyedBlockHandle, top_level::TopLevelIndex,
    };

    let mut group = c.benchmark_group("TLI find item");

    for item_count in [10u64, 100, 1_000, 1_000_000] {
        let items = {
            let mut items = Vec::with_capacity(item_count as usize);

            for x in 0..item_count {
                items.push(KeyedBlockHandle {
                    end_key: x.to_be_bytes().into(),
                    offset: x,
                });
            }

            items
        };

        let index = TopLevelIndex::from_boxed_slice(items.into());

        group.bench_function(
            format!("TLI get_next_block_handle ({item_count} items)"),
            |b| {
                let key = (item_count / 10 * 6).to_be_bytes();
                let expected: Arc<[u8]> = (item_count / 10 * 6 + 1).to_be_bytes().into();

                let block = index.get_lowest_block_containing_key(&key).unwrap();

                b.iter(|| {
                    assert_eq!(
                        expected,
                        index.get_next_block_handle(block.offset).unwrap().end_key
                    );
                })
            },
        );

        group.bench_function(
            format!("TLI get_block_containing_item ({item_count} items)"),
            |b| {
                let key = (item_count / 10 * 6).to_be_bytes();

                b.iter(|| {
                    assert_eq!(
                        key,
                        &*index.get_lowest_block_containing_key(&key).unwrap().end_key
                    );
                })
            },
        );
    }
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

            let block = ValueBlock {
                items,
                header: BlockHeader {
                    compression: CompressionType::Lz4,
                    crc: 0,
                    data_length: 0,
                    previous_block_offset: 0,
                },
            };

            b.iter(|| {
                block.size();
            })
        });
    }
}

fn value_block_size_find(c: &mut Criterion) {
    use lsm_tree::segment::block_index::{block_handle::KeyedBlockHandle, IndexBlock};

    let mut group = c.benchmark_group("Find item in IndexBlock");

    // NOTE: Anything above 1000 is unlikely
    for item_count in [10, 100, 500, 1_000, 5_000] {
        group.bench_function(format!("{item_count} items"), |b| {
            let items = (0u64..item_count)
                .map(|x| KeyedBlockHandle {
                    end_key: x.to_be_bytes().into(),
                    offset: 56,
                })
                .collect();

            let block = IndexBlock {
                items,
                header: BlockHeader {
                    compression: CompressionType::Lz4,
                    crc: 0,
                    data_length: 0,
                    previous_block_offset: 0,
                },
            };
            let key = &(item_count / 2).to_be_bytes();

            b.iter(|| block.get_lowest_data_block_handle_containing_item(key))
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
                items: items.clone().into_boxed_slice(),
                header: BlockHeader {
                    compression: CompressionType::Lz4,
                    crc: 0,
                    data_length: 0,
                    previous_block_offset: 0,
                },
            };

            // Serialize block
            block.header.crc = ValueBlock::create_crc(&block.items).unwrap();
            let (header, data) = ValueBlock::to_bytes_compressed(&items, 0).unwrap();

            let mut file = tempfile::tempfile().unwrap();
            header.serialize(&mut file).unwrap();
            file.write_all(&data).unwrap();

            b.iter(|| {
                let loaded_block = ValueBlock::from_file_compressed(&mut file, 0).unwrap();

                assert_eq!(loaded_block.items.len(), block.items.len());
                assert_eq!(loaded_block.header.crc, block.header.crc);
            });
        });
    }
}

fn file_descriptor_table(c: &mut Criterion) {
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

fn disk_point_read(c: &mut Criterion) {
    let folder = tempdir().unwrap();

    let tree = Config::new(folder)
        .block_size(1_024)
        .block_cache(Arc::new(BlockCache::with_capacity_bytes(0)))
        .open()
        .unwrap();

    for seqno in 0..5 {
        tree.insert("a", "b", seqno);
    }
    tree.flush_active_memtable().unwrap();

    for seqno in 5..10 {
        tree.insert("a", "b", seqno);
    }
    tree.flush_active_memtable().unwrap();

    c.bench_function("point read latest (uncached)", |b| {
        let tree = tree.clone();

        b.iter(|| {
            tree.get("a").unwrap().unwrap();
        });
    });

    c.bench_function("point read w/ seqno latest (uncached)", |b| {
        let snapshot = tree.snapshot(5);

        b.iter(|| {
            snapshot.get("a").unwrap().unwrap();
        });
    });
}

// TODO: benchmark point read disjoint vs non-disjoint level vs disjoint *tree*
// TODO: benchmark .prefix().next() and .next_back(), disjoint and non-disjoint

criterion_group!(
    benches,
    disk_point_read,
    full_scan,
    scan_vs_query,
    scan_vs_prefix,
    tli_find_item,
    memtable_get_upper_bound,
    value_block_size_find,
    value_block_size,
    load_block_from_disk,
    file_descriptor_table,
    tree_get_pairs,
);
criterion_main!(benches);
