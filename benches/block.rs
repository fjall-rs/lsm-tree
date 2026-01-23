use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{
    table::{
        block::{decoder::ParsedItem, BlockType, Header},
        Block, BlockHandle, BlockOffset, DataBlock,
    },
    CompressionType, InternalValue, SeqNo, ValueType,
};
use rand::Rng;
use std::io::{Seek, SeekFrom, Write};

/* fn value_block_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("ValueBlock::size");

    for item_count in [10, 100, 1_000] {
        group.bench_function(format!("{item_count} items"), |b| {
            let items = (0..item_count)
                .map(|_| {
                    InternalValue::from_components(
                        "a".repeat(16).as_bytes(),
                        "a".repeat(100).as_bytes(),
                        63,
                        lsm_tree::ValueType::Value,
                    )
                })
                .collect();

            let block = ValueBlock {
                items,
                header: BlockHeader {
                    compression: CompressionType::Lz4,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    previous_block_offset: 0,
                    uncompressed_length: 0,
                },
            };

            b.iter(|| {
                (&*block.items).size();
            })
        });
    }
} */

fn value_block_find(c: &mut Criterion) {
    let mut group = c.benchmark_group("DataBlock::point_read");

    for item_count in [10, 100, 1_000, 10_000] {
        let mut items = vec![];

        for item in 0u64..item_count {
            items.push(InternalValue::from_components(
                item.to_be_bytes(),
                b"",
                0,
                ValueType::Value,
            ));
        }

        let data = DataBlock::encode_into_vec(&items, 16, 0.0).unwrap();
        let data_len = data.len();
        let block = DataBlock::new(Block {
            header: Header {
                block_type: BlockType::Data,
                checksum: lsm_tree::Checksum::from_raw(0),
                data_length: data_len as u32,
                uncompressed_length: data_len as u32,
            },
            data: data.into(),
        });

        let mut rng = rand::rng();

        group.bench_function(format!("{item_count} items (linear scan)"), |b| {
            b.iter(|| {
                let needle = rng.random_range(0..item_count).to_be_bytes();

                let item = block
                    .iter()
                    .find(|item| {
                        item.compare_key(&needle, block.as_slice()) == std::cmp::Ordering::Equal
                    })
                    .unwrap()
                    .materialize(block.as_slice());

                assert_eq!(item.key.user_key, needle);
            })
        });

        group.bench_function(format!("{item_count} items (binary search)"), |b| {
            b.iter(|| {
                let needle = rng.random_range(0..item_count).to_be_bytes();

                let item = block.point_read(&needle, SeqNo::MAX).unwrap();
                assert_eq!(item.key.user_key, needle);
            })
        });
    }
}

fn encode_block(c: &mut Criterion) {
    let mut group = c.benchmark_group("Encode block");

    for comp_type in [CompressionType::None, CompressionType::Lz4] {
        for block_size in [4, 8, 16, 32, 64, 128] {
            let block_size = block_size * 1_024;

            let mut size = 0;

            let mut items = vec![];

            for x in 0u64.. {
                let value = InternalValue::from_components(
                    x.to_be_bytes(),
                    x.to_string().repeat(50).as_bytes(),
                    63,
                    ValueType::Value,
                );

                size += value.key.user_key.len() + value.value.len();

                items.push(value);

                if size >= block_size {
                    break;
                }
            }

            let data = DataBlock::encode_into_vec(&items, 16, 0.0).unwrap();

            group.bench_function(format!("{block_size} KiB [{comp_type}]"), |b| {
                b.iter(|| {
                    let mut buf = Vec::new();
                    let _header = Block::write_into(
                        &mut buf,
                        &data,
                        BlockType::Data,
                        comp_type,
                    )
                    .unwrap();
                });
            });
        }
    }
}

fn load_value_block_from_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("Load block from disk");

    for comp_type in [CompressionType::None, CompressionType::Lz4] {
        for block_size in [4, 8, 16, 32, 64, 128] {
            let block_size = block_size * 1_024;

            let mut size = 0;

            let mut items = vec![];

            for x in 0u64.. {
                let value = InternalValue::from_components(
                    x.to_be_bytes(),
                    x.to_string().repeat(50).as_bytes(),
                    63,
                    ValueType::Value,
                );

                size += value.key.user_key.len() + value.value.len();

                items.push(value);

                if size >= block_size {
                    break;
                }
            }

            let data = DataBlock::encode_into_vec(&items, 16, 0.0).unwrap();
            let mut buf = Vec::new();
            let header = Block::write_into(&mut buf, &data, BlockType::Data, comp_type).unwrap();

            let mut file = tempfile::tempfile().unwrap();
            file.write_all(&buf).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();

            let handle = BlockHandle::new(BlockOffset(0), buf.len() as u32);
            let expected_block = DataBlock::new(Block {
                header,
                data: data.into(),
            });

            group.bench_function(format!("{block_size} KiB [{comp_type}]"), |b| {
                b.iter(|| {
                    let loaded_block =
                        DataBlock::new(Block::from_file(&file, handle, comp_type).unwrap());

                    assert_eq!(loaded_block.len(), expected_block.len());
                    assert_eq!(
                        loaded_block.inner.header.checksum,
                        expected_block.inner.header.checksum
                    );
                });
            });
        }
    }
}

criterion_group!(
    benches,
    encode_block,
    value_block_find,
    load_value_block_from_disk,
);
criterion_main!(benches);
