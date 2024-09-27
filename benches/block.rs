use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{
    segment::{
        block::{header::Header as BlockHeader, ItemSize},
        meta::CompressionType,
        value_block::ValueBlock,
    },
    serde::Serializable,
    Checksum, InternalValue,
};
use std::io::Write;

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
    let mut group = c.benchmark_group("ValueBlock::find_latest");

    for item_count in [10, 100, 1_000, 10_000] {
        let mut items = vec![];

        for seqno in (0..(item_count - 2)).rev() {
            items.push(InternalValue::from_components(
                *b"a",
                *b"a",
                seqno,
                lsm_tree::ValueType::Value,
            ));
        }
        for seqno in (0..2).rev() {
            items.push(InternalValue::from_components(
                *b"b",
                *b"b",
                seqno,
                lsm_tree::ValueType::Value,
            ));
        }

        let block = ValueBlock {
            items: items.into_boxed_slice(),
            header: BlockHeader {
                compression: CompressionType::Lz4,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                previous_block_offset: 0,
                uncompressed_length: 0,
            },
        };

        group.bench_function(format!("{item_count} items (linear)"), |b| {
            b.iter(|| {
                let item = block
                    .items
                    .iter()
                    .find(|item| &*item.key.user_key == b"b")
                    .cloned()
                    .unwrap();
                assert_eq!(item.key.seqno, 1);
            })
        });

        group.bench_function(format!("{item_count} items (binary search)"), |b| {
            b.iter(|| {
                let item = block.get_latest(b"b").unwrap();
                assert_eq!(item.key.seqno, 1);
            })
        });
    }
}

fn load_value_block_from_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("Load block from disk");

    for comp_type in [
        CompressionType::None,
        CompressionType::Lz4,
        CompressionType::Miniz(6),
    ] {
        for block_size in [1, 4, 8, 16, 32, 64, 128] {
            let block_size = block_size * 1_024;

            let mut size = 0;

            let mut items = vec![];

            for x in 0u64.. {
                let value = InternalValue::from_components(
                    x.to_be_bytes(),
                    x.to_string().repeat(50).as_bytes(),
                    63,
                    lsm_tree::ValueType::Value,
                );

                size += value.size();

                items.push(value);

                if size >= block_size {
                    break;
                }
            }

            // Serialize block
            let (mut header, data) = ValueBlock::to_bytes_compressed(&items, 0, comp_type).unwrap();
            header.checksum = Checksum::from_bytes(&data);

            let mut file = tempfile::tempfile().unwrap();
            header.encode_into(&mut file).unwrap();
            file.write_all(&data).unwrap();

            let expected_block = ValueBlock {
                items: items.clone().into_boxed_slice(),
                header,
            };

            group.bench_function(format!("{block_size} KiB [{comp_type}]"), |b| {
                b.iter(|| {
                    let loaded_block = ValueBlock::from_file(&mut file, 0).unwrap();

                    assert_eq!(loaded_block.items.len(), expected_block.items.len());
                    assert_eq!(loaded_block.header.checksum, expected_block.header.checksum);
                });
            });
        }
    }
}

criterion_group!(benches, value_block_find, load_value_block_from_disk);
criterion_main!(benches);
