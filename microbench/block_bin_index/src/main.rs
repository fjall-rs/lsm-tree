use lsm_tree::{InternalValue, SeqNo};
use rand::{Rng, RngCore};
use std::io::Write;

fn generate_key(primary_key: u64, secondary_key: u64) -> [u8; 16] {
    scru128::new().into()
}

pub fn main() -> lsm_tree::Result<()> {
    env_logger::Builder::from_default_env().init();

    let mut rng = rand::rng();

    #[cfg(feature = "use_unsafe")]
    let used_unsafe = true;

    #[cfg(not(feature = "use_unsafe"))]
    let used_unsafe = false;

    for item_count in [10, 50, 100, 250, 500, 1_000, 2_000, 4_000] {
        let mut items = vec![];

        {
            let mut buf = [0u8; 16];

            for item in 0u64..item_count {
                let key = generate_key(item, 0);
                rng.fill_bytes(&mut buf);

                items.push(InternalValue::from_components(
                    &key,
                    &buf,
                    0,
                    lsm_tree::ValueType::Value,
                ));
            }
        }

        let intervals: &[u8] = if std::env::var("DEFAULT_RESTART_INTERVAL_ONLY").is_ok() {
            &[16]
        } else {
            &[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ]
        };

        for &restart_interval in intervals {
            // eprintln!("hash_ratio={hash_ratio}");

            use lsm_tree::segment::{
                block::{BlockType, Header},
                BlockOffset, Checksum, DataBlock,
            };

            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;
            // eprintln!("{bytes:?}");
            // eprintln!("{}", String::from_utf8_lossy(&bytes));
            // eprintln!("encoded into {} bytes", bytes.len());

            {
                use lsm_tree::segment::Block;
                use std::time::Instant;

                let block = DataBlock::new(Block {
                    data: lsm_tree::Slice::new(&bytes),
                    header: Header {
                        checksum: Checksum::from_raw(0),
                        data_length: 0,
                        uncompressed_length: 0,
                        previous_block_offset: BlockOffset(0),
                        block_type: BlockType::Data,
                    },
                });

                /* eprintln!(
                    "hash index conflicts: {:?} / {:?}",
                    block.hash_bucket_conflict_count(),
                    block.hash_bucket_count(),
                );
                eprintln!(
                    "hash index free slots: {:?} / {:?}",
                    block.hash_bucket_free_count(),
                    block.hash_bucket_count(),
                ); */

                {
                    const NUM_RUNS: u128 = 10_000_000;

                    let start = Instant::now();
                    for _ in 0..NUM_RUNS {
                        let needle = rng.random_range(0..item_count as usize);
                        let needle = &items[needle].key.user_key;

                        let mut iter = block.iter();

                        assert!(
                            iter.seek(&needle /* TODO: , SeqNo::MAX */),
                            "did not find key",
                        );
                        // block.point_read(&needle, None);
                    }

                    let rps_ns = {
                        let ns = start.elapsed().as_nanos();
                        ns / NUM_RUNS
                    };

                    /* eprintln!("one read took {:?}ns",); */

                    println!(
                        "{}",
                        serde_json::json!({
                            "block_size": bytes.len(),
                            "restart_interval": restart_interval,
                            "rps_ns": rps_ns,
                            "item_count": item_count,
                            "unsafe": used_unsafe,
                        })
                        .to_string(),
                    );
                }

                /*   {
                    let start = Instant::now();
                    for _ in 0..25_000 {
                        assert_eq!(items.len(), block.iter().count());
                    }

                    eprintln!("one iter() took {:?}ns", {
                        let ns = start.elapsed().as_nanos() as usize;
                        ns / 25_000 / items.len()
                    });
                } */

                /* {
                    let start = Instant::now();
                    for _ in 0..25_000 {
                        assert_eq!(items.len(), block.iter().rev().count());
                    }

                    eprintln!("one iter().rev() took {:?}ns", {
                        let ns = start.elapsed().as_nanos() as usize;
                        ns / 25_000 / items.len()
                    });
                } */
            }

            /* {
                let mut writer = vec![];
                header.encode_into(&mut writer)?;
                writer.write_all(&bytes)?;

                eprintln!("V3 format (uncompressed): {}B", writer.len());
            }

            {
                let mut writer = vec![];
                header.encode_into(&mut writer)?;

                let bytes = lz4_flex::compress_prepend_size(&bytes);
                writer.write_all(&bytes)?;

                eprintln!("V3 format (LZ4): {}B", writer.len());
            } */
        }
    }

    Ok(())
}
