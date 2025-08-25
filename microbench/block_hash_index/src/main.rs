use lsm_tree::segment::DataBlock;
use lsm_tree::{coding::Encode, InternalValue, ValueType};
use rand::Rng;
use std::io::Write;
use std::time::Instant;

pub fn main() -> lsm_tree::Result<()> {
    env_logger::Builder::from_default_env().init();

    #[cfg(feature = "use_unsafe")]
    let use_unsafe = true;

    #[cfg(not(feature = "use_unsafe"))]
    let use_unsafe = false;

    let mut rng = rand::rng();

    let mut items = vec![];
    let item_count = 500;

    for item in 0u128..item_count {
        items.push(InternalValue::from_components(
            item.to_be_bytes(),
            b"asevrasevfbss4b4n6tuziwernwawrbg",
            0,
            lsm_tree::ValueType::Value,
        ));
    }

    for hash_ratio in [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0] {
        // eprintln!("hash_ratio={hash_ratio}");

        use lsm_tree::segment::{
            block::{BlockType, Header},
            BlockOffset, Checksum, DataBlock,
        };

        let bytes = DataBlock::encode_into_vec(&items, 16, hash_ratio)?;
        // eprintln!("{bytes:?}");
        // eprintln!("{}", String::from_utf8_lossy(&bytes));
        // eprintln!("encoded into {} bytes", bytes.len());

        {
            use lsm_tree::segment::Block;

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
                const NUM_RUNS: u128 = 25_000_000;

                let start = Instant::now();
                for _ in 0..NUM_RUNS {
                    let needle = rng.random_range(0..item_count).to_be_bytes();
                    block.point_read(&needle, u64::MAX).unwrap();
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
                        "hash_ratio": format!("{hash_ratio:.1?}"),
                        "rps_ns": rps_ns,
                        "conflicts": block.get_hash_index_reader().map(|x| x.conflict_count()).unwrap_or_default(),
                        "free": block.get_hash_index_reader().map(|x| x.free_count()).unwrap_or_default(),
                        "use_unsafe": use_unsafe,
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

    Ok(())
}
