use lsm_tree::{
    segment::{
        block::{Block, BlockType, Header as BlockHeader},
        BlockHandle, BlockOffset, DataBlock,
    },
    CompressionType, InternalValue,
};
use std::time::Instant;

pub fn main() -> lsm_tree::Result<()> {
    env_logger::Builder::from_default_env().init();

    #[cfg(feature = "use_unsafe")]
    let used_unsafe = true;

    #[cfg(not(feature = "use_unsafe"))]
    let used_unsafe = false;

    for item_count in [100, 200, 400, 1_000, 2_000] {
        let mut items = vec![];

        for item in 0u64..item_count {
            items.push(InternalValue::from_components(
                item.to_be_bytes(),
                b"1asdabawerbwqerbqwr",
                0,
                lsm_tree::ValueType::Value,
            ));
        }

        let mut file = std::fs::File::create("block")?;

        let bytes = DataBlock::encode_into_vec(&items, 16, 1.33)?;
        let header = Block::write_into(&mut file, &bytes, BlockType::Data, CompressionType::None)?;
        let bytes_written = BlockHeader::serialized_len() as u32 + header.data_length;

        file.sync_all()?;
        drop(file);

        {
            let file = std::fs::File::open("block")?;

            {
                const NUM_RUNS: u128 = 5_000_000;

                let start = Instant::now();
                for _ in 0..NUM_RUNS {
                    let _block = lsm_tree::segment::Block::from_file(
                        &file,
                        BlockHandle::new(BlockOffset(0), bytes_written as u32),
                        BlockType::Data,
                        CompressionType::None,
                    )?;
                }

                let rps_ns = {
                    let ns = start.elapsed().as_nanos();
                    ns / NUM_RUNS
                };

                println!(
                    "{}",
                    serde_json::json!({
                        "block_size": bytes.len(),
                        "rps_ns": rps_ns,
                        "unsafe": used_unsafe,
                    })
                    .to_string(),
                );
            }
        }
    }

    Ok(())
}
