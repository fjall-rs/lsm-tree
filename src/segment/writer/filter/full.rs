use super::FilterWriter;
use crate::{
    config::BloomConstructionPolicy,
    segment::{filter::standard_bloom::Builder, Block},
    CompressionType,
};

pub struct FullFilterWriter {
    /// Key hashes for AMQ filter
    pub bloom_hash_buffer: Vec<u64>,
}

impl FullFilterWriter {
    pub fn new() -> Self {
        Self {
            bloom_hash_buffer: Vec::new(),
        }
    }
}

impl<W: std::io::Write + std::io::Seek> FilterWriter<W> for FullFilterWriter {
    fn register_key(&mut self, key: &[u8]) -> crate::Result<()> {
        self.bloom_hash_buffer.push(Builder::get_hash(key));
        Ok(())
    }

    fn finish(
        self: Box<Self>,
        file_writer: &mut sfa::Writer,
        bloom_policy: BloomConstructionPolicy,
    ) -> crate::Result<()> {
        if self.bloom_hash_buffer.is_empty() {
            log::trace!("Filter write has no buffered hashes - not building filter");
        } else {
            file_writer.start("filter")?;

            let n = self.bloom_hash_buffer.len();

            log::trace!("Constructing Bloom filter with {n} entries: {bloom_policy:?}");

            let start = std::time::Instant::now();

            let filter_bytes = {
                let mut builder = bloom_policy.init(n);

                for hash in self.bloom_hash_buffer {
                    builder.set_with_hash(hash);
                }

                builder.build()
            };

            log::trace!(
                "Built Bloom filter ({} B) in {:?}",
                filter_bytes.len(),
                start.elapsed(),
            );

            Block::write_into(
                file_writer,
                &filter_bytes,
                crate::segment::block::BlockType::Filter,
                CompressionType::None,
            )?;
        }

        Ok(())
    }
}
