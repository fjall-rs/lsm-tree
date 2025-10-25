use super::FilterWriter;
use crate::{
    config::BloomConstructionPolicy,
    segment::{filter::standard_bloom::Builder, Block},
    CompressionType, UserKey,
};

pub struct FullFilterWriter {
    /// Key hashes for AMQ filter
    pub bloom_hash_buffer: Vec<u64>,

    bloom_policy: BloomConstructionPolicy,
}

impl FullFilterWriter {
    pub fn new(bloom_policy: BloomConstructionPolicy) -> Self {
        Self {
            bloom_hash_buffer: Vec::new(),
            bloom_policy,
        }
    }
}

impl<W: std::io::Write + std::io::Seek> FilterWriter<W> for FullFilterWriter {
    fn set_filter_policy(
        mut self: Box<Self>,
        policy: BloomConstructionPolicy,
    ) -> Box<dyn FilterWriter<W>> {
        self.bloom_policy = policy;
        self
    }

    fn register_key(&mut self, key: &UserKey) -> crate::Result<()> {
        self.bloom_hash_buffer.push(Builder::get_hash(key));
        Ok(())
    }

    fn finish(self: Box<Self>, file_writer: &mut sfa::Writer) -> crate::Result<()> {
        if self.bloom_hash_buffer.is_empty() {
            log::trace!("Filter write has no buffered hashes - not building filter");
        } else {
            file_writer.start("filter")?;

            let n = self.bloom_hash_buffer.len();

            log::trace!(
                "Constructing Bloom filter with {n} entries: {:?}",
                self.bloom_policy,
            );

            let start = std::time::Instant::now();

            let filter_bytes = {
                let mut builder = self.bloom_policy.init(n);

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
