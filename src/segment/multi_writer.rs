use super::{
    trailer::SegmentFileTrailer,
    writer::{Options, Writer},
};
use crate::Value;
use std::sync::{atomic::AtomicU64, Arc};

/// Like `Writer` but will rotate to a new segment, once a segment grows larger than `target_size`
///
/// This results in a sorted "run" of segments
#[allow(clippy::module_name_repetitions)]
pub struct MultiWriter {
    /// Target size of segments in bytes
    ///
    /// If a segment reaches the target size, a new one is started,
    /// resulting in a sorted "run" of segments
    pub target_size: u64,

    pub opts: Options,
    created_items: Vec<SegmentFileTrailer>,

    segment_id_generator: Arc<AtomicU64>,
    current_segment_id: u64,

    pub writer: Writer,
}

impl MultiWriter {
    /// Sets up a new `MultiWriter` at the given segments folder
    pub fn new(
        segment_id_generator: Arc<AtomicU64>,
        target_size: u64,
        opts: Options,
    ) -> crate::Result<Self> {
        let current_segment_id =
            segment_id_generator.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let writer = Writer::new(Options {
            segment_id: current_segment_id,
            folder: opts.folder.clone(),
            evict_tombstones: opts.evict_tombstones,
            block_size: opts.block_size,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: opts.bloom_fp_rate,
        })?;

        Ok(Self {
            target_size,
            created_items: Vec::with_capacity(10),
            opts,
            segment_id_generator,
            current_segment_id,
            writer,
        })
    }

    fn get_next_segment_id(&mut self) -> u64 {
        self.current_segment_id = self
            .segment_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.current_segment_id
    }

    /// Flushes the current writer, stores its metadata, and sets up a new writer for the next segment
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating segment writer");

        let new_segment_id = self.get_next_segment_id();

        let new_writer = Writer::new(Options {
            segment_id: new_segment_id,
            folder: self.opts.folder.clone(),
            evict_tombstones: self.opts.evict_tombstones,
            block_size: self.opts.block_size,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: self.opts.bloom_fp_rate,
        })?;

        let mut old_writer = std::mem::replace(&mut self.writer, new_writer);

        if old_writer.item_count > 0 {
            // NOTE: if-check checks for item count
            self.created_items
                .push(old_writer.finish()?.expect("writer should emit result"));
        }

        Ok(())
    }

    /// Writes an item
    pub fn write(&mut self, item: Value) -> crate::Result<()> {
        self.writer.write(item)?;

        if self.writer.file_pos >= self.target_size {
            self.rotate()?;
        }

        Ok(())
    }

    /// Finishes the last segment, making sure all data is written durably
    ///
    /// Returns the metadata of created segments
    pub fn finish(mut self) -> crate::Result<Vec<SegmentFileTrailer>> {
        if let Some(last_writer_result) = self.writer.finish()? {
            self.created_items.push(last_writer_result);
        }

        Ok(self.created_items)
    }
}
