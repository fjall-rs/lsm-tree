use super::{
    meta::Metadata,
    writer::{Options, Writer},
};
use crate::{time::unix_timestamp, Value};
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
    created_items: Vec<Metadata>,

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
            folder: opts.folder.join(current_segment_id.to_string()),
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

        // Flush segment, and start new one
        self.writer.finish()?;

        let old_segment_id = self.current_segment_id;
        let new_segment_id = self.get_next_segment_id();

        let new_writer = Writer::new(Options {
            folder: self.opts.folder.join(new_segment_id.to_string()),
            evict_tombstones: self.opts.evict_tombstones,
            block_size: self.opts.block_size,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: self.opts.bloom_fp_rate,
        })?;

        let old_writer = std::mem::replace(&mut self.writer, new_writer);

        if old_writer.item_count > 0 {
            let metadata = Metadata::from_writer(old_segment_id, old_writer)?;
            self.created_items.push(metadata);
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
    pub fn finish(mut self) -> crate::Result<Vec<Metadata>> {
        // Finish writer and consume it
        // Don't use `rotate` because that will start a new writer, creating unneeded, empty segments
        self.writer.finish()?;

        if self.writer.item_count > 0 {
            let metadata = Metadata::from_writer(self.current_segment_id, self.writer)?;
            self.created_items.push(metadata);
        }

        let now = unix_timestamp();

        // Set creation date of all segments to the same timestamp
        for item in &mut self.created_items {
            item.created_at = now.as_micros();
        }

        Ok(self.created_items)
    }
}
