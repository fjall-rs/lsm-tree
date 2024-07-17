use crate::{
    descriptor_table::FileDescriptorTable,
    memtable::MemTable,
    segment::{block_index::BlockIndex, meta::SegmentId, writer::Writer, Segment},
    tree::inner::TreeId,
    BlockCache,
};
use std::{path::PathBuf, sync::Arc};

#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;

/// Flush options
#[doc(hidden)]
pub struct Options {
    /// [`MemTable`] to flush
    pub memtable: Arc<MemTable>,

    /// Tree ID
    pub tree_id: TreeId,

    /// Unique segment ID
    pub segment_id: SegmentId,

    /// Base folder of segments
    ///
    /// The segment will be stored as {folder}/{segment_id}
    #[allow(clippy::doc_markdown)]
    pub folder: PathBuf,

    /// Block size in bytes
    pub block_size: u32,

    // Block cache
    pub block_cache: Arc<BlockCache>,

    // Descriptor table
    pub descriptor_table: Arc<FileDescriptorTable>,
}

/// Flushes a memtable, creating a segment in the given folder
#[allow(clippy::module_name_repetitions)]
#[doc(hidden)]
pub fn flush_to_segment(opts: Options) -> crate::Result<Segment> {
    let segment_file_path = opts.folder.join(opts.segment_id.to_string());
    log::debug!("Flushing segment to {segment_file_path:?}");

    let mut segment_writer = Writer::new(crate::segment::writer::Options {
        segment_id: opts.segment_id,

        folder: opts.folder.clone(),
        evict_tombstones: false,
        block_size: opts.block_size,

        #[cfg(feature = "bloom")]
        bloom_fp_rate: 0.0001,
    })?;

    for entry in opts.memtable.iter() {
        segment_writer.write(entry)?;
    }

    let trailer = segment_writer
        .finish()?
        .expect("memtable should not be empty");

    log::debug!("Finalized segment write at {segment_file_path:?}");

    // TODO: if L0, L1, preload block index (non-partitioned)
    let block_index = Arc::new(BlockIndex::from_file(
        &segment_file_path,
        trailer.offsets.tli_ptr,
        (opts.tree_id, opts.segment_id).into(),
        opts.descriptor_table.clone(),
        opts.block_cache.clone(),
    )?);

    #[cfg(feature = "bloom")]
    let bloom_ptr = trailer.offsets.bloom_ptr;

    let created_segment = Segment {
        tree_id: opts.tree_id,

        metadata: trailer.metadata,
        offsets: trailer.offsets,

        descriptor_table: opts.descriptor_table.clone(),
        block_index,
        block_cache: opts.block_cache,

        // TODO: as Bloom method
        #[cfg(feature = "bloom")]
        bloom_filter: {
            use crate::serde::Deserializable;
            use std::io::Seek;

            assert!(bloom_ptr > 0, "can not find bloom filter block");

            let mut reader = std::fs::File::open(&segment_file_path)?;
            reader.seek(std::io::SeekFrom::Start(bloom_ptr))?;
            BloomFilter::deserialize(&mut reader)?
        },
    };

    opts.descriptor_table.insert(
        &segment_file_path,
        (opts.tree_id, created_segment.metadata.id).into(),
    );

    log::debug!("Flushed segment to {segment_file_path:?}");

    Ok(created_segment)
}
