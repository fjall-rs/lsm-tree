use super::{meta::Metadata, writer::FileOffsets};
use crate::{
    serde::{Deserializable, Serializable},
    DeserializeError, SerializeError,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    fs::File,
    io::{BufReader, Read, Seek, Write},
    path::Path,
};

pub const TRAILER_MAGIC: &[u8] = &[b'F', b'J', b'L', b'L', b'T', b'R', b'L', b'1'];
pub const TRAILER_SIZE: usize = 256;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct SegmentFileTrailer {
    #[doc(hidden)]
    pub metadata: Metadata,

    #[doc(hidden)]
    pub offsets: FileOffsets,
}

impl SegmentFileTrailer {
    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        reader.seek(std::io::SeekFrom::End(-(TRAILER_SIZE as i64)))?;

        // Parse all pointers
        let index_block_ptr = reader.read_u64::<BigEndian>()?;
        let tli_ptr = reader.read_u64::<BigEndian>()?;
        let bloom_ptr = reader.read_u64::<BigEndian>()?;
        let range_tombstone_ptr = reader.read_u64::<BigEndian>()?;
        let metadata_ptr = reader.read_u64::<BigEndian>()?;

        let remaining_padding = TRAILER_SIZE - 5 * std::mem::size_of::<u64>() - TRAILER_MAGIC.len();
        reader.seek_relative(remaining_padding as i64)?;

        // Check trailer magic
        let mut magic = [0u8; TRAILER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != TRAILER_MAGIC {
            return Err(crate::Error::Deserialize(DeserializeError::InvalidHeader(
                "SegmentMetadata",
            )));
        }

        let offsets = FileOffsets {
            index_block_ptr,
            tli_ptr,
            bloom_ptr,
            range_tombstone_ptr,
            metadata_ptr,
        };

        log::trace!("Trailer offsets: {offsets:#?}");

        // Jump to metadata and parse
        reader.seek(std::io::SeekFrom::Start(metadata_ptr))?;
        let metadata = Metadata::deserialize(&mut reader)?;

        Ok(Self { metadata, offsets })
    }
}

impl Serializable for SegmentFileTrailer {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        let mut v = Vec::with_capacity(TRAILER_SIZE);

        v.write_u64::<BigEndian>(self.offsets.index_block_ptr)?;
        v.write_u64::<BigEndian>(self.offsets.tli_ptr)?;
        v.write_u64::<BigEndian>(self.offsets.bloom_ptr)?;
        v.write_u64::<BigEndian>(self.offsets.range_tombstone_ptr)?;
        v.write_u64::<BigEndian>(self.offsets.metadata_ptr)?;

        // Pad with remaining bytes
        v.resize(TRAILER_SIZE - TRAILER_MAGIC.len(), 0);

        v.write_all(TRAILER_MAGIC)?;

        assert_eq!(
            v.len(),
            TRAILER_SIZE,
            "segment file trailer has invalid size"
        );

        writer.write_all(&v)?;

        Ok(())
    }
}
