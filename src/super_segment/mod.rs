pub(crate) mod binary_index;
pub(crate) mod data_block;
pub(crate) mod hash_index;

use crate::{segment::block::header::Header, Slice};

pub use data_block::DataBlock;

/// A block on disk.
///
/// Consists of a header and some bytes (the data/payload)
pub struct Block {
    pub header: Header,
    pub data: Slice,
}

impl Block {
    /// Returns the uncompressed block size in bytes
    #[must_use]
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/* impl Decode for Block {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Result<Self, DecodeError>
    where
        Self: Sized,
    {
        let header = Header::decode_from(reader)?;
        let data = Slice::from_reader(reader, header.data_length as usize)?;
        let data = match header.compression {
            CompressionType::None => data,

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => lz4_flex::decompress_size_prepended(&data)
                .map(Into::into)
                .map_err(|_| crate::Error::Decompress(header.compression))?,

            #[cfg(feature = "miniz")]
            CompressionType::Miniz(_) => miniz_oxide::inflate::decompress_to_vec(&data)
                .map(Into::into)
                .map_err(|_| crate::Error::Decompress(header.compression))?,
        };

        Ok(Self { header, data })
    }
} */
