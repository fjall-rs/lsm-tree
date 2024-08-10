use crate::CompressionType;
use value_log::Compressor;

#[derive(Copy, Clone, Debug)]
pub struct MyCompressor(CompressionType);

impl Default for MyCompressor {
    fn default() -> Self {
        Self(CompressionType::None)
    }
}

impl Compressor for MyCompressor {
    fn compress(&self, bytes: &[u8]) -> value_log::Result<Vec<u8>> {
        Ok(match self.0 {
            CompressionType::None => bytes.into(),

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => lz4_flex::compress_prepend_size(bytes),

            #[cfg(feature = "miniz")]
            CompressionType::Miniz(lvl) => miniz_oxide::deflate::compress_to_vec(bytes, lvl),
        })
    }

    fn decompress(&self, bytes: &[u8]) -> value_log::Result<Vec<u8>> {
        match self.0 {
            CompressionType::None => Ok(bytes.into()),

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                lz4_flex::decompress_size_prepended(bytes).map_err(|_| value_log::Error::Decompress)
            }

            #[cfg(feature = "miniz")]
            CompressionType::Miniz(_) => miniz_oxide::inflate::decompress_to_vec(bytes)
                .map_err(|_| value_log::Error::Decompress),
        }
    }
}
