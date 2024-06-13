use crate::CompressionType;
use std::sync::Arc;
use value_log::Compressor;

#[cfg(feature = "lz4")]
struct Lz4Compressor;

#[cfg(feature = "lz4")]
impl Compressor for Lz4Compressor {
    fn compress(&self, bytes: &[u8]) -> Result<Vec<u8>, value_log::CompressError> {
        Ok(lz4_flex::compress_prepend_size(bytes))
    }

    fn decompress(&self, bytes: &[u8]) -> Result<Vec<u8>, value_log::DecompressError> {
        lz4_flex::decompress_size_prepended(bytes)
            .map_err(|e| value_log::DecompressError(e.to_string()))
    }
}

#[cfg(feature = "miniz")]
struct MinizCompressor;

#[cfg(feature = "miniz")]
impl Compressor for MinizCompressor {
    fn compress(&self, bytes: &[u8]) -> Result<Vec<u8>, value_log::CompressError> {
        // TODO: level
        Ok(miniz_oxide::deflate::compress_to_vec(bytes, 10))
    }

    fn decompress(&self, bytes: &[u8]) -> Result<Vec<u8>, value_log::DecompressError> {
        // TODO: level
        miniz_oxide::inflate::decompress_to_vec(bytes)
            .map_err(|e| value_log::DecompressError(e.to_string()))
    }
}

pub fn get_vlog_compressor(compression: CompressionType) -> Arc<dyn Compressor + Send + Sync> {
    match compression {
        CompressionType::None => Arc::new(value_log::NoCompressor),

        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => Arc::new(Lz4Compressor),

        #[cfg(feature = "miniz")]
        CompressionType::Miniz => Arc::new(MinizCompressor),
    }
}
