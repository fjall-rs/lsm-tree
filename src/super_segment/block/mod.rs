mod encoder;
mod header;
mod trailer;

pub use encoder::{Encodable, Encoder};
pub use header::Header;
pub use trailer::{Trailer, TRAILER_START_MARKER};

use crate::Slice;

/// A block on disk.
///
/// Consists of a header and some bytes (the data/payload).
#[derive(Clone)]
pub struct Block {
    pub header: Header,
    pub data: Slice,
}

impl Block {
    /// Returns the uncompressed block size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.data.len()
    }
}
