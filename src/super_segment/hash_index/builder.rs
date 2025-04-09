use super::{calculate_bucket_position, MARKER_CONFLICT, MARKER_FREE};
use byteorder::WriteBytesExt;

pub const MAX_POINTERS_FOR_HASH_INDEX: u8 = u8::MAX - 2;

/// Builds a block hash index
#[derive(Debug)]
pub struct Builder(Vec<u8>);

impl Builder {
    /// Initializes a new builder with the given amount of buckets.
    pub fn new(bucket_count: u32) -> Self {
        Self(vec![MARKER_FREE; bucket_count as usize])
    }

    // NOTE: We know the hash index has a bucket count <= u8
    #[allow(clippy::cast_possible_truncation)]
    /// Returns the number of buckets.
    pub fn bucket_count(&self) -> u32 {
        self.0.len() as u32
    }

    /// Tries to map the given key to the binary index position.
    pub fn set(&mut self, key: &[u8], binary_index_pos: u8) -> bool {
        let bucket_pos = calculate_bucket_position(key, self.bucket_count());

        // SAFETY: We use modulo in `calculate_bucket_position`
        #[allow(unsafe_code)]
        let curr_marker = unsafe { *self.0.get_unchecked(bucket_pos) };

        match curr_marker {
            MARKER_CONFLICT => false,
            MARKER_FREE => {
                // SAFETY: We previously asserted that the slot exists
                #[allow(unsafe_code)]
                unsafe {
                    *self.0.get_unchecked_mut(bucket_pos) = binary_index_pos;
                }

                true
            }
            x if x == binary_index_pos => {
                // NOTE: If different keys map to the same bucket, we can keep
                // the mapping
                true
            }
            _ => {
                // NOTE: Mark as conflicted

                // SAFETY: We previously asserted that the slot exists
                #[allow(unsafe_code)]
                unsafe {
                    *self.0.get_unchecked_mut(bucket_pos) = MARKER_CONFLICT;
                }

                false
            }
        }
    }

    /// Consumes the builder, returning its raw bytes.
    ///
    /// Only used for tests
    #[cfg(test)]
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    /// Appends the raw index bytes to a writer.
    pub fn write<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        for byte in self.0 {
            writer.write_u8(byte)?;
        }
        Ok(())
    }
}
