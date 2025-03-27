use super::{calculate_bucket_position, MARKER_CONFLICT, MARKER_FREE};
use byteorder::WriteBytesExt;

pub const MAX_POINTERS_FOR_HASH_INDEX: u8 = u8::MAX - 2;

#[derive(Debug)]
pub struct Builder(Vec<u8>);

impl Builder {
    pub fn new(bucket_count: u32) -> Self {
        Self(vec![MARKER_FREE; bucket_count as usize])
    }

    // NOTE: We know the hash index has a bucket count <= u8
    #[allow(clippy::cast_possible_truncation)]
    /// Returns the number of buckets
    pub fn bucket_count(&self) -> u32 {
        self.0.len() as u32
    }

    pub fn set(&mut self, key: &[u8], binary_index_pos: u8) -> bool {
        let bucket_pos = calculate_bucket_position(key, self.bucket_count());

        // SAFETY: We used modulo
        #[warn(unsafe_code)]
        let curr_marker = unsafe { *self.0.get_unchecked(bucket_pos) };

        match curr_marker {
            MARKER_CONFLICT => false,
            MARKER_FREE => {
                // NOTE: Free slot

                // SAFETY: We previously asserted that the slot exists
                #[warn(unsafe_code)]
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
                #[warn(unsafe_code)]
                unsafe {
                    *self.0.get_unchecked_mut(bucket_pos) = MARKER_CONFLICT;
                }

                false
            }
        }
    }

    #[cfg(test)]
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    pub fn write<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        for byte in self.0 {
            writer.write_u8(byte)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn v3_hash_index_simple() {
        let mut hash_index = Builder::new(100);

        hash_index.set(b"a", 5);
        hash_index.set(b"b", 8);
        hash_index.set(b"c", 10);

        // NOTE: Hash index bytes need to be consistent across machines and compilations etc.
        assert_eq!(
            [
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 10, 254, 254, 254, 8, 254,
                254, 254, 5, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254
            ],
            &*hash_index.into_inner()
        );
    }

    #[test]
    fn v3_hash_index_conflict() {
        let mut hash_index = Builder::new(1);

        hash_index.set(b"a", 5);
        hash_index.set(b"b", 8);

        // NOTE: Hash index bytes need to be consistent across machines and compilations etc.
        assert_eq!([255], &*hash_index.into_inner());
    }

    #[test]
    fn v3_hash_index_same_offset() {
        let mut hash_index = Builder::new(1);

        hash_index.set(b"a", 5);
        hash_index.set(b"b", 5);

        // NOTE: Hash index bytes need to be consistent across machines and compilations etc.
        assert_eq!([5], &*hash_index.into_inner());
    }

    #[test]
    fn v3_hash_index_mix() {
        let mut hash_index = Builder::new(1);

        hash_index.set(b"a", 5);
        hash_index.set(b"b", 5);
        hash_index.set(b"c", 6);

        // NOTE: Hash index bytes need to be consistent across machines and compilations etc.
        assert_eq!([255], &*hash_index.into_inner());
    }
}
