use super::{calculate_bucket_position, MARKER_CONFLICT, MARKER_FREE};

/// Hash index lookup result
#[derive(Debug, Eq, PartialEq)]
pub enum Lookup {
    /// Key is found, can skip the binary index search - fast path
    Found(u8),

    /// Key's bucket was still FREE, so it definitely does not exist
    NotFound,

    /// Key is conflicted - we need to look in the binary index instead - slow path
    Conflicted,
}

/// Helper to read from an embedded block hash index
pub struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    /// Initializes a new hash index reader.
    pub fn new(bytes: &'a [u8], offset: u32, len: u32) -> Self {
        let offset = offset as usize;
        let len = len as usize;
        let end = offset + len;

        // NOTE: We consider the caller to be trustworthy
        #[warn(clippy::indexing_slicing)]
        Self(&bytes[offset..end])
    }

    // NOTE: Not used for performance reasons, so no need to be hyper-optimized
    #[allow(clippy::naive_bytecount)]
    /// Returns the amount of empty slots in the hash index.
    pub fn free_count(&self) -> usize {
        self.0.iter().filter(|&&byte| byte == MARKER_FREE).count()
    }

    // NOTE: Not used for performance reasons, so no need to be hyper-optimized
    #[allow(clippy::naive_bytecount)]
    /// Returns the amount of conflict markers in the hash index.
    pub fn conflict_count(&self) -> usize {
        self.0
            .iter()
            .filter(|&&byte| byte == MARKER_CONFLICT)
            .count()
    }

    /// Returns the binary index position if the key is not conflicted.
    pub fn get(&self, key: &[u8]) -> Lookup {
        // NOTE: Even with very high hash ratio, there will be nearly enough items to
        // cause us to create u32 buckets
        #[allow(clippy::cast_possible_truncation)]
        let bucket_count = self.0.len() as u32;

        let bucket_pos = calculate_bucket_position(key, bucket_count);

        // SAFETY: We use modulo in `calculate_bucket_position`
        #[allow(unsafe_code)]
        let marker = unsafe { *self.0.get_unchecked(bucket_pos) };

        match marker {
            MARKER_CONFLICT => Lookup::Conflicted,
            MARKER_FREE => Lookup::NotFound,
            idx => Lookup::Found(idx),
        }
    }
}
