use super::{calculate_bucket_position, MARKER_CONFLICT, MARKER_FREE};

pub struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub fn get(&self, key: &[u8]) -> Option<u8> {
        // NOTE: We know the hash index has a bucket count <= u8
        #[allow(clippy::cast_possible_truncation)]
        let bucket_count = self.0.len() as u8;

        let bucket_pos = calculate_bucket_position(key, bucket_count);

        // SAFETY: We used modulo
        #[allow(unsafe_code)]
        let marker = unsafe { *self.0.get_unchecked(bucket_pos) };

        match marker {
            MARKER_CONFLICT | MARKER_FREE => None,
            idx => Some(idx),
        }
    }
}
