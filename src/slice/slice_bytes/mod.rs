// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use bytes::{Bytes, BytesMut};

/// An immutable byte slice that can be cloned without additional heap allocation
///
/// There is no guarantee of any sort of alignment for zero-copy (de)serialization.
#[derive(Debug, Clone, Eq, Hash, Ord)]
pub struct Slice(pub(super) Bytes);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }

    #[doc(hidden)]
    #[must_use]
    pub fn empty() -> Self {
        Self(Bytes::from_static(&[]))
    }

    pub(crate) unsafe fn builder_unzeroed(len: usize) -> BytesMut {
        // Use `with_capacity` & `set_len`` to avoid zeroing the buffer
        let mut builder = BytesMut::with_capacity(len);

        // SAFETY: we just allocated `len` bytes, and `read_exact` will fail if
        // it doesn't fill the buffer, subsequently dropping the uninitialized
        // BytesMut object
        #[allow(unsafe_code)]
        unsafe {
            builder.set_len(len);
        }

        builder
    }

    #[doc(hidden)]
    #[must_use]
    pub fn slice(&self, range: impl std::ops::RangeBounds<usize>) -> Self {
        Self(self.0.slice(range))
    }

    #[doc(hidden)]
    #[must_use]
    pub fn fused(left: &[u8], right: &[u8]) -> Self {
        use std::io::Write;

        let len = left.len() + right.len();
        let mut builder = unsafe { Self::builder_unzeroed(len) };
        {
            let mut writer = &mut builder[..];

            writer.write_all(left).expect("should write");
            writer.write_all(right).expect("should write");
        }

        Self(builder.freeze())
    }

    /// Constructs a [`Slice`] from an I/O reader by pulling in `len` bytes.
    ///
    /// The reader may not read the existing buffer.
    #[doc(hidden)]
    pub fn from_reader<R: std::io::Read>(reader: &mut R, len: usize) -> std::io::Result<Self> {
        let mut builder = unsafe { Self::builder_unzeroed(len) };

        // SAFETY: Normally, read_exact over an uninitialized buffer is UB,
        // however we know that in lsm-tree etc. only I/O readers or cursors over Vecs are used
        // so it's safe
        //
        // The safe API is unstable: https://github.com/rust-lang/rust/issues/78485
        reader.read_exact(&mut builder)?;

        Ok(Self(builder.freeze()))
    }
}

impl From<Bytes> for Slice {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<Slice> for Bytes {
    fn from(value: Slice) -> Self {
        value.0
    }
}

// Bytes::from<Vec<u8>> is zero-copy optimized
impl From<Vec<u8>> for Slice {
    fn from(value: Vec<u8>) -> Self {
        Self(Bytes::from(value))
    }
}

// Bytes::from<String> is zero-copy optimized
impl From<String> for Slice {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}
