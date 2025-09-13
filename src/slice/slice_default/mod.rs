// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use byteview::ByteView;

/// An immutable byte slice that can be cloned without additional heap allocation
///
/// There is no guarantee of any sort of alignment for zero-copy (de)serialization.
#[derive(Debug, Clone, Eq, Hash, Ord)]
pub struct Slice(pub(super) ByteView);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self(bytes.into())
    }

    pub(crate) fn empty() -> Self {
        Self(ByteView::new(&[]))
    }

    pub(crate) unsafe fn builder_unzeroed(len: usize) -> byteview::Builder {
        ByteView::builder_unzeroed(len)
    }

    pub(crate) fn slice(&self, range: impl std::ops::RangeBounds<usize>) -> Self {
        Self(self.0.slice(range))
    }

    pub(crate) fn fused(left: &[u8], right: &[u8]) -> Self {
        Self(ByteView::fused(left, right))
    }

    pub(crate) fn from_reader<R: std::io::Read>(
        reader: &mut R,
        len: usize,
    ) -> std::io::Result<Self> {
        ByteView::from_reader(reader, len).map(Self)
    }
}

// Arc::from<Vec<u8>> is specialized
impl From<Vec<u8>> for Slice {
    fn from(value: Vec<u8>) -> Self {
        Self(ByteView::from(value))
    }
}

// Arc::from<Vec<String>> is specialized
impl From<String> for Slice {
    fn from(value: String) -> Self {
        Self(ByteView::from(value.into_bytes()))
    }
}

impl From<ByteView> for Slice {
    fn from(value: ByteView) -> Self {
        Self(value)
    }
}

impl From<Slice> for ByteView {
    fn from(value: Slice) -> Self {
        value.0
    }
}
