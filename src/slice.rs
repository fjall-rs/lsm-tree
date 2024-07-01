use std::sync::Arc;

/// An immutable byte slice that can be cloned without additional heap allocation.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Slice(Arc<[u8]>);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self::from(bytes)
    }

    /// Clones `self` into a new `Vec`.
    #[must_use]
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Returns `true` if the slice contains no elements.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of elements in the slice, also referred to
    /// as its 'length'.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl std::ops::Deref for Slice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Slice> for Vec<u8> {
    fn from(val: Slice) -> Self {
        val.0.to_vec()
    }
}

impl From<Slice> for Arc<[u8]> {
    fn from(val: Slice) -> Self {
        val.0
    }
}

impl From<&[u8]> for Slice {
    fn from(value: &[u8]) -> Self {
        Self(value.into())
    }
}

impl From<Arc<[u8]>> for Slice {
    fn from(value: Arc<[u8]>) -> Self {
        Self(value)
    }
}

impl From<Vec<u8>> for Slice {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into())
    }
}

impl From<std::borrow::Cow<'_, str>> for Slice {
    fn from(value: std::borrow::Cow<'_, str>) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<Box<str>> for Slice {
    fn from(value: Box<str>) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<&str> for Slice {
    fn from(value: &str) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<String> for Slice {
    fn from(value: String) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<Arc<str>> for Slice {
    fn from(value: Arc<str>) -> Self {
        Self::from(&*value)
    }
}

impl<const N: usize> From<[u8; N]> for Slice {
    fn from(value: [u8; N]) -> Self {
        Self::from(value.as_slice())
    }
}
