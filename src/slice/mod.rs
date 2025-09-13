// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

// Using tokio bytes
#[cfg(feature = "bytes_1")]
mod slice_bytes;

// Using byteview
#[cfg(not(feature = "bytes_1"))]
mod slice_default;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[cfg(not(feature = "bytes_1"))]
pub use slice_default::Slice;

#[cfg(feature = "bytes_1")]
pub use slice_bytes::Slice;

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for Slice {
    fn from(value: &[u8]) -> Self {
        #[cfg(not(feature = "bytes_1"))]
        {
            Self(byteview::ByteView::new(value))
        }

        #[cfg(feature = "bytes_1")]
        {
            Self(bytes::Bytes::from(value.to_vec()))
        }
    }
}

impl From<Arc<[u8]>> for Slice {
    fn from(value: Arc<[u8]>) -> Self {
        Self::from(&*value)
    }
}

impl From<&Vec<u8>> for Slice {
    fn from(value: &Vec<u8>) -> Self {
        Self::from(value.as_slice())
    }
}

impl From<&str> for Slice {
    fn from(value: &str) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<&String> for Slice {
    fn from(value: &String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<&Path> for Slice {
    fn from(value: &Path) -> Self {
        Self::from(value.as_os_str().as_encoded_bytes())
    }
}

impl From<PathBuf> for Slice {
    fn from(value: PathBuf) -> Self {
        Self::from(value.as_os_str().as_encoded_bytes())
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

impl<const N: usize> From<&[u8; N]> for Slice {
    fn from(value: &[u8; N]) -> Self {
        Self::from(value.as_slice())
    }
}

impl FromIterator<u8> for Slice {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = u8>,
    {
        Vec::from_iter(iter).into()
    }
}

impl std::ops::Deref for Slice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl std::borrow::Borrow<[u8]> for Slice {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl<T> PartialEq<T> for Slice
where
    T: AsRef<[u8]>,
{
    fn eq(&self, other: &T) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl PartialEq<Slice> for &[u8] {
    fn eq(&self, other: &Slice) -> bool {
        *self == other.as_ref()
    }
}

impl<T> PartialOrd<T> for Slice
where
    T: AsRef<[u8]>,
{
    fn partial_cmp(&self, other: &T) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}

impl PartialOrd<Slice> for &[u8] {
    fn partial_cmp(&self, other: &Slice) -> Option<std::cmp::Ordering> {
        (*self).partial_cmp(other.as_ref())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::Slice;
    use std::{fmt::Debug, sync::Arc};
    use test_log::test;

    fn assert_slice_handles<T>(v: T)
    where
        T: Clone + Debug,
        Slice: From<T> + PartialEq<T> + PartialOrd<T>,
    {
        // verify slice arc roundtrips
        let slice: Slice = v.clone().into();
        assert_eq!(slice, v, "slice_arc: {slice:?}, v: {v:?}");
        assert!(slice >= v, "slice_arc: {slice:?}, v: {v:?}");
    }

    #[test]
    fn slice_empty() {
        assert_eq!(Slice::empty(), []);
    }

    #[test]
    fn slice_fuse_empty() {
        let bytes = Slice::fused(&[], &[]);
        assert_eq!(&*bytes, &[] as &[u8]);
    }

    #[test]
    fn slice_fuse_one() {
        let bytes = Slice::fused(b"abc", &[]);
        assert_eq!(&*bytes, b"abc");
    }

    #[test]
    fn slice_fuse_two() {
        let bytes = Slice::fused(b"abc", b"def");
        assert_eq!(&*bytes, b"abcdef");
    }

    #[test]
    #[allow(unsafe_code)]
    fn slice_with_size() {
        assert_eq!(
            &*unsafe {
                let mut b = Slice::builder_unzeroed(5);
                b.fill(0);
                b.freeze()
            },
            [0; 5],
        );
        assert_eq!(
            &*unsafe {
                let mut b = Slice::builder_unzeroed(50);
                b.fill(0);
                b.freeze()
            },
            [0; 50],
        );
        assert_eq!(
            &*unsafe {
                let mut b = Slice::builder_unzeroed(50);
                b.fill(77);
                b.freeze()
            },
            [77; 50],
        );
    }

    /// This test verifies that we can create a `Slice` from various types and compare a `Slice` with them.
    #[test]
    fn test_slice_instantiation() {
        // - &[u8]
        assert_slice_handles::<&[u8]>(&[1, 2, 3, 4]);
        // - Arc<u8>
        assert_slice_handles::<Arc<[u8]>>(Arc::new([1, 2, 3, 4]));
        // - Vec<u8>
        assert_slice_handles::<Vec<u8>>(vec![1, 2, 3, 4]);
        // - &str
        assert_slice_handles::<&str>("hello");
        // - String
        assert_slice_handles::<String>("hello".to_string());
        // - [u8; N]
        assert_slice_handles::<[u8; 4]>([1, 2, 3, 4]);

        // Special case for these types
        // - Iterator<Item = u8>
        let slice = Slice::from_iter(vec![1, 2, 3, 4]);
        assert_eq!(slice, vec![1, 2, 3, 4]);

        // - Arc<str>
        let arc_str: Arc<str> = Arc::from("hello");
        let slice = Slice::from(arc_str.clone());
        assert_eq!(slice.as_ref(), arc_str.as_bytes());

        // - io::Read
        let mut reader = std::io::Cursor::new(vec![1, 2, 3, 4]);
        let slice = Slice::from_reader(&mut reader, 4).expect("read");
        assert_eq!(slice, vec![1, 2, 3, 4]);
    }
}
