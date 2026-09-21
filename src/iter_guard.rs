use crate::{
    blob_tree::Guard as BlobGuard, tree::Guard as StandardGuard, KvPair, UserKey, UserValue,
};

/// Guard to access key-value pairs
pub trait IterGuard {
    /// Accesses the key-value pair if the predicate returns `true`.
    ///
    /// The predicate receives the key - if returning false, the value
    /// may not be loaded if the tree is key-value separated.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn into_inner_if(
        self,
        pred: impl Fn(&UserKey) -> bool,
    ) -> crate::Result<(UserKey, Option<UserValue>)>;

    /// Accesses the key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn into_inner(self) -> crate::Result<KvPair>;

    /// Accesses the key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn key(self) -> crate::Result<UserKey>;

    /// Returns the value size.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn size(self) -> crate::Result<u32>;

    /// Accesses the value.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn value(self) -> crate::Result<UserValue>
    where
        Self: Sized,
    {
        self.into_inner().map(|(_, v)| v)
    }
}

/// Generic iterator value
pub enum IterGuardImpl {
    /// Iterator value of a standard LSM-tree
    Standard(StandardGuard),

    /// Iterator value of a key-value separated tree
    Blob(BlobGuard),
}

impl IterGuard for IterGuardImpl {
    fn into_inner_if(
        self,
        pred: impl Fn(&UserKey) -> bool,
    ) -> crate::Result<(UserKey, Option<UserValue>)> {
        match self {
            Self::Standard(inner) => IterGuard::into_inner_if(inner, pred),
            Self::Blob(inner) => IterGuard::into_inner_if(inner, pred),
        }
    }

    fn into_inner(self) -> crate::Result<KvPair> {
        match self {
            Self::Standard(inner) => IterGuard::into_inner(inner),
            Self::Blob(inner) => IterGuard::into_inner(inner),
        }
    }

    fn key(self) -> crate::Result<UserKey> {
        match self {
            Self::Standard(inner) => IterGuard::key(inner),
            Self::Blob(inner) => IterGuard::key(inner),
        }
    }

    fn size(self) -> crate::Result<u32> {
        match self {
            Self::Standard(inner) => IterGuard::size(inner),
            Self::Blob(inner) => IterGuard::size(inner),
        }
    }
}
