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

impl From<StandardGuard> for IterGuardImpl {
    fn from(guard: StandardGuard) -> Self {
        Self::Standard(guard)
    }
}

impl From<BlobGuard> for IterGuardImpl {
    fn from(guard: BlobGuard) -> Self {
        Self::Blob(guard)
    }
}

/// Calls the same method on whichever guard type is inside
macro_rules! dispatch {
    ($self:expr, $guard:ident => $call:expr) => {
        match $self {
            IterGuardImpl::Standard($guard) => $call,
            IterGuardImpl::Blob($guard) => $call,
        }
    };
}

// NOTE: Every method is forwarded, including the ones with a default implementation,
// because the guards may override them
impl IterGuard for IterGuardImpl {
    fn into_inner_if(
        self,
        pred: impl Fn(&UserKey) -> bool,
    ) -> crate::Result<(UserKey, Option<UserValue>)> {
        dispatch!(self, g => g.into_inner_if(pred))
    }

    fn into_inner(self) -> crate::Result<KvPair> {
        dispatch!(self, g => g.into_inner())
    }

    fn key(self) -> crate::Result<UserKey> {
        dispatch!(self, g => g.key())
    }

    fn size(self) -> crate::Result<u32> {
        dispatch!(self, g => g.size())
    }

    fn value(self) -> crate::Result<UserValue> {
        dispatch!(self, g => g.value())
    }
}
