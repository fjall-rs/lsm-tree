use crate::{
    blob_tree::Guard as BlobGuard, tree::Guard as StandardGuard, KvPair, UserKey, UserValue,
};
use enum_dispatch::enum_dispatch;

/// An iterator item
#[enum_dispatch]
pub trait IterGuard {
    /// Accesses the key-value tuple.
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

#[enum_dispatch(IterGuard)]
pub enum IterGuardImpl<'a> {
    Standard(StandardGuard),
    Blob(BlobGuard<'a>),
}
