use crate::{BlobTree, Tree};
use enum_dispatch::enum_dispatch;

/// May be a standard [`Tree`] or a [`BlobTree`].
#[derive(Clone)]
#[enum_dispatch(AbstractTree)]
pub enum AnyTree {
    /// Standard LSM-tree, see [`Tree`]
    Standard(Tree),

    /// Key-value separated LSM-tree, see [`BlobTree`]
    Blob(BlobTree),
}
