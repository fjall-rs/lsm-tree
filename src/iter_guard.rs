use crate::{UserKey, UserValue};

pub trait IterGuard {
    fn key(self) -> crate::Result<UserKey>;
    fn size(self) -> crate::Result<u32>;
    fn into_inner(self) -> crate::Result<(UserKey, UserValue)>;
}
