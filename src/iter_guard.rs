use crate::{UserKey, UserValue};

pub trait IterGuard {
    fn key(self) -> crate::Result<UserKey>;
    fn with_value(self) -> crate::Result<(UserKey, UserValue)>;
}
