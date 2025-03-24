#[derive(Copy, Clone, Default, Debug, std::hash::Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct BlockOffset(pub u64);

impl std::ops::Deref for BlockOffset {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::AddAssign<Self> for BlockOffset {
    fn add_assign(&mut self, rhs: Self) {
        *self += *rhs;
    }
}

impl std::ops::AddAssign<u64> for BlockOffset {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl std::fmt::Display for BlockOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
