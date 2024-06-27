#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub enum CompressionType {
    Lz4,
}

impl From<CompressionType> for u8 {
    fn from(val: CompressionType) -> Self {
        match val {
            CompressionType::Lz4 => 1,
        }
    }
}

impl TryFrom<u8> for CompressionType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Lz4),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lz4")
    }
}
