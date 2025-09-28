// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

// TODO: remove MaybeInlineValue because we can just store values flat and look at key instead

// TODO: add ValueType::is_vhandle

/// Value type (regular value or tombstone)
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub enum ValueType {
    /// Existing value
    Value,

    /// Deleted value
    Tombstone,

    /// "Weak" deletion (a.k.a. `SingleDelete` in `RocksDB`)
    WeakTombstone,

    /// Value handle
    ///
    /// Points to a blob in a blob file.
    Indirection,
}

impl ValueType {
    /// Returns `true` if the type is a tombstone marker (either normal or weak).
    #[must_use]
    pub fn is_tombstone(self) -> bool {
        self == Self::Tombstone || self == Self::WeakTombstone
    }

    pub(crate) fn is_indirection(self) -> bool {
        self == Self::Indirection
    }
}

impl TryFrom<u8> for ValueType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Value),
            0x0000_0001 => Ok(Self::Tombstone),
            0x0000_0011 => Ok(Self::WeakTombstone),
            0b1000_0000 => Ok(Self::Indirection),
            _ => Err(()),
        }
    }
}

impl From<ValueType> for u8 {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Value => 0,
            ValueType::Tombstone => 0x0000_0001,
            ValueType::WeakTombstone => 0x0000_0011,
            ValueType::Indirection => 0b1000_0000,
        }
    }
}
