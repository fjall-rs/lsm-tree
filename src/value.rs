// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    key::InternalKey,
    segment::block::ItemSize,
    Slice,
};
use std::io::{Read, Write};
use varint_rs::{VarintReader, VarintWriter};

/// User defined key
pub type UserKey = Slice;

/// User defined data (blob of bytes)
#[allow(clippy::module_name_repetitions)]
pub type UserValue = Slice;

/// Sequence number - a monotonically increasing counter
///
/// Values with the same seqno are part of the same batch.
///
/// A value with a higher sequence number shadows an item with the
/// same key and lower sequence number. This enables MVCC.
///
/// Stale items are lazily garbage-collected during compaction.
pub type SeqNo = u64;

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
}

impl TryFrom<u8> for ValueType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Value),
            1 => Ok(Self::Tombstone),
            2 => Ok(Self::WeakTombstone),
            _ => Err(()),
        }
    }
}

impl From<ValueType> for u8 {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Value => 0,
            ValueType::Tombstone => 1,
            ValueType::WeakTombstone => 2,
        }
    }
}

/// Internal representation of KV pairs
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Eq)]
pub struct InternalValue {
    /// Internal key
    pub key: InternalKey,

    /// User-defined value - an arbitrary byte array
    ///
    /// Supports up to 2^32 bytes
    pub value: UserValue,
}

impl InternalValue {
    /// Creates a new [`Value`].
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16, or the value length is greater than 2^32.
    pub fn new<V: Into<UserValue>>(key: InternalKey, value: V) -> Self {
        let value = value.into();

        assert!(!key.user_key.is_empty(), "key may not be empty");
        assert!(
            u32::try_from(value.len()).is_ok(),
            "values can be 2^32 bytes in length"
        );

        Self { key, value }
    }

    /// Creates a new [`Value`].
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16, or the value length is greater than 2^32.
    pub fn from_components<K: Into<UserKey>, V: Into<UserValue>>(
        user_key: K,
        value: V,
        seqno: SeqNo,
        value_type: ValueType,
    ) -> Self {
        let key = InternalKey::new(user_key, seqno, value_type);
        Self::new(key, value)
    }

    /// Creates a new tombstone.
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16.
    pub fn new_tombstone<K: Into<UserKey>>(key: K, seqno: u64) -> Self {
        let key = InternalKey::new(key, seqno, ValueType::Tombstone);
        Self::new(key, vec![])
    }

    /// Creates a new weak tombstone.
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16.
    pub fn new_weak_tombstone<K: Into<UserKey>>(key: K, seqno: u64) -> Self {
        let key = InternalKey::new(key, seqno, ValueType::WeakTombstone);
        Self::new(key, vec![])
    }

    #[doc(hidden)]
    #[must_use]
    pub fn is_tombstone(&self) -> bool {
        self.key.is_tombstone()
    }
}

impl PartialEq for InternalValue {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl PartialOrd for InternalValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

// Order by user key, THEN by sequence number
// This is one of the most important functions
// Otherwise queries will not match expected behaviour
impl Ord for InternalValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl ItemSize for InternalValue {
    fn size(&self) -> usize {
        std::mem::size_of::<SeqNo>()
            + std::mem::size_of::<ValueType>()
            + self.key.user_key.len()
            + self.value.len()
    }
}

impl std::fmt::Debug for InternalValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} => {:?}",
            self.key,
            if self.value.len() >= 64 {
                format!("[ ... {} bytes ]", self.value.len())
            } else {
                format!("{:?}", self.value)
            }
        )
    }
}

// TODO: 3.0.0 remove
impl Encode for InternalValue {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        self.key.encode_into(writer)?;

        // NOTE: Only write value len + value if we are actually a value
        if !self.is_tombstone() {
            // NOTE: We know values are limited to 32-bit length
            #[allow(clippy::cast_possible_truncation)]
            writer.write_u32_varint(self.value.len() as u32)?;
            writer.write_all(&self.value)?;
        }

        Ok(())
    }
}

// TODO: 3.0.0 remove
impl Decode for InternalValue {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let key = InternalKey::decode_from(reader)?;

        if key.is_tombstone() {
            Ok(Self {
                key,
                value: UserValue::empty(),
            })
        } else {
            // NOTE: Only read value if we are actually a value

            let value_len = reader.read_u32_varint()?;
            let value = UserValue::from_reader(reader, value_len as usize)?;

            Ok(Self { key, value })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use test_log::test;

    #[test]
    fn pik_cmp_user_key() {
        let a = InternalKey::new(*b"a", 0, ValueType::Value);
        let b = InternalKey::new(*b"b", 0, ValueType::Value);
        assert!(a < b);
    }

    #[test]
    fn pik_cmp_seqno() {
        let a = InternalKey::new(*b"a", 0, ValueType::Value);
        let b = InternalKey::new(*b"a", 1, ValueType::Value);
        assert!(a > b);
    }

    #[test]
    fn value_raw() -> crate::Result<()> {
        // Create an empty Value instance
        let value =
            InternalValue::from_components(vec![1, 2, 3], vec![3, 2, 1], 1, ValueType::Value);

        #[rustfmt::skip]
        let bytes = [
            // Seqno
            1,
            
            // Type
            0,

            // User key
            3, 1, 2, 3,
            
            // User value
            3, 3, 2, 1,
        ];

        // Deserialize the empty Value
        let deserialized = InternalValue::decode_from(&mut Cursor::new(bytes))?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(value, deserialized);

        Ok(())
    }

    #[test]
    fn value_empty_value() -> crate::Result<()> {
        // Create an empty Value instance
        let value = InternalValue::from_components(vec![1, 2, 3], vec![], 42, ValueType::Value);

        // Serialize the empty Value
        let mut serialized = Vec::new();
        value.encode_into(&mut serialized)?;

        // Deserialize the empty Value
        let deserialized = InternalValue::decode_from(&mut &serialized[..])?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(value, deserialized);

        Ok(())
    }

    #[test]
    fn value_with_value() -> crate::Result<()> {
        // Create an empty Value instance
        let value = InternalValue::from_components(
            vec![1, 2, 3],
            vec![6, 2, 6, 2, 7, 5, 7, 8, 98],
            42,
            ValueType::Value,
        );

        // Serialize the empty Value
        let mut serialized = Vec::new();
        value.encode_into(&mut serialized)?;

        // Deserialize the empty Value
        let deserialized = InternalValue::decode_from(&mut &serialized[..])?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(value, deserialized);

        Ok(())
    }
}
