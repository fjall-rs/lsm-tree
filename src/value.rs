use crate::{
    serde::{Deserializable, DeserializeError, Serializable, SerializeError},
    Slice,
};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::{
    cmp::Reverse,
    io::{Read, Write},
};
use varint_rs::{VarintReader, VarintWriter};

/// User defined key
pub type UserKey = Slice;

/// User defined data (blob of bytes)
#[allow(clippy::module_name_repetitions)]
pub type UserValue = Slice;

/// Key-value pair (tuple)
pub type KvPair = (crate::UserKey, crate::UserValue);

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

impl From<u8> for ValueType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Value,
            _ => Self::Tombstone,
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

#[derive(Clone, PartialEq, Eq)]
pub struct ParsedInternalKey {
    pub user_key: UserKey,
    pub seqno: SeqNo,
    pub value_type: ValueType,
}

impl std::fmt::Debug for ParsedInternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{}:{}",
            self.user_key,
            self.seqno,
            match self.value_type {
                ValueType::Value => "V",
                ValueType::Tombstone => "T",
                ValueType::WeakTombstone => "wT",
            },
        )
    }
}

impl ParsedInternalKey {
    pub fn new<K: Into<UserKey>>(user_key: K, seqno: SeqNo, value_type: ValueType) -> Self {
        let user_key = user_key.into();

        assert!(
            user_key.len() <= u16::MAX.into(),
            "keys can be 65535 bytes in length"
        );

        Self {
            user_key,
            seqno,
            value_type,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value_type == ValueType::Tombstone || self.value_type == ValueType::WeakTombstone
    }
}

impl PartialOrd for ParsedInternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Order by user key, THEN by sequence number
// This is one of the most important functions
// Otherwise queries will not match expected behaviour
impl Ord for ParsedInternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.user_key, Reverse(self.seqno)).cmp(&(&other.user_key, Reverse(other.seqno)))
    }
}

/// Internal representation of KV pairs
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Eq, PartialEq)]
pub struct InternalValue {
    /// Internal key
    pub key: ParsedInternalKey,

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
    pub fn new<V: Into<UserValue>>(key: ParsedInternalKey, value: V) -> Self {
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
        let key = ParsedInternalKey::new(user_key, seqno, value_type);
        Self::new(key, value)
    }

    /// Creates a new tombstone.
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16.
    pub fn new_tombstone<K: Into<UserKey>>(key: K, seqno: u64) -> Self {
        let key = key.into();
        let key = ParsedInternalKey::new(key, seqno, ValueType::Tombstone);
        Self::new(key, vec![])
    }

    /// Creates a new weak tombstone.
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16.
    pub fn new_weak_tombstone<K: Into<UserKey>>(key: K, seqno: u64) -> Self {
        let key = key.into();
        let key = ParsedInternalKey::new(key, seqno, ValueType::WeakTombstone);
        Self::new(key, vec![])
    }

    #[doc(hidden)]
    #[must_use]
    pub fn size(&self) -> usize {
        let key_size = self.key.user_key.len();
        let value_size = self.value.len();
        std::mem::size_of::<Self>() + key_size + value_size
    }

    #[doc(hidden)]
    #[must_use]
    pub fn is_tombstone(&self) -> bool {
        self.key.is_tombstone()
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

impl Serializable for ParsedInternalKey {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16_varint(self.user_key.len() as u16)?;
        writer.write_all(&self.user_key)?;

        writer.write_u64_varint(self.seqno)?;
        writer.write_u8(u8::from(self.value_type))?;

        Ok(())
    }
}

impl Deserializable for ParsedInternalKey {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let key_len = reader.read_u16_varint()?;
        let mut key = vec![0; key_len.into()];
        reader.read_exact(&mut key)?;

        let seqno = reader.read_u64_varint()?;
        let value_type = reader.read_u8()?.into();

        Ok(Self::new(key, seqno, value_type))
    }
}

impl Serializable for InternalValue {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        self.key.serialize(writer)?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u32_varint(self.value.len() as u32)?;
        writer.write_all(&self.value)?;

        Ok(())
    }
}

impl Deserializable for InternalValue {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let key = ParsedInternalKey::deserialize(reader)?;

        let value_len = reader.read_u32_varint()?;
        let mut value = vec![0; value_len as usize];
        reader.read_exact(&mut value)?;

        Ok(Self {
            key,
            value: value.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use test_log::test;

    #[test]
    fn pik_cmp_user_key() {
        let a = ParsedInternalKey::new(*b"a", 0, ValueType::Value);
        let b = ParsedInternalKey::new(*b"b", 0, ValueType::Value);
        assert!(a < b);
    }

    #[test]
    fn pik_cmp_seqno() {
        let a = ParsedInternalKey::new(*b"a", 0, ValueType::Value);
        let b = ParsedInternalKey::new(*b"a", 1, ValueType::Value);
        assert!(a > b);
    }

    #[test]
    fn value_raw() -> crate::Result<()> {
        // Create an empty Value instance
        let value =
            InternalValue::from_components(vec![1, 2, 3], vec![3, 2, 1], 1, ValueType::Value);

        #[rustfmt::skip]
        let  bytes = &[
            // Key
            3, 1, 2, 3,

            // Seqno
            1,
            
            // Type
            0,
            
            // Value
            3, 3, 2, 1,
        ];

        // Deserialize the empty Value
        let deserialized = InternalValue::deserialize(&mut Cursor::new(bytes))?;

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
        value.serialize(&mut serialized)?;

        // Deserialize the empty Value
        let deserialized = InternalValue::deserialize(&mut &serialized[..])?;

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
        value.serialize(&mut serialized)?;

        // Deserialize the empty Value
        let deserialized = InternalValue::deserialize(&mut &serialized[..])?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(value, deserialized);

        Ok(())
    }
}
