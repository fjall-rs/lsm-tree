// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    serde::{Deserializable, DeserializeError, Serializable, SerializeError},
    SeqNo, UserKey, ValueType,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    cmp::Reverse,
    io::{Read, Write},
};

#[derive(Clone, PartialEq, Eq)]
#[allow(clippy::module_name_repetitions)]
pub struct InternalKey {
    pub user_key: UserKey,
    pub seqno: SeqNo,
    pub value_type: ValueType,
}

impl std::fmt::Debug for InternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{}:{}",
            self.user_key,
            self.seqno,
            match self.value_type {
                ValueType::Value => "V",
                ValueType::Tombstone => "T",
                ValueType::WeakTombstone => "W",
            },
        )
    }
}

impl InternalKey {
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

impl Serializable for InternalKey {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16::<BigEndian>(self.user_key.len() as u16)?;
        writer.write_all(&self.user_key)?;

        writer.write_u64::<BigEndian>(self.seqno)?;
        writer.write_u8(u8::from(self.value_type))?;

        Ok(())
    }
}

impl Deserializable for InternalKey {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let key_len = reader.read_u16::<BigEndian>()?;
        let mut key = vec![0; key_len.into()];
        reader.read_exact(&mut key)?;

        let seqno = reader.read_u64::<BigEndian>()?;
        let value_type = reader.read_u8()?.into();

        Ok(Self::new(key, seqno, value_type))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Order by user key, THEN by sequence number
// This is one of the most important functions
// Otherwise queries will not match expected behaviour
impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.user_key, Reverse(self.seqno)).cmp(&(&other.user_key, Reverse(other.seqno)))
    }
}
