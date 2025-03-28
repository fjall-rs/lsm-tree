// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    SeqNo, UserKey, ValueType,
};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::{
    cmp::Reverse,
    io::{Read, Write},
};
use value_log::Slice;
use varint_rs::{VarintReader, VarintWriter};

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
            "keys can be 65535 bytes in length",
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

impl Encode for InternalKey {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        writer.write_u64_varint(self.seqno)?;

        writer.write_u8(u8::from(self.value_type))?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16_varint(self.user_key.len() as u16)?;
        writer.write_all(&self.user_key)?;

        Ok(())
    }
}

impl Decode for InternalKey {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let seqno = reader.read_u64_varint()?;

        let value_type = reader.read_u8()?;
        let value_type = value_type
            .try_into()
            .map_err(|()| DecodeError::InvalidTag(("ValueType", value_type)))?;

        let key_len = reader.read_u16_varint()?;
        let key = UserKey::from_reader(reader, key_len.into())?;

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

// TODO: wait for new crossbeam-skiplist
// TODO: https://github.com/crossbeam-rs/crossbeam/pull/1162
//
// impl Equivalent<InternalKeyRef<'_>> for InternalKey {
//     fn equivalent(&self, other: &InternalKeyRef<'_>) -> bool {
//         self.user_key == other.user_key && self.seqno == other.seqno
//     }
// }

// impl Comparable<InternalKeyRef<'_>> for InternalKey {
//     fn compare(&self, other: &InternalKeyRef<'_>) -> std::cmp::Ordering {
//         (&*self.user_key, Reverse(self.seqno)).cmp(&(other.user_key, Reverse(other.seqno)))
//     }
// }

// Temporary internal key without heap allocation
// #[derive(Debug, Eq)]
// pub struct InternalKeyRef<'a> {
//     pub user_key: &'a [u8],
//     pub seqno: SeqNo,
//     pub value_type: ValueType,
// }

// impl<'a> InternalKeyRef<'a> {
//     // Constructor for InternalKeyRef
//     pub fn new(user_key: &'a [u8], seqno: u64, value_type: ValueType) -> Self {
//         InternalKeyRef {
//             user_key,
//             seqno,
//             value_type,
//         }
//     }
// }

// impl<'a> PartialEq for InternalKeyRef<'a> {
//     fn eq(&self, other: &Self) -> bool {
//         self.user_key == other.user_key && self.seqno == other.seqno
//     }
// }

// impl<'a> PartialOrd for InternalKeyRef<'a> {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl<'a> Ord for InternalKeyRef<'a> {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         (&self.user_key, Reverse(self.seqno)).cmp(&(&other.user_key, Reverse(other.seqno)))
//     }
// }
