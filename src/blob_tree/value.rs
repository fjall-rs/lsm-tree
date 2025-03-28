// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Write};
use value_log::{Slice, UserValue, ValueHandle};
use varint_rs::{VarintReader, VarintWriter};

/// A value which may or may not be inlined into an index tree
///
/// If not inlined, the value is present in the value log, so it needs
/// to be fetched using the given value handle.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum MaybeInlineValue {
    /// Inlined value (classic LSM-tree)
    Inline(UserValue),

    /// The value is a handle (pointer) into the value log
    Indirect { vhandle: ValueHandle, size: u32 },
}

const TAG_INLINE: u8 = 0;
const TAG_INDIRECT: u8 = 1;

impl MaybeInlineValue {
    pub fn from_slice(bytes: &Slice) -> Result<Self, DecodeError> {
        let mut cursor = Cursor::new(&**bytes);

        match cursor.read_u8()? {
            TAG_INLINE => {
                // NOTE: Truncation is OK because we are only at the first couple
                // of bytes of the slice
                #[allow(clippy::cast_possible_truncation)]
                let size_len = {
                    let pos_before = cursor.position() as usize;
                    let _ = cursor.read_u32_varint()?;
                    let pos_after = cursor.position() as usize;
                    pos_after - pos_before
                };
                let slice = bytes.slice((1 + size_len)..);
                Ok(Self::Inline(slice))
            }
            TAG_INDIRECT => {
                let mut reader = &**bytes;
                Self::decode_from(&mut reader)
            }
            x => Err(DecodeError::InvalidTag(("MaybeInlineValue", x))),
        }
    }
}

impl Encode for MaybeInlineValue {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        match self {
            Self::Inline(bytes) => {
                writer.write_u8(TAG_INLINE)?;

                // NOTE: Values can be up to 2^32 bytes
                #[allow(clippy::cast_possible_truncation)]
                writer.write_u32_varint(bytes.len() as u32)?;

                writer.write_all(bytes)?;
            }
            Self::Indirect { vhandle, size } => {
                writer.write_u8(TAG_INDIRECT)?;
                vhandle.encode_into(writer)?;
                writer.write_u32_varint(*size)?;
            }
        }
        Ok(())
    }
}

impl Decode for MaybeInlineValue {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let tag = reader.read_u8()?;

        match tag {
            TAG_INLINE => {
                let len = reader.read_u32_varint()? as usize;
                let slice = UserValue::from_reader(reader, len)?;
                Ok(Self::Inline(slice))
            }
            TAG_INDIRECT => {
                let vhandle = ValueHandle::decode_from(reader)?;
                let size = reader.read_u32_varint()?;
                Ok(Self::Indirect { vhandle, size })
            }
            x => Err(DecodeError::InvalidTag(("MaybeInlineValue", x))),
        }
    }
}
