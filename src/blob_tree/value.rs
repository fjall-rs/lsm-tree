use crate::{
    serde::Deserializable, serde::Serializable, DeserializeError, SerializeError, UserValue,
};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::{
    io::{Read, Write},
    sync::Arc,
};
use value_log::ValueHandle;
use varint_rs::{VarintReader, VarintWriter};

/// A value which may or may not be inlined into an index tree
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum MaybeInlineValue {
    /// Inlined value (classic LSM-tree)
    Inline(UserValue),

    /// The value is a handle (pointer) into the value log
    Indirect { handle: ValueHandle, size: u32 },
}

impl Serializable for ValueHandle {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_u64_varint(self.offset)?;
        writer.write_u64_varint(self.segment_id)?;
        Ok(())
    }
}

impl Deserializable for ValueHandle {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let offset = reader.read_u64_varint()?;
        let segment_id = reader.read_u64_varint()?;
        Ok(Self { segment_id, offset })
    }
}

const TAG_INLINE: u8 = 0;
const TAG_INDIRECT: u8 = 1;

impl Serializable for MaybeInlineValue {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        match self {
            Self::Inline(bytes) => {
                writer.write_u8(TAG_INLINE)?;

                // NOTE: Values can be up to 2^32 bytes
                #[allow(clippy::cast_possible_truncation)]
                writer.write_u32_varint(bytes.len() as u32)?;

                writer.write_all(bytes)?;
            }
            Self::Indirect { handle, size } => {
                writer.write_u8(TAG_INDIRECT)?;
                handle.serialize(writer)?;
                writer.write_u32_varint(*size)?;
            }
        }
        Ok(())
    }
}

impl Deserializable for MaybeInlineValue {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let tag = reader.read_u8()?;

        match tag {
            TAG_INLINE => {
                let len = reader.read_u32_varint()? as usize;
                let mut bytes = vec![0; len];
                reader.read_exact(&mut bytes)?;
                Ok(Self::Inline(Arc::from(bytes)))
            }
            TAG_INDIRECT => {
                let handle = ValueHandle::deserialize(reader)?;
                let size = reader.read_u32_varint()?;
                Ok(Self::Indirect { handle, size })
            }
            x => Err(DeserializeError::InvalidTag(("MaybeInlineValue", x))),
        }
    }
}
