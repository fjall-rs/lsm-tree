use crate::{
    serde::Deserializable, serde::Serializable, DeserializeError, SerializeError, UserValue,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Read, Write},
    sync::Arc,
};
use value_log::ValueHandle;

/// A value which may or may not be inlined into an index tree
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum MaybeInlineValue {
    /// Inlined value (classic LSM-tree)
    Inline(UserValue),

    /// The value is a handle (pointer) into the value log
    Indirect(ValueHandle),
}

impl Serializable for ValueHandle {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_u64::<BigEndian>(self.offset)?;
        writer.write_u64::<BigEndian>(self.segment_id)?;
        Ok(())
    }
}

impl Deserializable for ValueHandle {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let offset = reader.read_u64::<BigEndian>()?;
        let segment_id = reader.read_u64::<BigEndian>()?;

        Ok(Self { segment_id, offset })
    }
}

impl Serializable for MaybeInlineValue {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        match self {
            Self::Inline(bytes) => {
                writer.write_u8(0)?;
                writer.write_u64::<BigEndian>(bytes.len() as u64)?;
                writer.write_all(bytes)?;
            }
            Self::Indirect(value_handle) => {
                writer.write_u8(1)?;
                value_handle.serialize(writer)?;
            }
        }
        Ok(())
    }
}

impl Deserializable for MaybeInlineValue {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let tag = reader.read_u8()?;

        match tag {
            0 => {
                let len = reader.read_u64::<BigEndian>()? as usize;
                let mut bytes = vec![0; len];
                reader.read_exact(&mut bytes)?;

                Ok(Self::Inline(Arc::from(bytes)))
            }
            1 => {
                let handle = ValueHandle::deserialize(reader)?;
                Ok(Self::Indirect(handle))
            }
            x => Err(DeserializeError::InvalidTag(("MaybeInlineValue", x))),
        }
    }
}
