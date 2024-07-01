use crate::serde::{Deserializable, Serializable};
use crate::value::UserKey;
use crate::Slice;
use std::io::{Read, Write};
use varint_rs::{VarintReader, VarintWriter};

/// Points to a block on file
#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct KeyedBlockHandle {
    /// Key of last item in block
    pub end_key: UserKey,

    /// Position of block in file
    pub offset: u64,
}

impl PartialEq for KeyedBlockHandle {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}
impl Eq for KeyedBlockHandle {}

impl std::hash::Hash for KeyedBlockHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.offset);
    }
}

impl PartialOrd for KeyedBlockHandle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyedBlockHandle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.end_key, self.offset).cmp(&(&other.end_key, other.offset))
    }
}

impl Serializable for KeyedBlockHandle {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), crate::SerializeError> {
        writer.write_u64_varint(self.offset)?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16_varint(self.end_key.len() as u16)?;

        writer.write_all(&self.end_key)?;

        Ok(())
    }
}

impl Deserializable for KeyedBlockHandle {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, crate::DeserializeError>
    where
        Self: Sized,
    {
        let offset = reader.read_u64_varint()?;

        let key_len = reader.read_u16_varint()?;

        let mut key = vec![0; key_len.into()];
        reader.read_exact(&mut key)?;

        Ok(Self {
            offset,
            end_key: Slice::from(key),
        })
    }
}
