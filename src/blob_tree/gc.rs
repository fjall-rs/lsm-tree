// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    blob_tree::value::{MaybeInlineValue, TAG_INDIRECT},
    compaction::stream::ExpiredKvCallback,
    vlog::BlobFileId,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FragmentationEntry {
    /// Number of unreferenced (garbage) blobs
    pub(crate) len: usize,

    /// Unreferenced (garbage) blob bytes that could be freed
    pub(crate) bytes: u64,
}

impl FragmentationEntry {
    #[must_use]
    pub fn new(len: usize, bytes: u64) -> Self {
        Self { len, bytes }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FragmentationMap(crate::HashMap<BlobFileId, FragmentationEntry>);

impl std::ops::Deref for FragmentationMap {
    type Target = crate::HashMap<BlobFileId, FragmentationEntry>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FragmentationMap {
    // TODO: unit test
    pub fn merge_into(self, other: &mut Self) {
        for (blob_file_id, diff) in self.0 {
            other
                .0
                .entry(blob_file_id)
                .and_modify(|counter| {
                    counter.bytes += diff.bytes;
                    counter.len += diff.len;
                })
                .or_insert(diff);
        }
    }
}

impl crate::coding::Encode for FragmentationMap {
    fn encode_into<W: std::io::Write>(&self, writer: &mut W) -> Result<(), crate::EncodeError> {
        use byteorder::{WriteBytesExt, LE};

        // NOTE: We know there are always less than 4 billion blob files
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u32::<LE>(self.len() as u32)?;

        for (blob_file_id, item) in self.iter() {
            writer.write_u64::<LE>(*blob_file_id)?;

            // NOTE: We know there are always less than 4 billion blobs in a blob file
            #[allow(clippy::cast_possible_truncation)]
            writer.write_u32::<LE>(item.len as u32)?;

            writer.write_u64::<LE>(item.bytes)?;
        }

        Ok(())
    }
}

impl crate::coding::Decode for FragmentationMap {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Result<Self, crate::DecodeError>
    where
        Self: Sized,
    {
        use byteorder::{ReadBytesExt, LE};

        let len = reader.read_u32::<LE>()?;
        let mut map =
            crate::HashMap::with_capacity_and_hasher(len as usize, rustc_hash::FxBuildHasher);

        for _ in 0..len {
            let id = reader.read_u64::<LE>()?;

            // NOTE: We know there are always less than 4 billion blobs in a blob file
            #[allow(clippy::cast_possible_truncation)]
            let len = reader.read_u32::<LE>()? as usize;

            let bytes = reader.read_u64::<LE>()?;

            map.insert(id, FragmentationEntry::new(len, bytes));
        }

        Ok(Self(map))
    }
}

impl ExpiredKvCallback for FragmentationMap {
    fn on_expired(&mut self, kv: &crate::InternalValue) {
        if kv.key.is_tombstone() {
            return;
        }

        let tag = *kv.value.first().expect("value should not be empty");

        if tag == TAG_INDIRECT {
            let parsed_indirection =
                MaybeInlineValue::from_slice(&kv.value).expect("should parse MaybeInlineValue");

            match parsed_indirection {
                MaybeInlineValue::Indirect { vhandle, size } => {
                    let size = u64::from(size);

                    self.0
                        .entry(vhandle.blob_file_id)
                        .and_modify(|counter| {
                            counter.len += 1;
                            counter.bytes += size;
                        })
                        .or_insert_with(|| FragmentationEntry {
                            bytes: size,
                            len: 1,
                        });
                }
                // NOTE: Unreachable because we check for the tag above
                MaybeInlineValue::Inline(_) => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        coding::{Decode, Encode},
        compaction::stream::CompactionStream,
        value::{InternalValue, ValueType},
        vlog::ValueHandle,
    };
    use std::collections::HashMap;
    use test_log::test;

    /// Tests encoding and decoding traits
    #[test]
    fn frag_map_roundtrip() {
        let map = FragmentationMap({
            let mut map = HashMap::default();
            map.insert(
                0,
                FragmentationEntry {
                    len: 1,
                    bytes: 1_000,
                },
            );
            map.insert(
                1,
                FragmentationEntry {
                    len: 2,
                    bytes: 2_000,
                },
            );
            map
        });

        let encoded = map.encode_into_vec();
        let decoded = FragmentationMap::decode_from(&mut &encoded[..]).expect("should decode map");
        assert_eq!(map, decoded);
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn compaction_stream_gc_count_drops() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = &[
            InternalValue::from_components("a", b"abc", 1, ValueType::Value),

            InternalValue::from_components("a", MaybeInlineValue::Indirect {
              size: 1000,
              vhandle: ValueHandle {
                blob_file_id: 0,
                on_disk_size: 500,
                offset: 0,
              }
            }.encode_into_vec(), 0, ValueType::Value),
        ];

        let mut my_watcher = FragmentationMap::default();

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_expiration_callback(&mut my_watcher);

        assert_eq!(
            // Seqno is reset to 0
            InternalValue::from_components(*b"a", b"abc", 0, ValueType::Value),
            iter.next().unwrap()?,
        );

        assert_eq!(
            {
                let mut map = HashMap::default();
                map.insert(
                    0,
                    FragmentationEntry {
                        len: 1,
                        bytes: 1_000,
                    },
                );
                map
            },
            my_watcher.0,
        );

        Ok(())
    }
}
