// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    blob_tree::handle::BlobIndirection, coding::Decode, compaction::stream::DroppedKvCallback,
    version::BlobFileList, vlog::BlobFileId,
};

/// Tracks fragmentation information in a blob file
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FragmentationEntry {
    /// Number of unreferenced (garbage) blobs
    pub(crate) len: usize,

    /// Unreferenced (garbage) blob bytes that could be freed (compressed)
    pub(crate) bytes: u64,

    /// Unreferenced (garbage) blob bytes that could be freed from disk
    pub(crate) on_disk_bytes: u64,
}

impl FragmentationEntry {
    #[must_use]
    pub fn new(len: usize, bytes: u64, on_disk_bytes: u64) -> Self {
        Self {
            len,
            bytes,
            on_disk_bytes,
        }
    }
}

/// Tracks fragmentation information in a value log (list of blob files)
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FragmentationMap(crate::HashMap<BlobFileId, FragmentationEntry>);

impl std::ops::Deref for FragmentationMap {
    type Target = crate::HashMap<BlobFileId, FragmentationEntry>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for FragmentationMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FragmentationMap {
    /// Returns the number of bytes that could be freed from disk.
    #[must_use]
    pub fn stale_bytes(&self) -> u64 {
        self.0.values().map(|x| x.on_disk_bytes).sum()
    }

    /// Removes blob file entries that are not part of the value log (anymore)
    /// to reduce linear memory growth.
    pub fn prune(&mut self, value_log: &BlobFileList) {
        self.0.retain(|&k, _| value_log.contains_key(k));
    }

    /// Merges a fragmentation map into another.
    ///
    /// This is used after a compaction stream is summed up (using the expiration callback), to apply
    /// the diff to the tree's fragmentation stats.
    pub fn merge_into(self, other: &mut Self) {
        for (blob_file_id, diff) in self.0 {
            other
                .0
                .entry(blob_file_id)
                .and_modify(|counter| {
                    counter.bytes += diff.bytes;
                    counter.len += diff.len;
                    counter.on_disk_bytes += diff.on_disk_bytes;
                })
                .or_insert(diff);
        }
    }
}

impl crate::coding::Encode for FragmentationMap {
    fn encode_into<W: std::io::Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        use byteorder::{WriteBytesExt, LE};

        #[expect(
            clippy::cast_possible_truncation,
            reason = "there are always less than 4 billion blob files"
        )]
        writer.write_u32::<LE>(self.len() as u32)?;

        for (blob_file_id, item) in self.iter() {
            writer.write_u64::<LE>(*blob_file_id)?;

            #[expect(
                clippy::cast_possible_truncation,
                reason = "there are always less than 4 billion blobs in a blob file"
            )]
            writer.write_u32::<LE>(item.len as u32)?;

            writer.write_u64::<LE>(item.bytes)?;

            writer.write_u64::<LE>(item.on_disk_bytes)?;
        }

        Ok(())
    }
}

impl crate::coding::Decode for FragmentationMap {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Result<Self, crate::Error>
    where
        Self: Sized,
    {
        use byteorder::{ReadBytesExt, LE};

        let len = reader.read_u32::<LE>()?;
        let mut map =
            crate::HashMap::with_capacity_and_hasher(len as usize, rustc_hash::FxBuildHasher);

        for _ in 0..len {
            let id = reader.read_u64::<LE>()?;
            let len = reader.read_u32::<LE>()? as usize;
            let bytes = reader.read_u64::<LE>()?;
            let on_disk_bytes = reader.read_u64::<LE>()?;
            map.insert(id, FragmentationEntry::new(len, bytes, on_disk_bytes));
        }

        Ok(Self(map))
    }
}

impl DroppedKvCallback for FragmentationMap {
    fn on_dropped(&mut self, kv: &crate::InternalValue) {
        if kv.key.value_type.is_indirection() {
            let mut reader = &kv.value[..];

            #[expect(
                clippy::expect_used,
                reason = "data is read and checked for corruption, so we expect to be able to deserialize BlobIndirection fine"
            )]
            let vptr =
                BlobIndirection::decode_from(&mut reader).expect("should parse BlobIndirection");

            let size = u64::from(vptr.size);
            let on_disk_size = u64::from(vptr.vhandle.on_disk_size);

            self.0
                .entry(vptr.vhandle.blob_file_id)
                .and_modify(|counter| {
                    counter.len += 1;
                    counter.bytes += size;
                    counter.on_disk_bytes += on_disk_size;
                })
                .or_insert_with(|| FragmentationEntry {
                    bytes: size,
                    on_disk_bytes: on_disk_size,
                    len: 1,
                });
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::{
        coding::{Decode, Encode},
        compaction::stream::CompactionStream,
        value::InternalValue,
        vlog::ValueHandle,
        ValueType,
    };
    use std::collections::HashMap;
    use test_log::test;

    #[test]
    fn frag_map_merge_into() {
        let mut map = FragmentationMap(HashMap::default());
        map.0.insert(
            0,
            FragmentationEntry {
                len: 1,
                bytes: 1_000,
                on_disk_bytes: 500,
            },
        );
        map.0.insert(
            1,
            FragmentationEntry {
                len: 2,
                bytes: 2_000,
                on_disk_bytes: 1_000,
            },
        );

        // test merge_into
        let mut diff = FragmentationMap(HashMap::default());
        diff.0.insert(
            0,
            FragmentationEntry {
                len: 3,
                bytes: 3_000,
                on_disk_bytes: 1_500,
            },
        );
        diff.0.insert(
            3,
            FragmentationEntry {
                len: 4,
                bytes: 4_000,
                on_disk_bytes: 2_000,
            },
        );

        diff.merge_into(&mut map);

        assert_eq!(map.0.len(), 3);
        assert_eq!(map.0[&0].len, 4);
        assert_eq!(map.0[&0].bytes, 4_000);
        assert_eq!(map.0[&0].on_disk_bytes, 2_000);
        assert_eq!(map.0[&1].len, 2);
        assert_eq!(map.0[&1].bytes, 2_000);
        assert_eq!(map.0[&1].on_disk_bytes, 1_000);
        assert_eq!(map.0[&3].len, 4);
        assert_eq!(map.0[&3].bytes, 4_000);
        assert_eq!(map.0[&3].on_disk_bytes, 2_000);
    }

    #[test]
    fn frag_map_roundtrip() {
        let map = FragmentationMap({
            let mut map = HashMap::default();
            map.insert(
                0,
                FragmentationEntry {
                    len: 1,
                    bytes: 1_000,
                    on_disk_bytes: 500,
                },
            );
            map.insert(
                1,
                FragmentationEntry {
                    len: 2,
                    bytes: 2_000,
                    on_disk_bytes: 1_000,
                },
            );
            map
        });

        let encoded = map.encode_into_vec();
        let decoded = FragmentationMap::decode_from(&mut &encoded[..]).expect("should decode map");
        assert_eq!(map, decoded);
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_gc_count_drops() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = &[
            InternalValue::from_components("a", b"abc", 1, ValueType::Value),

            InternalValue::from_components("a", BlobIndirection {
              size: 1000,
              vhandle: ValueHandle {
                blob_file_id: 0,
                on_disk_size: 500,
                offset: 0,
              }
            }.encode_into_vec(), 0, ValueType::Indirection),
        ];

        let mut my_watcher = FragmentationMap::default();

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_expiration_callback(&mut my_watcher);

        assert_eq!(
            // TODO: Seqno is normally reset to 0
            InternalValue::from_components(*b"a", b"abc", 1, ValueType::Value),
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
                        on_disk_bytes: 500,
                    },
                );
                map
            },
            my_watcher.0,
        );

        Ok(())
    }
}
