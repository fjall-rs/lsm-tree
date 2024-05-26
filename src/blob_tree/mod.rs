pub mod index;
pub mod value;

use self::value::MaybeInlineValue;
use crate::{
    file::BLOBS_FOLDER,
    r#abstract::AbstractTree,
    serde::{Deserializable, Serializable},
    Config, SeqNo,
};
use index::IndexTree;
use std::{io::Cursor, ops::RangeBounds, sync::Arc};
use value_log::{ValueHandle, ValueLog};

/// A key-value-separated log-structured merge tree
///
/// This tree is a composite structure, consisting of an
/// index tree (LSM-tree) and a log-structured value log
/// to reduce write amplification.
///
/// See <https://docs.rs/value-log> for more information.
pub struct BlobTree {
    /// Index tree that holds value handles or small inline values
    #[doc(hidden)]
    pub index: IndexTree,

    /// Log-structured value-log that stores large values
    blobs: ValueLog<IndexTree>,
}

impl BlobTree {
    pub fn open(config: Config) -> crate::Result<Self> {
        let path = &config.path;
        let vlog_path = path.join(BLOBS_FOLDER);

        let index: IndexTree = config.open()?.into();

        Ok(Self {
            index: index.clone(),
            blobs: ValueLog::open(vlog_path, value_log::Config::default(), index)?,
        })
    }

    fn resolve_value_handle(&self, item: RangeItem) -> RangeItem {
        match item {
            Ok((key, value)) => {
                let mut cursor = Cursor::new(value);
                let item = MaybeInlineValue::deserialize(&mut cursor).expect("should deserialize");

                match item {
                    MaybeInlineValue::Inline(bytes) => Ok((key, bytes)),
                    MaybeInlineValue::Indirect(handle) => match self.blobs.get(&handle) {
                        Ok(Some(bytes)) => Ok((key, bytes)),
                        Err(e) => Err(e.into()),
                        _ => panic!("Aahhhh"), // TODO:
                    },
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn flush_active_memtable(&self) -> crate::Result<()> {
        use crate::{
            file::SEGMENTS_FOLDER,
            segment::writer::{Options, Writer as SegmentWriter},
        };
        use value::MaybeInlineValue;

        log::debug!("flushing active memtable & performing key-value separation");

        let Some((segment_id, yanked_memtable)) = self.index.0.rotate_memtable() else {
            return Ok(());
        };

        let lsm_segment_folder = self.index.0.config.path.join(SEGMENTS_FOLDER);

        let mut segment_writer = SegmentWriter::new(Options {
            segment_id,
            block_size: self.index.0.config.inner.block_size,
            evict_tombstones: false,
            folder: lsm_segment_folder,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.0001,
        })?;
        let mut blob_writer = self.blobs.get_writer()?;

        let blob_id = blob_writer.segment_id();

        for entry in &yanked_memtable.items {
            let key = entry.key();

            let value = entry.value();
            let mut cursor = Cursor::new(value);
            let value = MaybeInlineValue::deserialize(&mut cursor).expect("oops");
            let MaybeInlineValue::Inline(value) = value else {
                panic!("values are initially always inlined");
            };

            let size = value.len();

            // TODO: blob threshold
            let value_wrapper = if size < 4_096 {
                MaybeInlineValue::Inline(value)
            } else {
                let offset = blob_writer.offset(&key.user_key);
                blob_writer.write(&key.user_key, &value)?;

                let value_handle = ValueHandle {
                    offset,
                    segment_id: blob_id,
                };
                MaybeInlineValue::Indirect(value_handle)
            };

            let mut serialized = vec![];
            value_wrapper
                .serialize(&mut serialized)
                .expect("should serialize");

            segment_writer.write(crate::Value::from(((key.clone()), serialized.into())))?;
        }

        self.blobs.register(blob_writer)?;
        self.index.0.consume_writer(segment_id, segment_writer)?;

        Ok(())
    }
}

type RangeItem = crate::Result<(crate::UserKey, crate::UserValue)>;

impl AbstractTree for BlobTree {
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(crate::UserKey, crate::UserValue)>> + '_
    {
        self.index
            .0
            .create_range(range, None, None)
            .map(|item| self.resolve_value_handle(item))
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(crate::UserKey, crate::UserValue)>> + '_
    {
        self.index
            .0
            .create_prefix(prefix, None, None)
            .map(|item| self.resolve_value_handle(item))
    }

    fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V, seqno: SeqNo) -> (u32, u32) {
        use value::MaybeInlineValue;

        // NOTE: Initially, we always write an inline value
        // On memtable flush, depending on the values' sizes, they will be separated
        // into inline or indirect values
        let item = MaybeInlineValue::Inline(value.as_ref().into());

        let mut value = vec![];
        item.serialize(&mut value).expect("should serialize");

        self.index.0.insert(key, value, seqno)
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<Arc<[u8]>>> {
        use value::MaybeInlineValue::{Indirect, Inline};

        let Some(value) = self.index.get_internal(key.as_ref())? else {
            return Ok(None);
        };

        match value {
            Inline(bytes) => Ok(Some(bytes)),
            Indirect(handle) => {
                // Resolve indirection using value log
                self.blobs.get(&handle).map_err(Into::into)
            }
        }
    }

    fn remove<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> (u32, u32) {
        self.index.0.remove(key, seqno)
    }
}
