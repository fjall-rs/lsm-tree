pub mod index;
pub mod value;

use crate::{
    file::BLOBS_FOLDER,
    r#abstract::AbstractTree,
    serde::{Deserializable, Serializable},
    Config, SeqNo,
};
use index::IndexTree;
use std::{io::Cursor, ops::RangeBounds, sync::Arc};
use value_log::ValueLog;

use self::value::MaybeInlineValue;

/// A key-value separated log-structured merge tree
///
/// The tree consists of an index tree (LSM-tree) and a log-structured value log
/// to reduce write amplification.
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
