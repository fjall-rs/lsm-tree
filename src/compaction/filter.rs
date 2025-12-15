//! Compaction filters

use std::path::Path;

use crate::{
    coding::Decode,
    compaction::{stream::StreamFilter, worker::Options},
    version::Version,
    vlog::Accessor,
    BlobIndirection, InternalValue, Slice,
};

/// Verdict returned by a [`CompactionFilter`].
pub enum FilterVerdict {
    /// Keep the item.
    Keep,
    /// Delete the item.
    Drop,
}

/// Trait for compaction filter objects.
pub trait CompactionFilter {
    #[allow(clippy::doc_markdown, reason = "thinks RocksDB is a Rust type")]
    /// Returns whether an item should be kept during compaction.
    /* TODO: perhaps prevented by super versions? check this
    ///
    /// # Warning!
    ///
    /// Compaction filters ignore transactions. Any item filtered out (deleted)
    /// by a compaction filter will immediately stop existing for all readers,
    /// even those in a snapshot which would otherwise expect the item to still
    /// exist. This mirrors the behavior of RocksDB since 6.0.
    // note: for rocksdb behavior, see
    // <https://github.com/facebook/rocksdb/wiki/Compaction-Filter>
     */
    ///
    /// # Errors
    ///
    /// If the filter errors, it should return [`FilterVerdict::Keep`].
    fn filter_item(&mut self, item: ItemAccessor<'_>) -> FilterVerdict;
}

/// Accessor for the key/value from a compaction filter.
pub struct ItemAccessor<'a> {
    pub(crate) item: &'a InternalValue,
    pub(crate) opts: &'a Options,
    pub(crate) version: &'a Version,
    pub(crate) blobs_folder: &'a Path,
}

impl<'a> ItemAccessor<'a> {
    /// Get the key of this item
    #[must_use]
    pub fn key(&self) -> &'a Slice {
        &self.item.key.user_key
    }

    /// Returns whether this item's value is stored separately.
    #[must_use]
    pub fn is_indirection(&self) -> bool {
        self.item.key.value_type.is_indirection()
    }

    /// Get the value of this item
    ///
    /// # Errors
    ///
    /// This method will return an error if blob retrieval fails.
    pub fn value(&self) -> crate::Result<Slice> {
        match self.item.key.value_type {
            crate::ValueType::Value => Ok(self.item.value.clone()),
            crate::ValueType::Tombstone => {
                // resolve and read the value from a blob
                let mut reader = &self.item.value[..];
                let indirection = BlobIndirection::decode_from(&mut reader)?;
                let vhandle = indirection.vhandle;
                let accessor = Accessor::new(&self.version.blob_files);

                let value = accessor.get(
                    self.opts.tree_id,
                    self.blobs_folder,
                    &self.item.key.user_key,
                    &vhandle,
                    &self.opts.config.cache,
                    &self.opts.config.descriptor_table,
                )?;

                if let Some(value) = value {
                    Ok(value)
                } else {
                    log::error!(
                        "failed to read referenced blob file during execution of compaction filter. key: {:?}, vptr: {:?}",
                        self.item.key, indirection
                    );
                    Err(crate::Error::Unrecoverable)
                }
            }
            crate::ValueType::WeakTombstone | crate::ValueType::Indirection => {
                unreachable!("tombstones are filtered out before calling filter")
            }
        }
    }
}

/// Adapts a [`CompactionFilter`] to a [`StreamFilter`].
// note: this slightly helps insulate CompactionStream from lifetime spam
pub(crate) struct StreamFilterAdapter<'a, 'b: 'a> {
    pub filter: Option<&'a mut (dyn CompactionFilter + 'b)>,
    pub opts: &'a Options,
    pub version: &'a Version,
    pub blobs_folder: &'a Path,
}

impl<'a, 'b: 'a> StreamFilter for StreamFilterAdapter<'a, 'b> {
    fn should_remove(&mut self, item: &InternalValue) -> bool {
        let Some(filter) = self.filter.as_mut() else {
            return false;
        };
        matches!(
            filter.filter_item(ItemAccessor {
                item,
                opts: self.opts,
                version: self.version,
                blobs_folder: self.blobs_folder,
            }),
            FilterVerdict::Drop,
        )
    }
}
