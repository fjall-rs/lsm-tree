//! Definitions for compaction filters
//!
//! Compaction filters allow users to run custom logic during compactions, e.g. custom cleanup rules such as TTL.
//! Because compactions run in background workers, using compactions filters instead of scans can massively increase the efficiency of the storage engine.
//!
//! # Examples
//!
//! Defining a compaction filter to drop data using some TTL rule:
//!
//! ```
//! // TODO: add an example to drop old data using "TTL"
//! ```

use crate::{
    coding::{Decode, Encode},
    compaction::{
        stream::{StreamFilter, StreamFilterVerdict},
        worker::Options,
    },
    key::InternalKey,
    version::Version,
    vlog::{Accessor, BlobFileWriter, ValueHandle},
    BlobIndirection, InternalValue, KvSeparationOptions, UserKey, UserValue, ValueType,
};
use std::{panic::RefUnwindSafe, path::Path};

/// Verdict returned by a [`CompactionFilter`]
#[non_exhaustive]
#[derive(Debug)]
pub enum Verdict {
    /// Keeps the item.
    Keep,

    /// Removes the item.
    Remove,

    /// Removes the item and replace it with a weak tombstone.
    ///
    /// This may cause old versions of this item to be resurrected.
    /// The semantics of this operation are identical to [`remove_weak`](crate::AbstractTree::remove_weak).
    RemoveWeak,

    /// Replaces the value of the item.
    ReplaceValue(UserValue),
    /// Destroys a value - does not leave behind a tombstone.
    ///
    /// Only use in situations where you absolutely 100% know your
    /// item key is never written or updated multiple times.
    Destroy,
}

/// Trait for compaction filter objects
pub trait CompactionFilter: Send {
    /// Returns whether an item should be kept during compaction.
    ///
    /// # Errors
    ///
    /// Returning an error will abort the running compaction.
    /// This should only be done when **strictly** necessary, such as when fetching a value fails.
    fn filter_item(
        &mut self,
        item: ItemAccessor<'_>,
        ctx: &CompactionFilterContext,
    ) -> crate::Result<Verdict>;

    /// Called when compaction is finished.
    fn finish(self: Box<Self>) {}
}

/// Context passed into [`CompactionFilterFactory::make_filter`] for each compaction run.
#[non_exhaustive]
#[derive(Debug)]
pub struct CompactionFilterContext {
    /// Whether we are compacting into the last level.
    pub is_last_level: bool,
}

/// Trait that creates compaction filter objects for each compaction
pub trait CompactionFilterFactory: Send + Sync + RefUnwindSafe {
    /// Returns a new compaction filter.
    fn make_filter(&self, context: &CompactionFilterContext) -> Box<dyn CompactionFilter>;
}

struct AccessorShared<'a> {
    opts: &'a Options,
    version: &'a Version,
    blobs_folder: &'a Path,
}

impl AccessorShared<'_> {
    /// Fetches a value from the blob store.
    fn get_indirect_value(
        &self,
        user_key: &[u8],
        vhandle: &ValueHandle,
    ) -> crate::Result<Option<UserValue>> {
        Accessor::new(&self.version.blob_files).get(
            self.opts.tree_id,
            self.blobs_folder,
            user_key,
            vhandle,
            &self.opts.config.cache,
        )
    }
}

/// Accessor for the key/value from a compaction filter
pub struct ItemAccessor<'a> {
    item: &'a InternalValue,
    shared: &'a AccessorShared<'a>,
}

impl<'a> ItemAccessor<'a> {
    /// Get the key of this item.
    #[must_use]
    pub fn key(&self) -> &'a UserKey {
        &self.item.key.user_key
    }

    /// Returns whether this item's value is stored separately.
    #[must_use]
    #[doc(hidden)]
    pub fn is_indirection(&self) -> bool {
        self.item.key.value_type.is_indirection()
    }

    /// Get the value of this item.
    ///
    /// # Errors
    ///
    /// This method will return an error if blob retrieval fails.
    pub fn value(&self) -> crate::Result<UserValue> {
        match self.item.key.value_type {
            crate::ValueType::Value => Ok(self.item.value.clone()),
            crate::ValueType::Indirection => {
                // resolve and read the value from a blob
                let mut reader = &self.item.value[..];
                let indirection = BlobIndirection::decode_from(&mut reader)?;
                let vhandle = indirection.vhandle;

                let value = self
                    .shared
                    .get_indirect_value(&self.item.key.user_key, &vhandle)?;

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
            crate::ValueType::WeakTombstone | crate::ValueType::Tombstone => {
                unreachable!("tombstones are filtered out before calling filter")
            }
        }
    }
}

/// Adapts a [`CompactionFilter`] to a [`StreamFilter`]
//
// NOTE: this slightly helps insulate CompactionStream from lifetime spam
pub(crate) struct StreamFilterAdapter<'a, 'b: 'a> {
    filter: Option<&'a mut (dyn CompactionFilter + 'b)>,
    shared: AccessorShared<'a>,
    blob_opts: Option<&'a KvSeparationOptions>,
    blob_writer: &'a mut Option<BlobFileWriter>,
    ctx: &'a CompactionFilterContext,
}

impl<'a, 'b: 'a> StreamFilterAdapter<'a, 'b> {
    pub fn new(
        filter: Option<&'a mut (dyn CompactionFilter + 'b)>,
        opts: &'a Options,
        version: &'a Version,
        blobs_folder: &'a Path,
        blob_writer: &'a mut Option<BlobFileWriter>,
        ctx: &'a CompactionFilterContext,
    ) -> Self {
        Self {
            filter,
            shared: AccessorShared {
                opts,
                version,
                blobs_folder,
            },
            blob_opts: opts.config.kv_separation_opts.as_ref(),
            blob_writer,
            ctx,
        }
    }

    /// Redirects a write to a blob file if KV separation is enabled and
    /// the value meets the separation threshold.
    fn handle_write(
        &mut self,
        prev_key: &InternalKey,
        new_value: UserValue,
    ) -> crate::Result<(ValueType, UserValue)> {
        let Some(blob_opts) = self.blob_opts else {
            return Ok((ValueType::Value, new_value));
        };

        #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
        let value_size = new_value.len() as u32;

        if value_size < blob_opts.separation_threshold {
            return Ok((ValueType::Value, new_value));
        }

        let writer = if let Some(writer) = self.blob_writer {
            writer
        } else {
            // Instantiate writer as necessary
            let writer = BlobFileWriter::new(
                self.shared.opts.blob_file_id_generator.clone(),
                self.shared.blobs_folder,
                self.shared.opts.tree_id,
                self.shared.opts.config.descriptor_table.clone(),
            )?
            .use_target_size(blob_opts.file_target_size)
            .use_compression(blob_opts.compression);

            self.blob_writer.insert(writer)
        };

        let indirection = BlobIndirection {
            vhandle: ValueHandle {
                blob_file_id: writer.blob_file_id(),
                offset: writer.offset(),
                on_disk_size: writer.write(&prev_key.user_key, prev_key.seqno, &new_value)?,
            },
            size: value_size,
        };

        Ok((ValueType::Indirection, indirection.encode_into_vec().into()))
    }
}

impl<'a, 'b: 'a> StreamFilter for StreamFilterAdapter<'a, 'b> {
    fn filter_item(&mut self, item: &InternalValue) -> crate::Result<StreamFilterVerdict> {
        let Some(filter) = self.filter.as_mut() else {
            return Ok(StreamFilterVerdict::Keep);
        };

        match filter.filter_item(
            ItemAccessor {
                item,
                shared: &self.shared,
            },
            &self.ctx,
        )? {
            Verdict::Destroy => Ok(StreamFilterVerdict::Drop),
            Verdict::Keep => Ok(StreamFilterVerdict::Keep),
            Verdict::Remove => Ok(StreamFilterVerdict::Replace((
                ValueType::Tombstone,
                UserValue::empty(),
            ))),
            Verdict::RemoveWeak => Ok(StreamFilterVerdict::Replace((
                ValueType::WeakTombstone,
                UserValue::empty(),
            ))),
            Verdict::ReplaceValue(new_value) => self
                .handle_write(&item.key, new_value)
                .map(StreamFilterVerdict::Replace),
        }
    }
}
