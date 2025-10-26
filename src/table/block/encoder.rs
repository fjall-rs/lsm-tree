// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    super::{
        block::binary_index::Builder as BinaryIndexBuilder,
        block::hash_index::{Builder as HashIndexBuilder, MAX_POINTERS_FOR_HASH_INDEX},
        util::longest_shared_prefix_length,
    },
    Trailer,
};
use std::marker::PhantomData;

pub trait Encodable<Context: Default> {
    fn key(&self) -> &[u8];

    fn encode_full_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        state: &mut Context,
    ) -> crate::Result<()>
    where
        Self: Sized;

    fn encode_truncated_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        state: &mut Context,
        shared_len: usize,
    ) -> crate::Result<()>
    where
        Self: Sized;
}

/// Block encoder
///
/// The block encoder accepts an ascending stream of items, encodes them into
/// restart intervals and builds binary index (and optionally a hash index).
///
/// # Example
///
/// A block with `restart_interval=4`
///
/// ```js
///                                                                         _______________
///                                                              __________|__________     |
///                                                             v          v          |    |
/// [h][t][t][t][h][t][t][t][h][t][t][t][h][t][t][t][h][t][t][t][0,1,2,3,4][0 C F F 3][ptr][ptr]
/// ^           ^           ^           ^           ^           ^          ^
/// 0           1           2           3           4           bin index  hash index
///
/// h = restart head
/// t = truncated item
/// ```
///
/// The binary index holds pointers to all restart heads.
/// Because restart heads hold a full key, they can be used to compare to a needle key.
///
/// For explanation of hash index, see `hash_index/mod.rs`.
pub struct Encoder<'a, Context: Default, Item: Encodable<Context>> {
    pub(crate) phantom: PhantomData<(Context, Item)>,

    pub(crate) writer: &'a mut Vec<u8>,

    pub(crate) state: Context,

    pub(crate) item_count: usize,
    pub(crate) restart_count: usize,

    pub(crate) restart_interval: u8,
    // pub(crate) use_prefix_truncation: bool, // TODO: support non-prefix truncation
    pub(crate) binary_index_builder: BinaryIndexBuilder,
    pub(crate) hash_index_builder: HashIndexBuilder,

    base_key: &'a [u8],
}

// TODO: support no binary index -> use in meta blocks with restart interval = 1
// TODO: adjust test + fuzz tests to also test for no binary index

impl<'a, Context: Default, Item: Encodable<Context>> Encoder<'a, Context, Item> {
    pub fn new(
        writer: &'a mut Vec<u8>,
        item_count: usize,
        restart_interval: u8, // TODO: should be NonZero
        hash_index_ratio: f32,
        first_key: &'a [u8],
    ) -> Self {
        let binary_index_builder = BinaryIndexBuilder::new(item_count / restart_interval as usize);
        let hash_index_builder = HashIndexBuilder::with_hash_ratio(item_count, hash_index_ratio);

        Self {
            phantom: PhantomData,

            writer,

            state: Context::default(),

            item_count: 0,
            restart_count: 0,

            restart_interval,
            // use_prefix_truncation: true,
            binary_index_builder,
            hash_index_builder,

            base_key: first_key,
        }
    }

    // /// Toggles prefix truncation.
    // pub fn use_prefix_truncation(mut self, flag: bool) -> Self {
    //     assert!(flag, "prefix truncation is currently required to be true");

    //     self.use_prefix_truncation = flag;

    //     self
    // }

    pub fn write(&mut self, item: &'a Item) -> crate::Result<()> {
        // NOTE: Check if we are a restart marker
        if self
            .item_count
            .is_multiple_of(usize::from(self.restart_interval))
        {
            self.restart_count += 1;

            if self.restart_interval > 0 {
                // NOTE: We know that data blocks will never even approach 4 GB in size
                #[allow(clippy::cast_possible_truncation)]
                self.binary_index_builder.insert(self.writer.len() as u32);
            }

            item.encode_full_into(&mut *self.writer, &mut self.state)?;

            self.base_key = item.key();
        } else {
            // NOTE: We can safely cast to u16, because keys are u16 long max
            #[allow(clippy::cast_possible_truncation)]
            let shared_prefix_len = longest_shared_prefix_length(self.base_key, item.key());

            item.encode_truncated_into(&mut *self.writer, &mut self.state, shared_prefix_len)?;
        }

        let restart_idx = self.restart_count - 1;

        if self.hash_index_builder.bucket_count() > 0 && restart_idx < MAX_POINTERS_FOR_HASH_INDEX {
            // NOTE: The max binary index is bound to u8 by conditional
            #[allow(clippy::cast_possible_truncation)]
            self.hash_index_builder.set(item.key(), restart_idx as u8);
        }

        self.item_count += 1;

        Ok(())
    }

    pub fn finish(self) -> crate::Result<()> {
        Trailer::write(self)
    }
}
