// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::super::hash_index::Builder as HashIndexBuilder;
use super::{super::binary_index::Builder as BinaryIndexBuilder, Trailer};
use crate::super_segment::util::longest_shared_prefix_length;
use std::marker::PhantomData;

pub trait Encodable<S: Default> {
    fn key(&self) -> &[u8];

    fn encode_full_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        state: &mut S,
    ) -> crate::Result<()>
    where
        Self: Sized;

    fn encode_truncated_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        state: &mut S,
        shared_len: usize,
    ) -> crate::Result<()>
    where
        Self: Sized;
}

/// Block encoder
pub struct Encoder<'a, S: Default, T: Encodable<S>> {
    pub(crate) phantom: PhantomData<(S, T)>,

    pub(crate) writer: Vec<u8>,

    pub(crate) state: S,

    pub(crate) item_count: usize,
    pub(crate) restart_count: usize,

    pub(crate) restart_interval: u8,
    pub(crate) use_prefix_truncation: bool,

    pub(crate) binary_index_builder: BinaryIndexBuilder,
    pub(crate) hash_index_builder: HashIndexBuilder,

    base_key: &'a [u8],
}

// TODO: maybe split into Builder
impl<'a, S: Default, T: Encodable<S>> Encoder<'a, S, T> {
    pub fn new(
        item_count: usize,
        restart_interval: u8,
        hash_index_ratio: f32,
        first_key: &'a [u8],
    ) -> Self {
        let binary_index_len = item_count / usize::from(restart_interval);
        let bucket_count = (item_count as f32 * hash_index_ratio) as u32; // TODO: verify

        Self {
            phantom: PhantomData,

            writer: Vec::new(),

            state: S::default(),

            item_count: 0,
            restart_count: 0,

            restart_interval,
            use_prefix_truncation: true,

            binary_index_builder: BinaryIndexBuilder::new(binary_index_len),
            hash_index_builder: HashIndexBuilder::new(bucket_count),

            base_key: first_key,
        }
    }

    /* /// Toggles prefix truncation.
    pub fn use_prefix_truncation(mut self, flag: bool) -> Self {
        self.use_prefix_truncation = flag;
        self
    } */

    pub fn write(&mut self, item: &'a T) -> crate::Result<()> {
        // NOTE: Check if we are a restart marker
        if self.item_count % usize::from(self.restart_interval) == 0 {
            self.restart_count += 1;

            if self.restart_interval > 0 {
                // NOTE: We know that data blocks will never even approach 4 GB in size
                #[allow(clippy::cast_possible_truncation)]
                self.binary_index_builder.insert(self.writer.len() as u32);
            }

            item.encode_full_into(&mut self.writer, &mut self.state)?;

            self.base_key = item.key();
        } else {
            // NOTE: We can safely cast to u16, because keys are u16 long max
            #[allow(clippy::cast_possible_truncation)]
            let shared_prefix_len = longest_shared_prefix_length(self.base_key, item.key());

            item.encode_truncated_into(&mut self.writer, &mut self.state, shared_prefix_len)?;
        }

        if self.hash_index_builder.bucket_count() > 0 {
            // NOTE: The max binary index is bound by u8 (technically u8::MAX - 2)
            #[allow(clippy::cast_possible_truncation)]
            self.hash_index_builder
                .set(item.key(), (self.restart_count - 1) as u8);
        }

        self.item_count += 1;

        Ok(())
    }

    pub fn finish(self) -> crate::Result<Vec<u8>> {
        Trailer::write(self)
    }
}
