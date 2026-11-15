// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{prefix::SharedPrefixExtractor, table::filter::standard_bloom::Builder};

/// Hashes the prefixes of the keys a table writer adds, skipping prefixes the
/// current filter partition already holds.
///
/// Keys arrive sorted and, by the extractor's contiguity rule, the keys that
/// share a prefix are adjacent, so a prefix is new unless the previous key
/// had it too. This is `RocksDB`'s adjacent-hash dedup extended to several
/// prefixes per key.
pub struct PrefixHashes {
    extractor: SharedPrefixExtractor,

    /// Prefix lengths of the previous key
    prev_lens: Vec<usize>,

    /// Prefix lengths of the current key
    lens: Vec<usize>,

    /// Whether the previous key's prefixes are in the current partition
    same_partition: bool,

    /// Prefixes the previous key starts with but stopped having, so no later
    /// key may have them again
    #[cfg(debug_assertions)]
    ended: Vec<usize>,
}

impl PrefixHashes {
    pub fn new(extractor: SharedPrefixExtractor) -> Self {
        Self {
            extractor,
            prev_lens: Vec::new(),
            lens: Vec::new(),
            same_partition: false,
            #[cfg(debug_assertions)]
            ended: Vec::new(),
        }
    }

    pub fn extractor(&self) -> &SharedPrefixExtractor {
        &self.extractor
    }

    /// Appends the hashes of `key`'s prefixes that the current partition lacks.
    ///
    /// `prev_key` is the key added before this one. A prefix equal to the
    /// whole key is skipped when `has_key_hash`, as the key's own hash is
    /// the same.
    pub fn push_new(
        &mut self,
        prev_key: Option<&[u8]>,
        key: &[u8],
        has_key_hash: bool,
        hashes: &mut Vec<u64>,
    ) {
        let lens = &mut self.lens;
        lens.clear();

        self.extractor.extract(key, &mut |len| {
            if len <= key.len() && !lens.contains(&len) {
                lens.push(len);
            }
        });

        let shared = prev_key.map_or(0, |prev| {
            prev.iter().zip(key).take_while(|(a, b)| a == b).count()
        });

        #[cfg(debug_assertions)]
        self.check_contiguity(key, shared);

        for &len in &self.lens {
            let in_partition =
                self.same_partition && len <= shared && self.prev_lens.contains(&len);
            let is_key = has_key_hash && len == key.len();

            if !in_partition && !is_key {
                if let Some(prefix) = key.get(..len) {
                    hashes.push(Builder::get_hash(prefix));
                }
            }
        }

        std::mem::swap(&mut self.prev_lens, &mut self.lens);
        self.same_partition = true;
    }

    /// Marks a filter partition boundary: the next key's prefixes go into a
    /// new partition, which holds none of them yet.
    pub fn start_partition(&mut self) {
        self.same_partition = false;
    }

    /// Panics if a prefix comes back after a key that starts with it lacked it.
    #[cfg(debug_assertions)]
    fn check_contiguity(&mut self, key: &[u8], shared: usize) {
        // A prefix longer than `shared` is not a prefix of `key`; as keys are
        // sorted, no later key starts with it either.
        self.ended.retain(|&len| len <= shared);

        for &len in &self.lens {
            assert!(
                !self.ended.contains(&len),
                "prefix extractor {:?} broke contiguity: {:?} had prefix {:?}, an earlier key starting with it did not",
                self.extractor.name(),
                key,
                key.get(..len),
            );
        }

        for &len in &self.prev_lens {
            if len <= shared && !self.lens.contains(&len) {
                self.ended.push(len);
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::prefix::{FixedPrefixExtractor, PrefixExtractor};
    use std::sync::Arc;

    fn run(extractor: SharedPrefixExtractor, keys: &[&[u8]], has_key_hash: bool) -> Vec<u64> {
        let mut prefixes = PrefixHashes::new(extractor);
        let mut hashes = vec![];
        let mut prev: Option<&[u8]> = None;
        for &key in keys {
            prefixes.push_new(prev, key, has_key_hash, &mut hashes);
            prev = Some(key);
        }
        hashes
    }

    fn h(bytes: &[u8]) -> u64 {
        Builder::get_hash(bytes)
    }

    struct PathSegments;

    impl PrefixExtractor for PathSegments {
        fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
            for (i, &byte) in key.iter().enumerate() {
                if byte == b'/' {
                    emit(i + 1);
                }
            }
        }

        fn name(&self) -> &'static str {
            "test::PathSegments"
        }
    }

    #[test]
    fn adjacent_keys_share_one_hash() {
        let hashes = run(
            Arc::new(FixedPrefixExtractor::new(3)),
            &[b"aaa1", b"aaa2", b"aaa3", b"bbb1", b"bbb2", b"c"],
            false,
        );
        assert_eq!(hashes, [h(b"aaa"), h(b"bbb")]);
    }

    #[test]
    fn multiple_prefixes_per_key_dedup_independently() {
        let hashes = run(
            Arc::new(PathSegments),
            &[b"a/x/1", b"a/x/2", b"a/y/1", b"b/y/1"],
            false,
        );
        assert_eq!(
            hashes,
            [h(b"a/"), h(b"a/x/"), h(b"a/y/"), h(b"b/"), h(b"b/y/")]
        );
    }

    #[test]
    fn partition_boundary_repeats_open_prefixes() {
        let mut prefixes = PrefixHashes::new(Arc::new(FixedPrefixExtractor::new(3)));
        let mut hashes = vec![];
        prefixes.push_new(None, b"aaa1", false, &mut hashes);
        prefixes.start_partition();
        prefixes.push_new(Some(b"aaa1"), b"aaa2", false, &mut hashes);
        prefixes.push_new(Some(b"aaa2"), b"aaa3", false, &mut hashes);
        assert_eq!(hashes, [h(b"aaa"), h(b"aaa")]);
    }

    #[test]
    fn prefix_equal_to_key_uses_key_hash() {
        let hashes = run(
            Arc::new(FixedPrefixExtractor::new(3)),
            &[b"aaa", b"aaa1"],
            true,
        );
        assert!(hashes.is_empty(), "{hashes:?}");

        let hashes = run(
            Arc::new(FixedPrefixExtractor::new(3)),
            &[b"aaa", b"aaa1"],
            false,
        );
        assert_eq!(hashes, [h(b"aaa")]);
    }

    /// Two bytes if the third is at least 0x80, else three: "ab\x01" has
    /// prefix "ab\x01", the next key "ab\x90" has "ab".
    struct HighSplit;

    impl PrefixExtractor for HighSplit {
        fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
            match key.get(2) {
                None => {}
                Some(&byte) if byte >= 0x80 => emit(2),
                Some(_) => emit(3),
            }
        }

        fn name(&self) -> &'static str {
            "test::HighSplit"
        }
    }

    /// The previous key starts with "ab" without having it, so "ab" is new
    /// though it is within the bytes both keys share.
    #[test]
    fn prefix_the_previous_key_lacked_is_new() {
        let hashes = run(
            Arc::new(HighSplit),
            &[b"ab\x01", b"ab\x90", b"ab\x91"],
            false,
        );
        assert_eq!(hashes, [h(b"ab\x01"), h(b"ab")]);
    }

    /// Extracts "a" from "a" and from "a@...", but not from "a!": the
    /// whole-key-plus-domain mistake from the `PrefixExtractor` docs.
    struct DomainAndWholeKey;

    impl PrefixExtractor for DomainAndWholeKey {
        fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
            if let Some(at) = key.iter().position(|&b| b == b'@') {
                emit(at);
            }
            emit(key.len());
        }

        fn name(&self) -> &'static str {
            "test::DomainAndWholeKey"
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "broke contiguity")]
    fn non_contiguous_extractor_panics() {
        run(Arc::new(DomainAndWholeKey), &[b"a", b"a!", b"a@x"], false);
    }

    #[test]
    fn contiguous_extractor_passes_check() {
        // "ab" ends at "abz", which starts with it, and never comes back
        struct SplitWidth;

        impl PrefixExtractor for SplitWidth {
            fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
                match key.len() {
                    0 | 1 => {}
                    2 => emit(2),
                    _ if key.get(2).unwrap() < &b'm' => emit(2),
                    _ => emit(3),
                }
            }

            fn name(&self) -> &'static str {
                "test::SplitWidth"
            }
        }

        let hashes = run(
            Arc::new(SplitWidth),
            &[b"ab", b"ab1", b"abz1", b"abz2", b"ac"],
            false,
        );
        assert_eq!(hashes, [h(b"ab"), h(b"abz"), h(b"ac")]);
    }
}
