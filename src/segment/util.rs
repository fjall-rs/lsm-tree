// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub fn longest_shared_prefix_length(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter()
        .zip(s2.iter())
        .take_while(|(c1, c2)| c1 == c2)
        .count()
}

// TODO: Fuzz test
pub fn compare_prefixed_slice(prefix: &[u8], suffix: &[u8], needle: &[u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering::{Equal, Greater, Less};

    if needle.is_empty() {
        let combined_len = prefix.len() + suffix.len();

        return if combined_len > 0 { Greater } else { Equal };
    }

    match prefix.len().cmp(&needle.len()) {
        Equal => match prefix.cmp(needle) {
            Equal => {}
            ordering => return ordering,
        },
        Greater => {
            // SAFETY: We know that the prefix is longer than the needle, so we can safely
            // truncate it to the needle's length
            #[allow(unsafe_code)]
            let prefix = unsafe { prefix.get_unchecked(0..needle.len()) };
            return prefix.cmp(needle);
        }
        Less => {
            // SAFETY: We know that the needle is longer than the prefix, so we can safely
            // truncate it to the prefix's length
            #[allow(unsafe_code)]
            let needle = unsafe { needle.get_unchecked(0..prefix.len()) };

            match prefix.cmp(needle) {
                Equal => {}
                ordering => return ordering,
            }
        }
    }

    // SAFETY: We know that the prefix is definitely not longer than the needle
    // so we can safely truncate
    #[allow(unsafe_code)]
    let needle = unsafe { needle.get_unchecked(prefix.len()..) };
    suffix.cmp(needle)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn v3_compare_prefixed_slice() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        assert_eq!(Equal, compare_prefixed_slice(b"", b"", b""));

        assert_eq!(Greater, compare_prefixed_slice(b"a", b"", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"", b"a", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"a", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"b", b"a", b"a"));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"b", b"a"));

        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"y"));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"yyy"));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"yyy"));
        assert_eq!(Less, compare_prefixed_slice(b"yyyy", b"a", b"yyyyb"));
        assert_eq!(Less, compare_prefixed_slice(b"yyy", b"b", b"yyyyb"));
    }
}
