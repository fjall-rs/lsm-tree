// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Returns the index of the partition point according to the given predicate
/// (the index of the first element of the second partition).
///
/// This seems to be faster than std's `partition_point`: <https://github.com/rust-lang/rust/issues/138796>
pub fn partition_point<T, F>(slice: &[T], pred: F) -> usize
where
    F: Fn(&T) -> bool,
{
    let mut left = 0;
    let mut right = slice.len();

    if right == 0 {
        return 0;
    }

    while left < right {
        let mid = (left + right) / 2;

        // SAFETY: See https://github.com/rust-lang/rust/blob/ebf0cf75d368c035f4c7e7246d203bd469ee4a51/library/core/src/slice/mod.rs#L2834-L2836
        #[warn(unsafe_code)]
        let item = unsafe { slice.get_unchecked(mid) };

        if pred(item) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    left
}

#[cfg(test)]
mod tests {
    use super::partition_point;
    use test_log::test;

    #[test]
    fn binary_search_first() {
        let items = [1, 2, 3, 4, 5];
        let idx = partition_point(&items, |&x| x < 1);
        assert_eq!(0, idx);

        let std_pp_idx = items.partition_point(|&x| x < 1);
        assert_eq!(std_pp_idx, idx);
    }

    #[test]
    fn binary_search_last() {
        let items = [1, 2, 3, 4, 5];
        let idx = partition_point(&items, |&x| x < 5);
        assert_eq!(4, idx);

        let std_pp_idx = items.partition_point(|&x| x < 5);
        assert_eq!(std_pp_idx, idx);
    }

    #[test]
    fn binary_search_middle() {
        let items = [1, 2, 3, 4, 5];
        let idx = partition_point(&items, |&x| x < 3);
        assert_eq!(2, idx);

        let std_pp_idx = items.partition_point(|&x| x < 3);
        assert_eq!(std_pp_idx, idx);
    }

    #[test]
    fn binary_search_none() {
        let items = [1, 2, 3, 4, 5];
        let idx = partition_point(&items, |&x| x < 10);
        assert_eq!(5, idx);

        let std_pp_idx = items.partition_point(|&x| x < 10);
        assert_eq!(std_pp_idx, idx);
    }

    #[test]
    fn binary_search_empty() {
        let items: [i32; 0] = [];
        let idx = partition_point(&items, |&x| x < 10);
        assert_eq!(0, idx);

        let std_pp_idx = items.partition_point(|&x| x < 10);
        assert_eq!(std_pp_idx, idx);
    }
}
