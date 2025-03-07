// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

// NOTE: PERF: For some reason, hand-rolling a binary search is
// faster than using slice::partition_point

/// Returns the index of the partition point according to the given predicate
/// (the index of the first element of the second partition).
///
/// Faster alternative to [`slice::partition_point`] (according to benchmarks).
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

        // TODO: PERF: could use get_unchecked for perf... but unsafe
        let item = slice.get(mid).expect("should exist");

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

        let pp_idx = items.partition_point(|&x| x < 1);
        assert_eq!(pp_idx, idx);
    }

    #[test]
    fn binary_search_last() {
        let items = [1, 2, 3, 4, 5];
        let idx = partition_point(&items, |&x| x < 5);
        assert_eq!(4, idx);

        let pp_idx = items.partition_point(|&x| x < 5);
        assert_eq!(pp_idx, idx);
    }

    #[test]
    fn binary_search_middle() {
        let items = [1, 2, 3, 4, 5];
        let idx = partition_point(&items, |&x| x < 3);
        assert_eq!(2, idx);

        let pp_idx = items.partition_point(|&x| x < 3);
        assert_eq!(pp_idx, idx);
    }

    #[test]
    fn binary_search_none() {
        let items = [1, 2, 3, 4, 5];
        let idx = partition_point(&items, |&x| x < 10);
        assert_eq!(5, idx);

        let pp_idx = items.partition_point(|&x| x < 10);
        assert_eq!(pp_idx, idx);
    }

    #[test]
    fn binary_search_empty() {
        let items: [i32; 0] = [];
        let idx = partition_point(&items, |&x| x < 10);
        assert_eq!(0, idx);

        let pp_idx = items.partition_point(|&x| x < 10);
        assert_eq!(pp_idx, idx);
    }
}
