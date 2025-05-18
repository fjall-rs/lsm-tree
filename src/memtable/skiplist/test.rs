// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::{
    collections::BTreeMap,
    fmt::{Debug, Write},
    num::NonZero,
    ops::RangeBounds,
    sync::Barrier,
};

use super::*;
use quickcheck::{Arbitrary, Gen};
use rand::{rng, RngCore};

#[test]
fn test_basic() {
    let v = SkipMap::<usize, usize>::new(rng().next_u32());
    assert_eq!(v.insert(1, 1), Ok(()));
    assert_eq!(v.len(), 1);
    assert_eq!(v.insert(1, 2), Err((1, 2)));
    assert_eq!(v.len(), 1);
    assert_eq!(v.insert(2, 2), Ok(()));
    assert_eq!(v.len(), 2);
    assert_eq!(v.insert(2, 1), Err((2, 1)));
    let got: Vec<_> = v.iter().map(|e| (*e.key(), *e.value())).collect();
    assert_eq!(got, vec![(1, 1), (2, 2)]);
    let got_rev: Vec<_> = v.iter().rev().map(|e| (*e.key(), *e.value())).collect();
    assert_eq!(got_rev, vec![(2, 2), (1, 1)]);
}

#[test]
#[allow(clippy::unwrap_used)]
fn test_basic_strings() {
    let v = SkipMap::<String, usize>::new(rng().next_u32());
    let mut foo = String::new();
    foo.write_str("foo").unwrap();
    assert_eq!(v.insert(foo, 1), Ok(()));
    assert_eq!(v.len(), 1);
    assert_eq!(v.insert("foo".into(), 2), Err(("foo".into(), 2)));
    assert_eq!(v.len(), 1);
    assert_eq!(v.insert("bar".into(), 2), Ok(()));
    assert_eq!(v.len(), 2);
    assert_eq!(v.insert("bar".into(), 1), Err(("bar".into(), 1)));
    let got: Vec<_> = v.iter().map(|e| (e.key().clone(), *e.value())).collect();
    assert_eq!(got, vec![("bar".into(), 2), ("foo".into(), 1)]);
}

#[derive(Clone, Debug)]
struct TestOperation<K, V> {
    key: K,
    value: V,
}

impl<K, V> Arbitrary for TestOperation<K, V>
where
    K: Arbitrary,
    V: Arbitrary,
{
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            key: K::arbitrary(g),
            value: V::arbitrary(g),
        }
    }
}

#[derive(Debug, Clone)]
struct TestOperations<K, V> {
    seed: u32,
    threads: usize,
    ops: Vec<TestOperation<K, V>>,
}

impl<K, V> Arbitrary for TestOperations<K, V>
where
    K: Arbitrary,
    V: Arbitrary,
{
    fn arbitrary(g: &mut Gen) -> Self {
        let max_threads = std::thread::available_parallelism()
            .map(NonZero::get)
            .unwrap_or(64)
            * 16;
        Self {
            seed: u32::arbitrary(g),
            threads: 1usize.max(usize::arbitrary(g) % max_threads),
            ops: <Vec<TestOperation<K, V>> as Arbitrary>::arbitrary(g),
        }
    }
}

fn prop<K, V>(operations: TestOperations<K, V>) -> bool
where
    K: Arbitrary + Ord + Eq + Debug + Send + Sync + Clone,
    V: Arbitrary + Eq + Debug + Send + Sync + Clone,
{
    #[cfg(not(miri))]
    const TRACK_OUTCOMES: bool = true;
    #[cfg(miri)]
    const TRACK_OUTCOMES: bool = false;

    let mut skipmap = SkipMap::new(operations.seed);
    let barrier = Barrier::new(operations.threads);

    let outcomes = std::thread::scope(|scope| {
        let (mut ops, mut threads_to_launch) = (operations.ops.as_slice(), operations.threads);
        let mut thread_outcomes = Vec::new();
        while threads_to_launch > 0 {
            let items = ops.len() / threads_to_launch;
            let (subslice, remaining) = ops.split_at(items);
            ops = remaining;
            threads_to_launch -= 1;
            let skipmap = &skipmap;
            let barrier = &barrier;
            let spawned = scope.spawn(move || {
                barrier.wait();
                let mut outcomes = Vec::new();
                for op in subslice {
                    outcomes.push(skipmap.insert(op.key.clone(), op.value.clone()).is_ok());
                }
                outcomes
            });

            if TRACK_OUTCOMES {
                thread_outcomes.push(spawned);
            }
        }

        thread_outcomes
            .into_iter()
            .flat_map(|v| v.join().unwrap())
            .collect::<Vec<_>>()
    });

    #[cfg(miri)]
    if true {
        return true;
    }

    let successful_ops = operations
        .ops
        .into_iter()
        .zip(outcomes)
        .filter_map(|(op, outcome)| outcome.then_some(op))
        .collect::<Vec<_>>();

    skipmap.check_integrity();

    verify_ranges(&skipmap, &successful_ops);

    let skipmap_items: Vec<_> = skipmap
        .iter()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();
    let skipmap_items_rev: Vec<_> = skipmap
        .iter()
        .rev()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();

    let mut skipmap_items_rev_rev = skipmap_items_rev.clone();
    skipmap_items_rev_rev.reverse();

    assert_eq!(successful_ops.len(), skipmap.len(), "len");
    assert_eq!(skipmap_items.len(), skipmap.len(), "items");
    assert_eq!(skipmap_items.len(), skipmap_items_rev.len(), "rev items");
    assert_eq!(
        skipmap_items, skipmap_items_rev_rev,
        "Forward iteration should match\n{skipmap_items:#?}\n{skipmap_items_rev_rev:#?}",
    );

    true
}

#[test]
fn test_quickcheck_strings() {
    quickcheck::quickcheck(prop as fn(TestOperations<String, i32>) -> bool);
}

#[test]
fn test_quickcheck_ints() {
    quickcheck::quickcheck(prop as fn(TestOperations<i64, i32>) -> bool);
}

#[allow(clippy::indexing_slicing)]
fn verify_ranges<K, V>(skipmap: &SkipMap<K, V>, successful_ops: &Vec<TestOperation<K, V>>)
where
    K: Ord + Eq + Debug + Clone,
    V: Eq + Debug + Clone,
{
    let mut successful_keys_sorted = successful_ops
        .iter()
        .map(|op| op.key.clone())
        .collect::<Vec<_>>();
    successful_keys_sorted.sort();

    let btree = successful_ops
        .iter()
        .map(|TestOperation { key, value }| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();

    for _ in 0..10 {
        if successful_ops.is_empty() {
            break;
        }
        let (a, b) = (
            rng().next_u32() as usize % successful_ops.len(),
            rng().next_u32() as usize % successful_ops.len(),
        );

        let (start, end) = (a.min(b), a.max(b));

        fn assert_range_eq<K, V, B: RangeBounds<K> + Clone + std::fmt::Debug>(
            a: &BTreeMap<K, V>,
            b: &SkipMap<K, V>,
            bounds: B,
        ) where
            K: Ord + Eq + Debug + Clone,
            V: Eq + Debug + Clone,
        {
            {
                let ra = a
                    .range(bounds.clone())
                    .map(|(a, b)| (a.clone(), b.clone()))
                    .collect::<Vec<_>>();

                let rb = b
                    .range(bounds.clone())
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect::<Vec<_>>();

                assert_eq!(
                    ra,
                    rb,
                    "{} {:?} forward: {:#?} != {:#?}",
                    std::any::type_name::<B>(),
                    bounds,
                    ra,
                    rb
                );
            }
            {
                let ra = a
                    .range(bounds.clone())
                    .rev()
                    .map(|(a, b)| (a.clone(), b.clone()))
                    .collect::<Vec<_>>();

                let rb = b
                    .range(bounds.clone())
                    .rev()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect::<Vec<_>>();

                assert_eq!(
                    ra,
                    rb,
                    "{} {:?} backwards: {:#?} != {:#?}",
                    std::any::type_name::<B>(),
                    bounds,
                    ra,
                    rb
                );
            }
        }

        let (start, end) = (&successful_keys_sorted[start], &successful_keys_sorted[end]);
        assert_range_eq(&btree, skipmap, ..);
        assert_range_eq(&btree, skipmap, ..end);
        assert_range_eq(&btree, skipmap, ..=end);
        assert_range_eq(&btree, skipmap, start..);
        assert_range_eq(&btree, skipmap, start..end);
        assert_range_eq(&btree, skipmap, start..=end);
    }
}
