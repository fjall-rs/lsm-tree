use crate::{
    binary_search::partition_point, version::run::Ranged, KeyRange, Segment, SegmentId, UserKey,
};
use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
};

pub trait Identifiable<Id> {
    fn id(&self) -> Id;
}

impl Identifiable<SegmentId> for Segment {
    fn id(&self) -> SegmentId {
        self.id()
    }
}

#[derive(Clone, Debug)]
pub struct Partition<T: Clone + Debug + Ranged + Identifiable<SegmentId>> {
    key_range: KeyRange,
    segments: VecDeque<T>,
}

#[derive(Clone, Debug, Default)]
pub struct KeyRangePartitions<T: Clone + Debug + Ranged + Identifiable<SegmentId>>(
    Vec<Partition<T>>,
);

impl<T: Clone + Debug + Ranged + Identifiable<SegmentId>> KeyRangePartitions<T> {
    pub fn new(pairs: impl Iterator<Item = (UserKey, UserKey)>) -> Self {
        let mut partitions = vec![];

        for (start_key, end_key) in pairs {
            partitions.push(Partition {
                key_range: KeyRange::new((start_key, end_key)),
                segments: VecDeque::new(),
            });
        }

        Self(partitions)
    }

    pub fn index_segment(&mut self, segment: &T) {
        let key_range = &segment.key_range();
        let start_key = key_range.min();

        let idx = partition_point(&self.0, |x| x.key_range.max() < start_key);

        if let Some(slice) = self.0.get_mut(idx..) {
            for partition in slice
                .iter_mut()
                .filter(|x| x.key_range.overlaps_with_key_range(key_range))
            {
                partition.segments.push_back(segment.clone());
            }
        }
    }

    pub fn into_optimized_runs(mut self) -> Vec<Vec<T>> {
        let mut optimized = VecDeque::new();
        let mut blacklist = HashSet::<SegmentId>::default();

        while self
            .0
            .iter()
            .any(|partition| !partition.segments.is_empty())
        {
            let run = {
                let mut v: Vec<T> = vec![];

                for partition in &mut self.0 {
                    let Some(front) = partition.segments.front() else {
                        continue;
                    };

                    let curr_id = front.id();

                    if blacklist.contains(&curr_id) {
                        partition.segments.pop_front().expect("front should exist");
                        continue;
                    }

                    if v.iter()
                        .any(|x| x.key_range().overlaps_with_key_range(front.key_range()))
                    {
                        continue;
                    }

                    // NOTE: We just got front previously
                    #[allow(clippy::expect_used)]
                    v.push(partition.segments.pop_front().expect("front should exist"));

                    blacklist.insert(curr_id);
                }

                v
            };

            #[cfg(debug_assertions)]
            {
                let ranges = run.iter().map(Ranged::key_range).collect::<Vec<_>>();
                debug_assert!(KeyRange::is_disjoint(&ranges));
            }

            if !run.is_empty() {
                optimized.push_front(run);
            }
        }

        optimized.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct FauxSegment {
        key_range: KeyRange,
        id: SegmentId,
    }

    impl Identifiable<SegmentId> for FauxSegment {
        fn id(&self) -> SegmentId {
            self.id
        }
    }

    impl Ranged for FauxSegment {
        fn key_range(&self) -> &KeyRange {
            &self.key_range
        }
    }

    #[test]
    fn key_range_partition_single_key_twice() {
        let a = FauxSegment {
            key_range: KeyRange::new((UserKey::new(&[0; 8]), UserKey::new(&[0; 8]))),
            id: 0,
        };
        let b = FauxSegment {
            key_range: KeyRange::new((UserKey::new(&[0; 8]), UserKey::new(&[0; 8]))),
            id: 1,
        };

        {
            let mut index = KeyRangePartitions::<FauxSegment>::new(std::iter::once((
                UserKey::new(&[0; 8]),
                UserKey::new(&[0; 8]),
            )));

            index.index_segment(&a);
            index.index_segment(&b);

            assert_eq!(
                vec![vec![b.clone()], vec![a.clone()]],
                index.into_optimized_runs()
            );
        }

        {
            let mut index = KeyRangePartitions::<FauxSegment>::new(std::iter::once((
                UserKey::new(&[0; 8]),
                UserKey::new(&[0; 8]),
            )));

            index.index_segment(&b);
            index.index_segment(&a);

            assert_eq!(vec![vec![a], vec![b]], index.into_optimized_runs());
        }
    }

    #[test]
    fn key_range_partition_single_key() {
        let a = FauxSegment {
            key_range: KeyRange::new((UserKey::new(b"a"), UserKey::new(b"b"))),
            id: 0,
        };
        let b = FauxSegment {
            key_range: KeyRange::new((UserKey::new(b"a"), UserKey::new(b"a"))),
            id: 1,
        };

        {
            let mut index = KeyRangePartitions::<FauxSegment>::new(std::iter::once((
                UserKey::new(b"a"),
                UserKey::new(b"b"),
            )));

            index.index_segment(&a);
            index.index_segment(&b);

            assert_eq!(
                vec![vec![b.clone()], vec![a.clone()]],
                index.into_optimized_runs()
            );
        }

        {
            let mut index = KeyRangePartitions::<FauxSegment>::new(std::iter::once((
                UserKey::new(b"a"),
                UserKey::new(b"b"),
            )));

            index.index_segment(&b);
            index.index_segment(&a);

            assert_eq!(vec![vec![a], vec![b]], index.into_optimized_runs());
        }
    }

    #[test]
    fn key_range_partition_one_segment() {
        let segment = FauxSegment {
            key_range: KeyRange::new((UserKey::new(b"a"), UserKey::new(b"b"))),
            id: 0,
        };

        let mut index = KeyRangePartitions::<FauxSegment>::new(std::iter::once((
            UserKey::new(b"a"),
            UserKey::new(b"b"),
        )));

        index.index_segment(&segment);

        assert_eq!(vec![vec![segment]], index.into_optimized_runs());
    }

    #[test]
    fn key_range_partition_two_to_one() {
        let a = FauxSegment {
            key_range: KeyRange::new((UserKey::new(b"a"), UserKey::new(b"b"))),
            id: 0,
        };
        let b = FauxSegment {
            key_range: KeyRange::new((UserKey::new(b"c"), UserKey::new(b"d"))),
            id: 1,
        };

        {
            let mut index = KeyRangePartitions::<FauxSegment>::new(
                [
                    (UserKey::new(b"a"), UserKey::new(b"b")),
                    (UserKey::new(b"b"), UserKey::new(b"c")),
                    (UserKey::new(b"c"), UserKey::new(b"d")),
                ]
                .into_iter(),
            );

            index.index_segment(&a);
            index.index_segment(&b);

            eprintln!("{index:#?}");

            assert_eq!(
                vec![vec![a.clone(), b.clone()]],
                index.into_optimized_runs()
            );
        }

        {
            let mut index = KeyRangePartitions::<FauxSegment>::new(
                [
                    (UserKey::new(b"a"), UserKey::new(b"b")),
                    (UserKey::new(b"b"), UserKey::new(b"c")),
                    (UserKey::new(b"c"), UserKey::new(b"d")),
                ]
                .into_iter(),
            );

            index.index_segment(&b);
            index.index_segment(&a);

            assert_eq!(vec![vec![a, b]], index.into_optimized_runs());
        }
    }

    #[test]
    fn key_range_partition_full_overlap() {
        let a = FauxSegment {
            key_range: KeyRange::new((UserKey::new(b"a"), UserKey::new(b"z"))),
            id: 0,
        };
        let b = FauxSegment {
            key_range: KeyRange::new((UserKey::new(b"a"), UserKey::new(b"z"))),
            id: 1,
        };

        {
            let mut index = KeyRangePartitions::<FauxSegment>::new(std::iter::once((
                UserKey::new(b"a"),
                UserKey::new(b"z"),
            )));

            index.index_segment(&a);
            index.index_segment(&b);

            assert_eq!(
                vec![vec![b.clone()], vec![a.clone()]],
                index.into_optimized_runs()
            );
        }

        {
            let mut index = KeyRangePartitions::<FauxSegment>::new(std::iter::once((
                UserKey::new(b"a"),
                UserKey::new(b"z"),
            )));

            index.index_segment(&b);
            index.index_segment(&a);

            assert_eq!(vec![vec![a], vec![b]], index.into_optimized_runs());
        }
    }

    #[test]
    fn key_range_partition_partial_overlap() {
        let a = FauxSegment {
            key_range: KeyRange::new((UserKey::new(b"a"), UserKey::new(b"k"))),
            id: 0,
        };
        let b = FauxSegment {
            key_range: KeyRange::new((UserKey::new(b"c"), UserKey::new(b"z"))),
            id: 1,
        };

        {
            let mut index = KeyRangePartitions::<FauxSegment>::new(
                [
                    (UserKey::new(b"a"), UserKey::new(b"c")),
                    (UserKey::new(b"c"), UserKey::new(b"k")),
                    (UserKey::new(b"k"), UserKey::new(b"z")),
                ]
                .into_iter(),
            );

            index.index_segment(&a);
            index.index_segment(&b);

            assert_eq!(
                vec![vec![b.clone()], vec![a.clone()]],
                index.into_optimized_runs()
            );
        }
    }
}
