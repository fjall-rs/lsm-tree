// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::run::Ranged;
use crate::version::Run;
use std::fmt::Debug;

pub fn optimize_runs<T: Clone + Debug + Ranged>(runs: Vec<Run<T>>) -> Vec<Run<T>> {
    if runs.len() <= 1 {
        runs
    } else {
        let mut new_runs: Vec<Run<T>> = Vec::new();

        for run in runs.iter().rev() {
            'run: for segment in run.iter().rev() {
                for existing_run in new_runs.iter_mut().rev() {
                    if existing_run
                        .iter()
                        .all(|x| !segment.key_range().overlaps_with_key_range(x.key_range()))
                    {
                        existing_run.push(segment.clone());
                        continue 'run;
                    }
                }

                new_runs.insert(0, Run::new(vec![segment.clone()]));
            }
        }

        new_runs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KeyRange;
    use test_log::test;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeSegment {
        id: u64,
        key_range: KeyRange,
    }

    impl Ranged for FakeSegment {
        fn key_range(&self) -> &KeyRange {
            &self.key_range
        }
    }

    fn s(id: u64, min: &str, max: &str) -> FakeSegment {
        FakeSegment {
            id,
            key_range: KeyRange::new((min.as_bytes().into(), max.as_bytes().into())),
        }
    }

    #[test]
    fn optimize_runs_empty() {
        let runs = vec![];
        let runs = optimize_runs::<FakeSegment>(runs);

        assert_eq!(Vec::<Run<FakeSegment>>::new(), &*runs);
    }

    #[test]
    fn optimize_runs_one() {
        let runs = vec![Run::new(vec![s(0, "a", "b")])];
        let runs = optimize_runs::<FakeSegment>(runs);

        assert_eq!(vec![Run::new(vec![s(0, "a", "b")])], &*runs);
    }

    #[test]
    fn optimize_runs_two_overlap() {
        let runs = vec![
            Run::new(vec![s(0, "a", "b")]),
            Run::new(vec![s(1, "a", "b")]),
        ];
        let runs = optimize_runs::<FakeSegment>(runs);

        assert_eq!(
            vec![
                Run::new(vec![s(0, "a", "b")]),
                Run::new(vec![s(1, "a", "b")])
            ],
            &*runs
        );
    }

    #[test]
    fn optimize_runs_two_overlap_2() {
        let runs = vec![
            Run::new(vec![s(0, "a", "z")]),
            Run::new(vec![s(1, "c", "f")]),
        ];
        let runs = optimize_runs::<FakeSegment>(runs);

        assert_eq!(
            vec![
                Run::new(vec![s(0, "a", "z")]),
                Run::new(vec![s(1, "c", "f")])
            ],
            &*runs
        );
    }

    #[test]
    fn optimize_runs_two_overlap_3() {
        let runs = vec![
            Run::new(vec![s(0, "c", "f")]),
            Run::new(vec![s(1, "a", "z")]),
        ];
        let runs = optimize_runs::<FakeSegment>(runs);

        assert_eq!(
            vec![
                Run::new(vec![s(0, "c", "f")]),
                Run::new(vec![s(1, "a", "z")])
            ],
            &*runs
        );
    }

    #[test]
    fn optimize_runs_two_disjoint() {
        let runs = vec![
            Run::new(vec![s(0, "a", "c")]),
            Run::new(vec![s(1, "d", "f")]),
        ];
        let runs = optimize_runs::<FakeSegment>(runs);

        assert_eq!(vec![Run::new(vec![s(0, "a", "c"), s(1, "d", "f")])], &*runs);
    }

    #[test]
    fn optimize_runs_two_disjoint_2() {
        let runs = vec![
            Run::new(vec![s(1, "d", "f")]),
            Run::new(vec![s(0, "a", "c")]),
        ];
        let runs = optimize_runs::<FakeSegment>(runs);

        assert_eq!(vec![Run::new(vec![s(0, "a", "c"), s(1, "d", "f")])], &*runs);
    }
}
