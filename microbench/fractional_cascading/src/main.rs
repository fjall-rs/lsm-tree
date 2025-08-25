use lsm_tree::{KeyRange, UserKey};
use rand::Rng;
use std::{sync::Arc, time::Instant};

#[cfg(feature = "fast_partition_point")]
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
        #[cfg(feature = "use_unsafe")]
        let item = unsafe { slice.get_unchecked(mid) };

        #[cfg(not(feature = "use_unsafe"))]
        let item = slice.get(mid).unwrap();

        if pred(item) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    left
}

pub fn get_segment_containing_key(segments: &[Arc<Segment>], key: &[u8]) -> Option<Arc<Segment>> {
    #[cfg(feature = "fast_partition_point")]
    let idx = partition_point(segments, |segment| segment.key_range.max() < &key);

    #[cfg(not(feature = "fast_partition_point"))]
    let idx = segments.partition_point(|segment| segment.key_range.max() < &key);

    segments
        .get(idx)
        .filter(|x| x.key_range.min() <= &key)
        .cloned()
}

#[derive(Clone, Debug)]
struct Segment {
    // id: String,
    is_lmax: bool,
    key_range: KeyRange,
    next: (u32, u32),
}

fn run(num_sst: usize) {
    eprintln!("Benchmarking {num_sst} SSTs");

    let keys = (0..num_sst * 2)
        .map(|x| x.to_be_bytes().to_vec())
        .collect::<Vec<_>>();

    let lowest_level = keys
        .chunks(2)
        .map(|x| KeyRange::new((UserKey::new(&x[0]), UserKey::new(&x[1]))))
        .enumerate()
        .map(|(idx, key_range)| {
            Arc::new(Segment {
                // id: format!("Lmax-{idx}"),
                is_lmax: true,
                key_range,
                next: (u32::MAX, u32::MAX),
            })
        })
        .collect::<Vec<_>>();

    let mut levels = vec![lowest_level];

    for _ in 0..10 {
        let next_level = &levels[0];

        if next_level.len() <= 10 {
            break;
        }

        let new_upper_level = next_level
            .chunks(10)
            .enumerate()
            .map(|(idx, x)| {
                let idx = idx as u32;
                let key_range = KeyRange::aggregate(x.iter().map(|x| &x.key_range));
                Arc::new(Segment {
                    // id: format!("L3-{idx}"),
                    is_lmax: false,
                    key_range,
                    next: (idx * 10, idx * 10 + 9),
                })
            })
            .collect::<Vec<_>>();

        levels.insert(0, new_upper_level);
    }

    for (idx, level) in levels.iter().enumerate() {
        eprintln!("L{:?} = {}", idx + 1, level.len());
    }

    let mut rng = rand::rng();

    const RUNS: usize = 20_000_000;

    let start = Instant::now();

    for _ in 0..RUNS {
        let idx = rng.random_range(0..keys.len());
        let key = &keys[idx];

        // NOTE: Naive search
        #[cfg(not(feature = "cascading"))]
        {
            for (_idx, level) in levels.iter().enumerate() {
                let _segment = get_segment_containing_key(&level, &*key).unwrap();
                // eprintln!("found {segment:?} in L{}", idx + 1);
            }
        }

        // NOTE: Search with fractional cascading
        #[cfg(feature = "cascading")]
        {
            let mut bounds: (u32, u32) = (u32::MAX, u32::MAX);

            for (idx, level) in levels.iter().enumerate() {
                let segment = if idx == 0 {
                    get_segment_containing_key(&level, &*key).expect("should find segment")
                } else {
                    let (lo, hi) = bounds;
                    let lo = lo as usize;
                    let hi = hi as usize;

                    #[cfg(feature = "use_unsafe")]
                    let slice = unsafe { level.get_unchecked(lo..=hi) };

                    #[cfg(not(feature = "use_unsafe"))]
                    let slice = level.get(lo..=hi).unwrap();

                    get_segment_containing_key(slice, &*key).expect("should find segment")
                };
                // eprintln!("found {segment:?} in L{}", idx + 1);

                bounds = segment.next;
            }
        }
    }

    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos();
    let per_run = ns / RUNS as u128;

    #[cfg(feature = "cascading")]
    let cascading = true;

    #[cfg(not(feature = "cascading"))]
    let cascading = false;

    #[cfg(feature = "fast_partition_point")]
    let fast_partition_point = true;

    #[cfg(not(feature = "fast_partition_point"))]
    let fast_partition_point = false;

    #[cfg(feature = "use_unsafe")]
    let used_unsafe = true;

    #[cfg(not(feature = "use_unsafe"))]
    let used_unsafe = false;

    println!(
        "{{\"lmax_ssts\": {num_sst}, \"ns\":{per_run}, \"unsafe\":{used_unsafe}, \"std_partition_point\":{}, \"cascading\":{cascading} }}",
        !fast_partition_point,
    );
}

fn main() {
    for lmax_sst_count in [100, 500, 1_000, 2_000, 4_000, 10_000] {
        run(lmax_sst_count);
    }
}
