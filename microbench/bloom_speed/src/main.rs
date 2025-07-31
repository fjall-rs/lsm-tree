use rand::{Rng, RngCore};
use std::time::Instant;

const NUM_READS: usize = 100_000_000;

pub fn main() {
    let mut rng = rand::rng();

    let keys = (0..100_000_000u128)
        .map(|x| x.to_be_bytes())
        .collect::<Vec<_>>();

    for fpr in [0.25, 0.1, 0.01, 0.001, 0.0001, 0.00001, 0.000001] {
        let n = keys.len();

        {
            use lsm_tree::segment::filter::standard_bloom::Builder;
            use lsm_tree::segment::filter::standard_bloom::StandardBloomFilterReader as Reader;

            let mut filter = Builder::with_fp_rate(n, fpr);

            for key in &keys {
                filter.set_with_hash(Builder::get_hash(key));
            }

            let filter_bytes = filter.build();
            let filter = Reader::new(&filter_bytes).unwrap();

            eprintln!("-- standard n={n} e={fpr} --");

            {
                let start = Instant::now();

                for _ in 0..NUM_READS {
                    use rand::seq::IndexedRandom;

                    // let sample = keys.choose(&mut rng).unwrap();

                    let mut sample = [0; 8];
                    rng.fill(&mut sample);

                    let hash = Builder::get_hash(&sample);
                    filter.contains_hash(hash);
                    // assert!(filter.contains_hash(hash));
                }

                let ns = start.elapsed().as_nanos();
                let per_read = ns / NUM_READS as u128;
                eprintln!("  true positive in {per_read}ns");

                #[cfg(feature = "use_unsafe")]
                let use_unsafe = true;

                #[cfg(not(feature = "use_unsafe"))]
                let use_unsafe = false;

                let filter_size_bytes = filter_bytes.len();
                println!(
                    r#"{{"key_count":{n},"fpr":{fpr},"impl":"standard","ns":{per_read},"bytes":{filter_size_bytes},"unsafe":{use_unsafe}}}"#
                );
            }
        }

        {
            use lsm_tree::segment::filter::blocked_bloom::BlockedBloomFilterReader as Reader;
            use lsm_tree::segment::filter::blocked_bloom::Builder;

            let mut filter = Builder::with_fp_rate(n, fpr);

            for key in &keys {
                filter.set_with_hash(Builder::get_hash(key));
            }

            let filter_bytes = filter.build();
            let filter = Reader::new(&filter_bytes).unwrap();

            eprintln!("-- blocked n={n} e={fpr} --");

            {
                let start = Instant::now();

                for _ in 0..NUM_READS {
                    use rand::seq::IndexedRandom;

                    // let sample = keys.choose(&mut rng).unwrap();

                    let mut sample = [0; 8];
                    rng.fill(&mut sample);

                    let hash = Builder::get_hash(&sample);
                    filter.contains_hash(hash);

                    // assert!(filter.contains_hash(hash));
                }

                let ns = start.elapsed().as_nanos();
                let per_read = ns / NUM_READS as u128;
                eprintln!("  true positive in {per_read}ns");

                #[cfg(feature = "use_unsafe")]
                let use_unsafe = true;

                #[cfg(not(feature = "use_unsafe"))]
                let use_unsafe = false;

                let filter_size_bytes = filter_bytes.len();
                println!(
                    r#"{{"key_count":{n},"fpr":{fpr},"impl":"blocked","ns":{per_read},"bytes":{filter_size_bytes},"unsafe":{use_unsafe}}}"#
                );
            }
        }
    }
}
