use rand::RngCore;
use std::time::Instant;

const NUM_READS: usize = 100_000_000;

pub fn main() {
    let mut rng = rand::rng();

    let keys = (0..100_000_000u64)
        .map(|x| x.to_be_bytes())
        .collect::<Vec<_>>();

    for fpr in [0.25, 0.1, 0.01, 0.001, 0.000_1, 0.000_01, 0.000_001] {
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
                let mut hits = 0;

                for _ in 0..NUM_READS {
                    let mut key = [0; 16];
                    rng.fill_bytes(&mut key);
                    let hash = Builder::get_hash(&key);
                    
                    if filter.contains_hash(hash) {
                        hits += 1;
                    }
                }

                let real_fpr = hits as f64 / NUM_READS as f64;

                let filter_size_bytes = filter_bytes.len();
                println!(
                    r#"{{"real_fpr":{real_fpr},"key_count":{n},"target_fpr":{fpr},"impl":"standard","false_hits":{hits},"bytes":{filter_size_bytes}}}"#
                );
            }
        }

        {
            use lsm_tree::segment::filter::blocked_bloom::Builder;
            use lsm_tree::segment::filter::blocked_bloom::BlockedBloomFilterReader as Reader;

            let mut filter = Builder::with_fp_rate(n, fpr);

            for key in &keys {
                filter.set_with_hash(Builder::get_hash(key));
            }

            let filter_bytes = filter.build();
            let filter = Reader::new(&filter_bytes).unwrap();

            eprintln!("-- blocked n={n} e={fpr} --");

            {
                let mut hits = 0;

                for _ in 0..NUM_READS {
                    let mut key = [0; 16];
                    rng.fill_bytes(&mut key);
                    let hash = Builder::get_hash(&key);
                    
                    if filter.contains_hash(hash) {
                        hits += 1;
                    }
                }

                let real_fpr = hits as f64 / NUM_READS as f64;

                let filter_size_bytes = filter_bytes.len();
                println!(
                    r#"{{"real_fpr":{real_fpr},"key_count":{n},"target_fpr":{fpr},"impl":"blocked","false_hits":{hits},"bytes":{filter_size_bytes}}}"#
                );
            }
        }
    }
}
