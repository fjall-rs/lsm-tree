#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Unstructured};
use lsm_tree::table::util::compare_prefixed_slice;

fn main() {
    fuzz!(|data: &[u8]| {
        let mut unstructured = Unstructured::new(data);

        let Ok(prefix) = Vec::<u8>::arbitrary(&mut unstructured) else {
            return;
        };
        let Ok(suffix) = Vec::<u8>::arbitrary(&mut unstructured) else {
            return;
        };
        let Ok(needle) = Vec::<u8>::arbitrary(&mut unstructured) else {
            return;
        };

        let result = compare_prefixed_slice(&prefix, &suffix, &needle);

        let combined: Vec<u8> = prefix.iter().chain(suffix.iter()).copied().collect();
        let expected = combined.as_slice().cmp(&needle);

        assert_eq!(
            result, expected,
            "compare_prefixed_slice({:?}, {:?}, {:?}) = {:?}, but expected {:?}",
            prefix, suffix, needle, result, expected
        );
    });
}
