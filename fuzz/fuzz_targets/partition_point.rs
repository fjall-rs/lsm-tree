#![no_main]
use libfuzzer_sys::{
    arbitrary::{Arbitrary, Unstructured},
    fuzz_target,
};
use lsm_tree::binary_search::partition_point;

fuzz_target!(|data: &[u8]| {
    let mut unstructured = Unstructured::new(data);

    if let Ok(mut items) = <Vec<u8> as Arbitrary>::arbitrary(&mut unstructured) {
        items.sort();
        items.dedup();

        let idx = partition_point(&items, |&x| x < 128);
        let std_pp_idx = items.partition_point(|&x| x < 128);
        assert_eq!(std_pp_idx, idx);
    }
});
