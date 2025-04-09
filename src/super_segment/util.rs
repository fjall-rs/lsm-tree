use std::cmp::Ordering;

pub fn longest_shared_prefix_length(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter()
        .zip(s2.iter())
        .take_while(|(c1, c2)| c1 == c2)
        .count()
}

// TODO: Fuzz test
pub fn compare_prefixed_slice(prefix: &[u8], suffix: &[u8], needle: &[u8]) -> Ordering {
    if needle.is_empty() {
        let combined_len = prefix.len() + suffix.len();

        return if combined_len > 0 {
            Ordering::Greater
        } else {
            Ordering::Equal
        };
    }

    match prefix.len().cmp(&needle.len()) {
        Ordering::Equal => match prefix.cmp(needle) {
            Ordering::Equal => {}
            ordering => return ordering,
        },
        Ordering::Greater => {
            // SAFETY: We know that the prefix is longer than the needle, so we can safely
            // truncate it to the needle's length
            #[allow(unsafe_code)]
            let prefix = unsafe { prefix.get_unchecked(0..needle.len()) };
            return prefix.cmp(needle);
        }
        Ordering::Less => {
            // SAFETY: We know that the needle is longer than the prefix, so we can safely
            // truncate it to the prefix's length
            #[allow(unsafe_code)]
            let needle = unsafe { needle.get_unchecked(0..prefix.len()) };

            match prefix.cmp(needle) {
                Ordering::Equal => {}
                ordering => return ordering,
            }
        }
    }

    // SAFETY: We know that the prefix is definitely not longer than the needle
    // so we can safely truncate
    #[allow(unsafe_code)]
    let needle = unsafe { needle.get_unchecked(prefix.len()..) };
    suffix.cmp(needle)
}
