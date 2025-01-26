use lsm_tree::prefix_extractor::PrefixExtractor;

pub struct TestPrefixExtractor {
    prefix_len: usize,
}

impl TestPrefixExtractor {
    #[must_use]
    pub fn new(prefix_len: usize) -> Self {
        Self { prefix_len }
    }
}

impl PrefixExtractor for TestPrefixExtractor {
    fn in_domain(&self, key: &[u8]) -> bool {
        key.len() > self.prefix_len
    }

    fn transform<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        key.get(0..self.prefix_len)
            .expect("prefix len out of range, in_domain should be used first")
    }
}
