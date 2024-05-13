use super::block_index::block_handle::KeyedBlockHandle;
use std::collections::VecDeque;

#[derive(Debug)]
pub struct DataBlockHandleQueue(VecDeque<KeyedBlockHandle>);

impl From<VecDeque<KeyedBlockHandle>> for DataBlockHandleQueue {
    fn from(value: VecDeque<KeyedBlockHandle>) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for DataBlockHandleQueue {
    type Target = VecDeque<KeyedBlockHandle>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DataBlockHandleQueue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DataBlockHandleQueue {
    // TODO: see TLI
    fn get_start_block(&self, key: &[u8]) -> Option<(usize, &KeyedBlockHandle)> {
        let idx = self.partition_point(|x| &*x.end_key < key);

        let block = self.get(idx)?;

        if key > &*block.end_key {
            None
        } else {
            Some((idx, block))
        }
    }

    // TODO: see TLI
    fn get_end_block(&self, key: &[u8]) -> Option<(usize, &KeyedBlockHandle)> {
        let idx = self.partition_point(|x| &*x.end_key <= key) + 1;

        let block = self.get(idx)?;
        Some((idx, block))
    }

    pub fn truncate_start(&mut self, start_key: &[u8]) {
        let result = self.get_start_block(start_key);

        if let Some((idx, _)) = result {
            // IMPORTANT: Remove all handles lower and including eligible block handle
            //
            // If our block handles look like this:
            //
            // [a, b, c, d, e, f]
            //
            // and we want start at 'c', we would load data block 'c'
            // and get rid of a, b, resulting in:
            //
            // current_lo = c
            //
            // [d, e, f]
            self.drain(..idx);
        }
    }

    pub fn truncate_end(&mut self, end_key: &[u8]) {
        let result = self.get_end_block(end_key);

        if let Some((idx, _)) = result {
            // IMPORTANT: Remove all handles higher and including eligible block handle
            //
            // If our block handles look like this:
            //
            // [a, b, c, d, e, f]
            //
            // and we want end at 'c', we would load data block 'c'
            // and get rid of d, e, f, resulting in:
            //
            // current_hi = c
            //
            // [a, b, c]
            self.drain(idx..);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use test_log::test;

    fn bh(start_key: Arc<[u8]>, offset: u64, size: u32) -> KeyedBlockHandle {
        KeyedBlockHandle {
            end_key: start_key,
            offset,
            size,
        }
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn dbh_queue_start() {
        let queue = vec![
            bh("a".as_bytes().into(), 0, 0),
            bh("f".as_bytes().into(), 0, 0),
            bh("z".as_bytes().into(), 0, 0),
        ];
        let queue: VecDeque<KeyedBlockHandle> = queue.into();
        let mut queue = DataBlockHandleQueue::from(queue);

        queue.truncate_start(b"y");

        assert_eq!(queue.len(), 1);
        assert_eq!(&*queue.front().unwrap().end_key, b"z");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn dbh_queue_start_2() {
        let queue = vec![
            bh("a".as_bytes().into(), 0, 0),
            bh("f".as_bytes().into(), 0, 0),
            bh("z".as_bytes().into(), 0, 0),
        ];
        let queue: VecDeque<KeyedBlockHandle> = queue.into();
        let mut queue = DataBlockHandleQueue::from(queue);

        queue.truncate_start(b"b");

        assert_eq!(queue.len(), 2);
        assert_eq!(&*queue.front().unwrap().end_key, b"f");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn dbh_queue_end() {
        let queue = vec![
            bh("a".as_bytes().into(), 0, 0),
            bh("f".as_bytes().into(), 0, 0),
            bh("z".as_bytes().into(), 0, 0),
        ];
        let queue: VecDeque<KeyedBlockHandle> = queue.into();
        let mut queue = DataBlockHandleQueue::from(queue);

        queue.truncate_end(b"d");

        assert_eq!(queue.len(), 2);
        assert_eq!(&*queue.front().unwrap().end_key, b"a");
        assert_eq!(&*queue.back().unwrap().end_key, b"f");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn dbh_queue_end_2() {
        let queue = vec![
            bh("i".as_bytes().into(), 0, 0),
            bh("p".as_bytes().into(), 0, 0),
            bh("y".as_bytes().into(), 0, 0),
        ];
        let queue: VecDeque<KeyedBlockHandle> = queue.into();
        let mut queue = DataBlockHandleQueue::from(queue);

        queue.truncate_end(b"i");

        assert_eq!(queue.len(), 2);
        assert_eq!(&*queue.front().unwrap().end_key, b"i");
        assert_eq!(&*queue.back().unwrap().end_key, b"p");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn dbh_queue_double_ended() {
        let queue = vec![
            bh("a".as_bytes().into(), 0, 0),
            bh("f".as_bytes().into(), 0, 0),
            bh("k".as_bytes().into(), 0, 0),
            bh("p".as_bytes().into(), 0, 0),
            bh("y".as_bytes().into(), 0, 0),
        ];
        let queue: VecDeque<KeyedBlockHandle> = queue.into();
        let mut queue = DataBlockHandleQueue::from(queue);

        queue.truncate_start(b"g");
        queue.truncate_end(b"j");

        assert_eq!(queue.len(), 1);
        assert_eq!(&*queue.front().unwrap().end_key, b"k");
    }
}
