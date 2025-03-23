use byteorder::{BigEndian, ReadBytesExt};

type FencePtr = u32;

const FENCE_PTR_SIZE: usize = std::mem::size_of::<FencePtr>();

pub struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8], offset: usize, len: usize) -> Self {
        Self {
            bytes: &bytes[offset..(offset + len * FENCE_PTR_SIZE)],
        }
    }

    pub fn len(&self) -> usize {
        self.bytes.len() / FENCE_PTR_SIZE
    }

    pub fn get(&self, idx: usize) -> FencePtr {
        let offset = idx * FENCE_PTR_SIZE;

        let mut bytes = self.bytes.get(offset..).expect("should be in array");
        bytes.read_u32::<BigEndian>().expect("should read")
    }
}
