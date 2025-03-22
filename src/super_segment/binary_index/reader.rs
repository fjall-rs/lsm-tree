use byteorder::{BigEndian, ReadBytesExt};

pub struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn len(&self) -> usize {
        self.bytes.len() / std::mem::size_of::<u32>()
    }

    pub fn get(&self, idx: usize) -> u32 {
        let offset = idx * std::mem::size_of::<u32>();

        let mut bytes = self.bytes.get(offset..).expect("should be in array");
        bytes.read_u32::<BigEndian>().expect("should read")
    }
}
