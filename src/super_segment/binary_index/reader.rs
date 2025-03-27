use byteorder::{LittleEndian, ReadBytesExt};

pub struct Reader<'a> {
    bytes: &'a [u8],
    step_size: usize,
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8], offset: usize, len: usize, step_size: usize) -> Self {
        Self {
            bytes: &bytes[offset..(offset + len * step_size)],
            step_size,
        }
    }

    pub fn len(&self) -> usize {
        self.bytes.len() / self.step_size
    }

    pub(crate) fn get(&self, idx: usize) -> usize {
        let offset = idx * self.step_size;

        let mut bytes = self.bytes.get(offset..).expect("should be in array");

        if self.step_size == 2 {
            bytes
                .read_u16::<LittleEndian>()
                .expect("should read")
                .into()
        } else {
            bytes.read_u32::<LittleEndian>().expect("should read") as usize
        }
    }
}
