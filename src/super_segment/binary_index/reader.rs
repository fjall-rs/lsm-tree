use byteorder::{LittleEndian, ReadBytesExt};

macro_rules! unwrappy {
    ($x:expr) => {
        // $x.expect("should read")

        unsafe { $x.unwrap_unchecked() }
    };
}

pub struct Reader<'a> {
    bytes: &'a [u8],
    step_size: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8], offset: u32, len: u32, step_size: u8) -> Self {
        let offset = offset as usize;
        let len = len as usize;
        let step_size = step_size as usize;

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

        let mut bytes = &self.bytes[offset..];

        if self.step_size == 2 {
            unwrappy!(bytes.read_u16::<LittleEndian>()).into()
        } else {
            unwrappy!(bytes.read_u32::<LittleEndian>()) as usize
        }
    }
}
