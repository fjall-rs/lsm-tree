use byteorder::{BigEndian, WriteBytesExt};

#[derive(Debug)]
pub struct Builder(Vec<u32>);

impl Builder {
    pub fn new(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn insert(&mut self, pos: u32) {
        self.0.push(pos);
    }

    pub fn write<W: std::io::Write>(self, writer: &mut W) -> crate::Result<usize> {
        let len = self.0.len();

        for offset in self.0 {
            writer.write_u32::<BigEndian>(offset)?; // TODO: benchmark little endian on x86_64
        }

        Ok(len)
    }
}
