use byteorder::{LittleEndian, WriteBytesExt};

#[derive(Debug)]
pub struct Builder(Vec<u32>);

impl Builder {
    pub fn new(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn insert(&mut self, pos: u32) {
        self.0.push(pos);
    }

    pub fn write<W: std::io::Write>(&self, writer: &mut W) -> crate::Result<(u8, usize)> {
        // NOTE: We check if the pointers may fit in 16-bits
        // If so, we halve the index size by storing u16 instead of u32
        let step_size = {
            if u16::try_from(*self.0.last().expect("should not be empty")).is_ok() {
                2
            } else {
                4
            }
        };

        let len = self.0.len();

        if step_size == 2 {
            // Write u16 index
            for &offset in &self.0 {
                let offset = offset as u16;
                writer.write_u16::<LittleEndian>(offset)?;
            }
        } else {
            // Write u32 index
            for &offset in &self.0 {
                writer.write_u32::<LittleEndian>(offset)?;
            }
        }

        Ok((step_size, len))
    }
}
