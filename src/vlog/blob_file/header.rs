// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    checksum::{ChecksummedReader, ChecksummedWriter},
    coding::{Decode, Encode},
    Checksum, SeqNo,
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

pub const BLOB_HEADER_MAGIC: &[u8] = b"BLOB";

// NOTE:
// BLOB HEADER LAYOUT
//
// [MAGIC_BYTES; 4B]
// [Checksum; 16B]
// [Seqno; 8B]
// [key len; 2B]
// [real val len; 4B]
// [on-disk val len; 4B]
// [header checksum; 4B]
// [...key; ?]
// [...val; ?]
pub struct BlobHeader {
    pub data_checksum: u128,
    pub seqno: SeqNo,
    pub key_len: u16,
    pub uncompressed_len: u32,
    pub on_disk_len: u32,
}

impl BlobHeader {
    pub fn serialized_len() -> usize {
        BLOB_HEADER_MAGIC.len()
            + std::mem::size_of::<u128>() // Data checksum
            + std::mem::size_of::<u64>() // SeqNo
            + std::mem::size_of::<u16>() // Key length
            + std::mem::size_of::<u32>() // Real value length
            + std::mem::size_of::<u32>() // On-disk value length
            + std::mem::size_of::<u32>() // Header checksum
    }
}

impl Encode for BlobHeader {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        use byteorder::LE;

        let header_checksum = {
            let mut writer = ChecksummedWriter::new(&mut *writer);

            // Write header
            writer.write_all(BLOB_HEADER_MAGIC)?;

            // Write checksum
            writer.write_u128::<LittleEndian>(self.data_checksum)?;

            // Write seqno
            writer.write_u64::<LittleEndian>(self.seqno)?;

            // Write key len
            writer.write_u16::<LittleEndian>(self.key_len)?;

            // Write compressed (on-disk) value length
            writer.write_u32::<LittleEndian>(self.on_disk_len)?;

            // Write uncompressed value length
            writer.write_u32::<LittleEndian>(self.uncompressed_len)?;

            writer.checksum()
        };

        #[expect(
            clippy::cast_possible_truncation,
            reason = "we purposefully only use the lower 4 bytes as checksum"
        )]
        // Write 4-byte header checksum
        writer.write_u32::<LE>(header_checksum.into_u128() as u32)?;

        Ok(())
    }
}

impl Decode for BlobHeader {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        use byteorder::LE;

        let mut protected_reader = ChecksummedReader::new(reader);

        // Check magic
        let mut magic = [0u8; BLOB_HEADER_MAGIC.len()];
        protected_reader.read_exact(&mut magic)?;

        if magic != BLOB_HEADER_MAGIC {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        // Read data checksum
        let data_checksum = protected_reader.read_u128::<LE>()?;

        // Read seqno
        let seqno = protected_reader.read_u64::<LE>()?;

        // Read key len
        let key_len = protected_reader.read_u16::<LE>()?;

        // Read on-disk data length
        let on_disk_len = protected_reader.read_u32::<LE>()?;

        // Read uncompressed data length
        let uncompressed_len = protected_reader.read_u32::<LE>()?;

        #[expect(
            clippy::cast_possible_truncation,
            reason = "we purposefully only use the lower 4 bytes as checksum"
        )]
        // Get header checksum
        let got_checksum = protected_reader.checksum().into_u128() as u32;
        let got_checksum = Checksum::from_raw(u128::from(got_checksum));

        let reader = protected_reader.into_inner();

        // Read & check checksum
        let header_checksum: u128 = reader.read_u32::<LE>()?.into();
        let header_checksum = Checksum::from_raw(header_checksum);

        if header_checksum != got_checksum {
            return Err(crate::Error::ChecksumMismatch {
                got: got_checksum,
                expected: header_checksum,
            });
        }

        Ok(Self {
            data_checksum,
            seqno,
            key_len,
            uncompressed_len,
            on_disk_len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn blob_header_serde_roundtrip() -> crate::Result<()> {
        let header = BlobHeader {
            data_checksum: 5,
            seqno: 42,
            key_len: 123,
            uncompressed_len: 124_124_124,
            on_disk_len: 252_356,
        };

        let bytes = header.encode_into_vec();

        assert_eq!(bytes.len(), BlobHeader::serialized_len());

        let decoded = BlobHeader::decode_from(&mut &bytes[..])?;

        assert_eq!(decoded.data_checksum, header.data_checksum);
        assert_eq!(decoded.seqno, header.seqno);
        assert_eq!(decoded.key_len, header.key_len);
        assert_eq!(decoded.uncompressed_len, header.uncompressed_len);
        assert_eq!(decoded.on_disk_len, header.on_disk_len);

        Ok(())
    }
}
