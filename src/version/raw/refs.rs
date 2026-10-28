use crate::{vlog::BlobFileId, Checksum, TableId};

/// Reference to a table
///
/// Includes full file checksum, which is stored in a manifest (e.g. fjall's db manifest)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub struct TableRef {
    pub(crate) id: TableId,
    pub(crate) checksum: Checksum,
}

impl TableRef {
    /// Creates a table reference from an ID and a full-file checksum.
    #[must_use]
    pub fn new(id: TableId, checksum: Checksum) -> Self {
        Self { id, checksum }
    }

    /// Returns the table ID.
    #[must_use]
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Returns the table's full-file checksum.
    #[must_use]
    pub fn checksum(&self) -> Checksum {
        self.checksum
    }
}

impl From<&crate::Table> for TableRef {
    fn from(table: &crate::Table) -> Self {
        Self {
            id: table.id(),
            checksum: table.checksum(),
        }
    }
}

/// Reference to a blob file
///
/// Includes full file checksum, which is stored in a manifest (e.g. fjall's db manifest)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub struct BlobFileRef {
    pub(crate) id: BlobFileId,
    pub(crate) checksum: Checksum,
}

impl BlobFileRef {
    /// Creates a blob-file reference from an ID and a full-file checksum.
    #[must_use]
    pub fn new(id: BlobFileId, checksum: Checksum) -> Self {
        Self { id, checksum }
    }

    /// Returns the blob-file ID.
    #[must_use]
    pub fn id(&self) -> BlobFileId {
        self.id
    }

    /// Returns the blob file's full-file checksum.
    #[must_use]
    pub fn checksum(&self) -> Checksum {
        self.checksum
    }
}

impl From<&crate::BlobFile> for BlobFileRef {
    fn from(blob_file: &crate::BlobFile) -> Self {
        Self {
            id: blob_file.id(),
            checksum: blob_file.checksum(),
        }
    }
}

#[cfg(test)]
impl serde::Serialize for Checksum {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u128(self.into_u128())
    }
}

#[cfg(test)]
impl<'de> serde::Deserialize<'de> for Checksum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <u128 as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Self::from_raw(value))
    }
}
