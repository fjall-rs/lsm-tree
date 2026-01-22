use crate::{
    blob_tree::FragmentationMap,
    fs::{FileSystem, StdFileSystem},
    vlog::{BlobFile, BlobFileId},
};
use std::collections::BTreeMap;

pub struct BlobFileList<F: FileSystem = StdFileSystem>(BTreeMap<BlobFileId, BlobFile<F>>);

impl<F: FileSystem> Clone for BlobFileList<F> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<F: FileSystem> Default for BlobFileList<F> {
    fn default() -> Self {
        Self(BTreeMap::default())
    }
}

impl<F: FileSystem> BlobFileList<F> {
    pub fn new(blob_files: BTreeMap<BlobFileId, BlobFile<F>>) -> Self {
        Self(blob_files)
    }

    /// On-disk size
    pub fn on_disk_size(&self) -> u64 {
        self.iter().map(|bf| bf.0.meta.total_compressed_bytes).sum()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn extend<I: IntoIterator<Item = (BlobFileId, BlobFile<F>)>>(&mut self, iter: I) {
        self.0.extend(iter);
    }

    pub fn contains_key(&self, key: BlobFileId) -> bool {
        self.0.contains_key(&key)
    }

    pub fn prune_dead(&mut self, gc_stats: &FragmentationMap) -> Vec<BlobFile<F>> {
        self.0
            .extract_if(.., |_, blob_file| blob_file.is_dead(gc_stats))
            .map(|(_, v)| v)
            .collect()
    }

    pub fn insert(&mut self, key: BlobFileId, value: BlobFile<F>) {
        self.0.insert(key, value);
    }

    pub fn remove(&mut self, key: BlobFileId) -> Option<BlobFile<F>> {
        self.0.remove(&key)
    }

    pub fn iter(&self) -> impl Iterator<Item = &BlobFile<F>> {
        self.0.values()
    }

    pub fn list_ids(&self) -> impl Iterator<Item = &BlobFileId> {
        self.0.keys()
    }

    pub fn get(&self, key: BlobFileId) -> Option<&BlobFile<F>> {
        self.0.get(&key)
    }
}
