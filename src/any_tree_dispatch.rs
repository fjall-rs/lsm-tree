impl AbstractTree for AnyTree {
    #[inline]
    fn print_trace(&self, __enum_dispatch_arg_0: &[u8]) -> crate::Result<()> {
        match self {
            Self::Standard(inner) => AbstractTree::print_trace(inner, __enum_dispatch_arg_0),
            Self::Blob(inner) => AbstractTree::print_trace(inner, __enum_dispatch_arg_0),
        }
    }
    #[inline]
    fn table_file_cache_size(&self) -> usize {
        match self {
            Self::Standard(inner) => AbstractTree::table_file_cache_size(inner),
            Self::Blob(inner) => AbstractTree::table_file_cache_size(inner),
        }
    }
    #[inline]
    fn version_memtable_size_sum(&self) -> u64 {
        match self {
            Self::Standard(inner) => AbstractTree::version_memtable_size_sum(inner),
            Self::Blob(inner) => AbstractTree::version_memtable_size_sum(inner),
        }
    }
    #[inline]
    fn next_table_id(&self) -> TableId {
        match self {
            Self::Standard(inner) => AbstractTree::next_table_id(inner),
            Self::Blob(inner) => AbstractTree::next_table_id(inner),
        }
    }
    #[inline]
    fn id(&self) -> crate::TreeId {
        match self {
            Self::Standard(inner) => AbstractTree::id(inner),
            Self::Blob(inner) => AbstractTree::id(inner),
        }
    }
    #[inline]
    fn get_internal_entry(
        &self,
        __enum_dispatch_arg_0: &[u8],
        __enum_dispatch_arg_1: SeqNo,
    ) -> crate::Result<Option<InternalValue>> {
        match self {
            Self::Standard(inner) => AbstractTree::get_internal_entry(
                inner,
                __enum_dispatch_arg_0,
                __enum_dispatch_arg_1,
            ),
            Self::Blob(inner) => AbstractTree::get_internal_entry(
                inner,
                __enum_dispatch_arg_0,
                __enum_dispatch_arg_1,
            ),
        }
    }
    #[inline]
    fn current_version(&self) -> Version {
        match self {
            Self::Standard(inner) => AbstractTree::current_version(inner),
            Self::Blob(inner) => AbstractTree::current_version(inner),
        }
    }
    #[inline]
    fn get_version_history_lock(&self) -> RwLockWriteGuard<'_, crate::version::SuperVersions> {
        match self {
            Self::Standard(inner) => AbstractTree::get_version_history_lock(inner),
            Self::Blob(inner) => AbstractTree::get_version_history_lock(inner),
        }
    }
    #[inline]
    fn flush_active_memtable(&self, __enum_dispatch_arg_0: SeqNo) -> crate::Result<()> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::flush_active_memtable(inner, __enum_dispatch_arg_0)
            }
            Self::Blob(inner) => {
                AbstractTree::flush_active_memtable(inner, __enum_dispatch_arg_0)
            }
        }
    }
    #[inline]
    fn flush(
        &self,
        __enum_dispatch_arg_0: &MutexGuard<'_, ()>,
        __enum_dispatch_arg_1: SeqNo,
    ) -> crate::Result<Option<u64>> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::flush(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::flush(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn iter(
        &self,
        __enum_dispatch_arg_0: SeqNo,
        __enum_dispatch_arg_1: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::iter(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::iter(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn prefix<K: AsRef<[u8]>>(
        &self,
        __enum_dispatch_arg_0: K,
        __enum_dispatch_arg_1: SeqNo,
        __enum_dispatch_arg_2: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        match self {
            Self::Standard(inner) => AbstractTree::prefix::<K>(
                inner,
                __enum_dispatch_arg_0,
                __enum_dispatch_arg_1,
                __enum_dispatch_arg_2,
            ),
            Self::Blob(inner) => AbstractTree::prefix::<K>(
                inner,
                __enum_dispatch_arg_0,
                __enum_dispatch_arg_1,
                __enum_dispatch_arg_2,
            ),
        }
    }
    #[inline]
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        __enum_dispatch_arg_0: R,
        __enum_dispatch_arg_1: SeqNo,
        __enum_dispatch_arg_2: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        match self {
            Self::Standard(inner) => AbstractTree::range::<K, R>(
                inner,
                __enum_dispatch_arg_0,
                __enum_dispatch_arg_1,
                __enum_dispatch_arg_2,
            ),
            Self::Blob(inner) => AbstractTree::range::<K, R>(
                inner,
                __enum_dispatch_arg_0,
                __enum_dispatch_arg_1,
                __enum_dispatch_arg_2,
            ),
        }
    }
    #[inline]
    fn tombstone_count(&self) -> u64 {
        match self {
            Self::Standard(inner) => AbstractTree::tombstone_count(inner),
            Self::Blob(inner) => AbstractTree::tombstone_count(inner),
        }
    }
    #[inline]
    fn weak_tombstone_count(&self) -> u64 {
        match self {
            Self::Standard(inner) => AbstractTree::weak_tombstone_count(inner),
            Self::Blob(inner) => AbstractTree::weak_tombstone_count(inner),
        }
    }
    #[inline]
    fn weak_tombstone_reclaimable_count(&self) -> u64 {
        match self {
            Self::Standard(inner) => AbstractTree::weak_tombstone_reclaimable_count(inner),
            Self::Blob(inner) => AbstractTree::weak_tombstone_reclaimable_count(inner),
        }
    }
    #[inline]
    fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        __enum_dispatch_arg_0: R,
    ) -> crate::Result<()> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::drop_range::<K, R>(inner, __enum_dispatch_arg_0)
            }
            Self::Blob(inner) => AbstractTree::drop_range::<K, R>(inner, __enum_dispatch_arg_0),
        }
    }
    #[inline]
    fn clear(&self) -> crate::Result<()> {
        match self {
            Self::Standard(inner) => AbstractTree::clear(inner),
            Self::Blob(inner) => AbstractTree::clear(inner),
        }
    }
    #[inline]
    fn major_compact(
        &self,
        __enum_dispatch_arg_0: u64,
        __enum_dispatch_arg_1: SeqNo,
    ) -> crate::Result<()> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::major_compact(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::major_compact(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn stale_blob_bytes(&self) -> u64 {
        match self {
            Self::Standard(inner) => AbstractTree::stale_blob_bytes(inner),
            Self::Blob(inner) => AbstractTree::stale_blob_bytes(inner),
        }
    }
    #[inline]
    fn filter_size(&self) -> u64 {
        match self {
            Self::Standard(inner) => AbstractTree::filter_size(inner),
            Self::Blob(inner) => AbstractTree::filter_size(inner),
        }
    }
    #[inline]
    fn pinned_filter_size(&self) -> usize {
        match self {
            Self::Standard(inner) => AbstractTree::pinned_filter_size(inner),
            Self::Blob(inner) => AbstractTree::pinned_filter_size(inner),
        }
    }
    #[inline]
    fn pinned_block_index_size(&self) -> usize {
        match self {
            Self::Standard(inner) => AbstractTree::pinned_block_index_size(inner),
            Self::Blob(inner) => AbstractTree::pinned_block_index_size(inner),
        }
    }
    #[inline]
    fn version_free_list_len(&self) -> usize {
        match self {
            Self::Standard(inner) => AbstractTree::version_free_list_len(inner),
            Self::Blob(inner) => AbstractTree::version_free_list_len(inner),
        }
    }
    #[cfg(feature = "metrics")]
    #[inline]
    fn metrics(&self) -> &Arc<crate::Metrics> {
        match self {
            Self::Standard(inner) => AbstractTree::metrics(inner),
            Self::Blob(inner) => AbstractTree::metrics(inner),
        }
    }
    #[inline]
    fn get_flush_lock(&self) -> MutexGuard<'_, ()> {
        match self {
            Self::Standard(inner) => AbstractTree::get_flush_lock(inner),
            Self::Blob(inner) => AbstractTree::get_flush_lock(inner),
        }
    }
    #[warn(clippy::type_complexity)]
    #[inline]
    fn flush_to_tables(
        &self,
        __enum_dispatch_arg_0: impl Iterator<Item = crate::Result<InternalValue>>,
    ) -> crate::Result<Option<FlushToTablesResult>> {
        match self {
            Self::Standard(inner) => AbstractTree::flush_to_tables(inner, __enum_dispatch_arg_0),
            Self::Blob(inner) => AbstractTree::flush_to_tables(inner, __enum_dispatch_arg_0),
        }
    }
    #[inline]
    fn register_tables(
        &self,
        __enum_dispatch_arg_0: &[Table],
        __enum_dispatch_arg_1: Option<&[BlobFile]>,
        __enum_dispatch_arg_2: Option<crate::blob_tree::FragmentationMap>,
        __enum_dispatch_arg_3: &[crate::tree::inner::MemtableId],
        __enum_dispatch_arg_4: SeqNo,
    ) -> crate::Result<()> {
        match self {
            Self::Standard(inner) => AbstractTree::register_tables(
                inner,
                __enum_dispatch_arg_0,
                __enum_dispatch_arg_1,
                __enum_dispatch_arg_2,
                __enum_dispatch_arg_3,
                __enum_dispatch_arg_4,
            ),
            Self::Blob(inner) => AbstractTree::register_tables(
                inner,
                __enum_dispatch_arg_0,
                __enum_dispatch_arg_1,
                __enum_dispatch_arg_2,
                __enum_dispatch_arg_3,
                __enum_dispatch_arg_4,
            ),
        }
    }
    #[inline]
    fn clear_active_memtable(&self) {
        match self {
            Self::Standard(inner) => AbstractTree::clear_active_memtable(inner),
            Self::Blob(inner) => AbstractTree::clear_active_memtable(inner),
        }
    }
    #[inline]
    fn sealed_memtable_count(&self) -> usize {
        match self {
            Self::Standard(inner) => AbstractTree::sealed_memtable_count(inner),
            Self::Blob(inner) => AbstractTree::sealed_memtable_count(inner),
        }
    }
    #[inline]
    fn compact(
        &self,
        __enum_dispatch_arg_0: Arc<dyn crate::compaction::CompactionStrategy>,
        __enum_dispatch_arg_1: SeqNo,
    ) -> crate::Result<()> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::compact(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::compact(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn get_next_table_id(&self) -> TableId {
        match self {
            Self::Standard(inner) => AbstractTree::get_next_table_id(inner),
            Self::Blob(inner) => AbstractTree::get_next_table_id(inner),
        }
    }
    #[inline]
    fn tree_config(&self) -> &Config {
        match self {
            Self::Standard(inner) => AbstractTree::tree_config(inner),
            Self::Blob(inner) => AbstractTree::tree_config(inner),
        }
    }
    #[inline]
    fn get_highest_seqno(&self) -> Option<SeqNo> {
        match self {
            Self::Standard(inner) => AbstractTree::get_highest_seqno(inner),
            Self::Blob(inner) => AbstractTree::get_highest_seqno(inner),
        }
    }
    #[inline]
    fn active_memtable(&self) -> Arc<Memtable> {
        match self {
            Self::Standard(inner) => AbstractTree::active_memtable(inner),
            Self::Blob(inner) => AbstractTree::active_memtable(inner),
        }
    }
    #[inline]
    fn tree_type(&self) -> crate::TreeType {
        match self {
            Self::Standard(inner) => AbstractTree::tree_type(inner),
            Self::Blob(inner) => AbstractTree::tree_type(inner),
        }
    }
    #[inline]
    fn rotate_memtable(&self) -> Option<Arc<Memtable>> {
        match self {
            Self::Standard(inner) => AbstractTree::rotate_memtable(inner),
            Self::Blob(inner) => AbstractTree::rotate_memtable(inner),
        }
    }
    #[inline]
    fn table_count(&self) -> usize {
        match self {
            Self::Standard(inner) => AbstractTree::table_count(inner),
            Self::Blob(inner) => AbstractTree::table_count(inner),
        }
    }
    #[inline]
    fn level_table_count(&self, __enum_dispatch_arg_0: usize) -> Option<usize> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::level_table_count(inner, __enum_dispatch_arg_0)
            }
            Self::Blob(inner) => AbstractTree::level_table_count(inner, __enum_dispatch_arg_0),
        }
    }
    #[inline]
    fn l0_run_count(&self) -> usize {
        match self {
            Self::Standard(inner) => AbstractTree::l0_run_count(inner),
            Self::Blob(inner) => AbstractTree::l0_run_count(inner),
        }
    }
    #[inline]
    fn blob_file_count(&self) -> usize {
        match self {
            Self::Standard(inner) => AbstractTree::blob_file_count(inner),
            Self::Blob(inner) => AbstractTree::blob_file_count(inner),
        }
    }
    #[inline]
    fn approximate_len(&self) -> usize {
        match self {
            Self::Standard(inner) => AbstractTree::approximate_len(inner),
            Self::Blob(inner) => AbstractTree::approximate_len(inner),
        }
    }
    #[inline]
    fn disk_space(&self) -> u64 {
        match self {
            Self::Standard(inner) => AbstractTree::disk_space(inner),
            Self::Blob(inner) => AbstractTree::disk_space(inner),
        }
    }
    #[inline]
    fn get_highest_memtable_seqno(&self) -> Option<SeqNo> {
        match self {
            Self::Standard(inner) => AbstractTree::get_highest_memtable_seqno(inner),
            Self::Blob(inner) => AbstractTree::get_highest_memtable_seqno(inner),
        }
    }
    #[inline]
    fn get_highest_persisted_seqno(&self) -> Option<SeqNo> {
        match self {
            Self::Standard(inner) => AbstractTree::get_highest_persisted_seqno(inner),
            Self::Blob(inner) => AbstractTree::get_highest_persisted_seqno(inner),
        }
    }
    #[inline]
    fn len(
        &self,
        __enum_dispatch_arg_0: SeqNo,
        __enum_dispatch_arg_1: Option<(Arc<Memtable>, SeqNo)>,
    ) -> crate::Result<usize> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::len(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::len(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn is_empty(
        &self,
        __enum_dispatch_arg_0: SeqNo,
        __enum_dispatch_arg_1: Option<(Arc<Memtable>, SeqNo)>,
    ) -> crate::Result<bool> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::is_empty(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::is_empty(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn first_key_value(
        &self,
        __enum_dispatch_arg_0: SeqNo,
        __enum_dispatch_arg_1: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Option<IterGuardImpl> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::first_key_value(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::first_key_value(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn last_key_value(
        &self,
        __enum_dispatch_arg_0: SeqNo,
        __enum_dispatch_arg_1: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Option<IterGuardImpl> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::last_key_value(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::last_key_value(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn size_of<K: AsRef<[u8]>>(
        &self,
        __enum_dispatch_arg_0: K,
        __enum_dispatch_arg_1: SeqNo,
    ) -> crate::Result<Option<u32>> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::size_of::<K>(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::size_of::<K>(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn get<K: AsRef<[u8]>>(
        &self,
        __enum_dispatch_arg_0: K,
        __enum_dispatch_arg_1: SeqNo,
    ) -> crate::Result<Option<UserValue>> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::get::<K>(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::get::<K>(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn contains_key<K: AsRef<[u8]>>(
        &self,
        __enum_dispatch_arg_0: K,
        __enum_dispatch_arg_1: SeqNo,
    ) -> crate::Result<bool> {
        match self {
            Self::Standard(inner) => {
                AbstractTree::contains_key::<K>(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::contains_key::<K>(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        __enum_dispatch_arg_0: K,
        __enum_dispatch_arg_1: V,
        __enum_dispatch_arg_2: SeqNo,
    ) -> (u64, u64) {
        match self {
            Self::Standard(inner) => AbstractTree::insert::<K, V>(
                inner,
                __enum_dispatch_arg_0,
                __enum_dispatch_arg_1,
                __enum_dispatch_arg_2,
            ),
            Self::Blob(inner) => AbstractTree::insert::<K, V>(
                inner,
                __enum_dispatch_arg_0,
                __enum_dispatch_arg_1,
                __enum_dispatch_arg_2,
            ),
        }
    }
    #[inline]
    fn remove<K: Into<UserKey>>(
        &self,
        __enum_dispatch_arg_0: K,
        __enum_dispatch_arg_1: SeqNo,
    ) -> (u64, u64) {
        match self {
            Self::Standard(inner) => {
                AbstractTree::remove::<K>(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::remove::<K>(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
    #[inline]
    fn remove_weak<K: Into<UserKey>>(
        &self,
        __enum_dispatch_arg_0: K,
        __enum_dispatch_arg_1: SeqNo,
    ) -> (u64, u64) {
        match self {
            Self::Standard(inner) => {
                AbstractTree::remove_weak::<K>(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
            Self::Blob(inner) => {
                AbstractTree::remove_weak::<K>(inner, __enum_dispatch_arg_0, __enum_dispatch_arg_1)
            }
        }
    }
}
