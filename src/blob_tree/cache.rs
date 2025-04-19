use crate::Cache;
use std::sync::Arc;
use value_log::BlobCache;

#[derive(Clone)]
pub struct MyBlobCache(pub(crate) Arc<Cache>);

impl BlobCache for MyBlobCache {
    fn get(
        &self,
        vlog_id: value_log::ValueLogId,
        vhandle: &value_log::ValueHandle,
    ) -> Option<value_log::UserValue> {
        todo!()

        // self.0.get_blob(vlog_id, vhandle)
    }

    fn insert(
        &self,
        vlog_id: value_log::ValueLogId,
        vhandle: &value_log::ValueHandle,
        value: value_log::UserValue,
    ) {
        todo!()

        // self.0.insert_blob(vlog_id, vhandle, value);
    }
}
