use std::{future::Future, sync::Arc};

use moka::policy::EvictionPolicy;

use crate::array::{group::ArrayGroupData, util::ZeroAccessor};

pub type BatchIndex = usize;
pub type SharedCacheKey = (String, BatchIndex);

#[derive(Clone)]
pub struct SharedCache {
    cache: Arc<moka::future::Cache<SharedCacheKey, ZeroAccessor<ArrayGroupData>>>,
}

impl SharedCache {
    pub fn new(max_size: usize) -> Self {
        let cache = moka::future::Cache::builder()
            .max_capacity(max_size as u64) // Convert KB to bytes
            .weigher(
                |_key: &SharedCacheKey, value: &ZeroAccessor<ArrayGroupData>| {
                    value.underlying_bytes().len() as u32
                },
            )
            .eviction_policy(EvictionPolicy::lru())
            .time_to_idle(std::time::Duration::from_secs(30)) // 30 seconds idle time
            .build();
        Self {
            cache: Arc::new(cache),
        }
    }

    pub async fn get_or_insert_with<F, Fut>(
        &self,
        key: SharedCacheKey,
        loader: F,
    ) -> ZeroAccessor<ArrayGroupData>
    where
        F: FnOnce(&SharedCacheKey) -> Fut + Send + 'static,
        Fut: Future<Output = ZeroAccessor<ArrayGroupData>> + Send + 'static,
    {
        // moka’s “load‐through” API
        self.cache
            .get_with(key.clone(), {
                let key_clone = key.clone();
                async move { loader(&key_clone).await }
            })
            .await
    }
}
