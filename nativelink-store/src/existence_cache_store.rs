// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use nativelink_config::stores::{EvictionPolicy, ExistenceCacheSpec as ExistenceCacheStoreConfig};
use nativelink_error::{error_if, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::evicting_map::{EvictingMap, LenEntry};
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo};

#[derive(Clone, Debug)]
struct ExistanceItem(u64);

impl LenEntry for ExistanceItem {
    #[inline]
    fn len(&self) -> u64 {
        self.0
    }

    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

#[derive(MetricsComponent)]
pub struct ExistenceCacheStore<I: InstantWrapper> {
    #[metric(group = "inner_store")]
    inner_store: Store,
    existence_cache: EvictingMap<DigestInfo, ExistanceItem, I>,
}

impl ExistenceCacheStore<SystemTime> {
    pub fn new(config: &ExistenceCacheStoreConfig, inner_store: Store) -> Arc<Self> {
        Self::new_with_time(config, inner_store, SystemTime::now())
    }
}

impl<I: InstantWrapper> ExistenceCacheStore<I> {
    pub fn new_with_time(
        config: &ExistenceCacheStoreConfig,
        inner_store: Store,
        anchor_time: I,
    ) -> Arc<Self> {
        let empty_policy = EvictionPolicy::default();
        let eviction_policy = config.eviction_policy.as_ref().unwrap_or(&empty_policy);
        Arc::new(Self {
            inner_store,
            existence_cache: EvictingMap::new(eviction_policy, anchor_time),
        })
    }

    pub async fn exists_in_cache(&self, digest: &DigestInfo) -> bool {
        let mut results = [None];
        self.existence_cache
            .sizes_for_keys([digest], &mut results[..], true /* peek */)
            .await;
        results[0].is_some()
    }

    pub async fn remove_from_cache(&self, digest: &DigestInfo) {
        self.existence_cache.remove(digest).await;
    }

    async fn inner_has_with_results(
        self: Pin<&Self>,
        keys: &[DigestInfo],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.existence_cache
            .sizes_for_keys(keys, results, true /* peek */)
            .await;

        let not_cached_keys: Vec<_> = keys
            .iter()
            .zip(results.iter())
            .filter_map(|(digest, result)| result.map_or_else(|| Some(digest.into()), |_| None))
            .collect();

        // Hot path optimization when all keys are cached.
        if not_cached_keys.is_empty() {
            return Ok(());
        }

        // Now query only the items not found in the cache.
        let mut inner_results = vec![None; not_cached_keys.len()];
        self.inner_store
            .has_with_results(&not_cached_keys, &mut inner_results)
            .await
            .err_tip(|| "In ExistenceCacheStore::inner_has_with_results")?;

        // Insert found from previous query into our cache.
        {
            // Note: Sadly due to some weird lifetime issues we need to collect here, but
            // in theory we don't actually need to collect.
            let inserts = not_cached_keys
                .iter()
                .zip(inner_results.iter())
                .filter_map(|(key, result)| {
                    result.map(|size| (key.borrow().into_digest(), ExistanceItem(size)))
                })
                .collect::<Vec<_>>();
            let _ = self.existence_cache.insert_many(inserts).await;
        }

        // Merge the results from the cache and the query.
        {
            let mut inner_results_iter = inner_results.into_iter();
            // We know at this point that any None in results was queried and will have
            // a result in inner_results_iter, so use this knowledge to fill in the results.
            for result in results.iter_mut() {
                if result.is_none() {
                    *result = inner_results_iter
                        .next()
                        .expect("has_with_results returned less results than expected");
                }
            }
            // Ensure that there was no logic error by ensuring our iterator is not empty.
            error_if!(
                inner_results_iter.next().is_some(),
                "has_with_results returned more results than expected"
            );
        }

        Ok(())
    }
}

#[async_trait]
impl<I: InstantWrapper> StoreDriver for ExistenceCacheStore<I> {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // TODO(allada) This is a bit of a hack to get around the lifetime issues with the
        // existence_cache. We need to convert the digests to owned values to be able to
        // insert them into the cache. In theory it should be able to elide this conversion
        // but it seems to be a bit tricky to get right.
        let digests: Vec<_> = digests
            .iter()
            .map(|key| key.borrow().into_digest())
            .collect();
        self.inner_has_with_results(&digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let digest = key.into_digest();
        let mut exists = [None];
        self.inner_has_with_results(&[digest], &mut exists)
            .await
            .err_tip(|| "In ExistenceCacheStore::update")?;
        if exists[0].is_some() {
            // We need to drain the reader to avoid the writer complaining that we dropped
            // the connection prematurely.
            reader
                .drain()
                .await
                .err_tip(|| "In ExistenceCacheStore::update")?;
            return Ok(());
        }
        let result = self.inner_store.update(digest, reader, size_info).await;
        if result.is_ok() {
            if let UploadSizeInfo::ExactSize(size) = size_info {
                let _ = self
                    .existence_cache
                    .insert(digest, ExistanceItem(size))
                    .await;
            }
        }
        result
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let digest = key.into_digest();
        let result = self
            .inner_store
            .get_part(digest, writer, offset, length)
            .await;
        if result.is_ok() {
            let _ = self
                .existence_cache
                .insert(digest, ExistanceItem(digest.size_bytes()))
                .await;
        }
        result
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn std::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send + 'static> {
        self
    }
}

#[async_trait]
impl<I: InstantWrapper> HealthStatusIndicator for ExistenceCacheStore<I> {
    fn get_name(&self) -> &'static str {
        "ExistenceCacheStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}
