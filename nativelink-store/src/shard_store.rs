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

use std::hash::{DefaultHasher, Hasher};
use std::ops::BitXor;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{FuturesUnordered, TryStreamExt};
use nativelink_error::{error_if, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::health_utils::{default_health_status_indicator, HealthStatusIndicator};
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo};

#[derive(MetricsComponent)]
struct StoreAndWeight {
    #[metric(help = "The weight of the store")]
    weight: u32,
    #[metric(help = "The underlying store")]
    store: Store,
}

#[derive(MetricsComponent)]
pub struct ShardStore {
    // The weights will always be in ascending order a specific store is choosen based on the
    // the hash of the key hash that is nearest-binary searched using the u32 as the index.
    #[metric(
        group = "stores",
        help = "The weights and stores that are used to determine which store to use"
    )]
    weights_and_stores: Vec<StoreAndWeight>,
}

impl ShardStore {
    pub fn new(
        config: &nativelink_config::stores::ShardStore,
        stores: Vec<Store>,
    ) -> Result<Arc<Self>, Error> {
        error_if!(
            config.stores.len() != stores.len(),
            "Config shards do not match stores length"
        );
        error_if!(
            config.stores.is_empty(),
            "ShardStore must have at least one store"
        );
        let total_weight: u64 = config
            .stores
            .iter()
            .map(|shard_config| shard_config.weight.unwrap_or(1) as u64)
            .sum();
        let mut weights: Vec<u32> = config
            .stores
            .iter()
            .map(|shard_config| {
                u32::try_from(
                    u32::MAX as u64 * shard_config.weight.unwrap_or(1) as u64 / total_weight,
                )
                .expect("Weight is too large")
            })
            .scan(0, |state, weight| {
                *state += weight;
                Some(*state)
            })
            .collect();
        // Our last item should always be the max.
        *weights.last_mut().unwrap() = u32::MAX;
        Ok(Arc::new(Self {
            weights_and_stores: weights
                .into_iter()
                .zip(stores)
                .map(|(weight, store)| StoreAndWeight { weight, store })
                .collect(),
        }))
    }

    fn get_store_index(&self, store_key: &StoreKey) -> usize {
        let key = match store_key {
            StoreKey::Digest(digest) => {
                // Quote from std primitive array documentation:
                //     Array’s try_from(slice) implementations (and the corresponding slice.try_into()
                //     array implementations) succeed if the input slice length is the same as the result
                //     array length. They optimize especially well when the optimizer can easily determine
                //     the slice length, e.g. <[u8; 4]>::try_from(&slice[4..8]).unwrap(). Array implements
                //     TryFrom returning.
                let size_bytes = digest.size_bytes().to_le_bytes();
                0.bitxor(u32::from_le_bytes(
                    digest.packed_hash()[0..4].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[4..8].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[8..12].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[12..16].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[16..20].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[20..24].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[24..28].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[28..32].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(size_bytes[0..4].try_into().unwrap()))
                .bitxor(u32::from_le_bytes(size_bytes[4..8].try_into().unwrap()))
            }
            StoreKey::Str(s) => {
                let mut hasher = DefaultHasher::new();
                hasher.write(s.as_bytes());
                let key_u64 = hasher.finish();
                (key_u64 >> 32) as u32 // We only need the top 32 bits.
            }
        };
        self.weights_and_stores
            .binary_search_by_key(&key, |item| item.weight)
            .unwrap_or_else(|index| index)
    }

    fn get_store(&self, key: &StoreKey) -> &Store {
        let index = self.get_store_index(key);
        &self.weights_and_stores[index].store
    }
}

#[async_trait]
impl StoreDriver for ShardStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        if keys.len() == 1 {
            // Hot path: It is very common to lookup only one key.
            let store_idx = self.get_store_index(&keys[0]);
            let store = &self.weights_and_stores[store_idx].store;
            return store
                .has_with_results(keys, results)
                .await
                .err_tip(|| "In ShardStore::has_with_results() for store {store_idx}}");
        }
        type KeyIdxVec = Vec<usize>;
        type KeyVec<'a> = Vec<StoreKey<'a>>;
        let mut keys_for_store: Vec<(KeyIdxVec, KeyVec)> = self
            .weights_and_stores
            .iter()
            .map(|_| (Vec::new(), Vec::new()))
            .collect();
        // Bucket each key into the store that it belongs to.
        keys.iter()
            .enumerate()
            .map(|(key_idx, key)| (key, key_idx, self.get_store_index(key)))
            .for_each(|(key, key_idx, store_idx)| {
                keys_for_store[store_idx].0.push(key_idx);
                keys_for_store[store_idx].1.push(key.borrow());
            });

        // Build all our futures for each store.
        let mut future_stream: FuturesUnordered<_> = keys_for_store
            .into_iter()
            .enumerate()
            .map(|(store_idx, (key_idxs, keys))| async move {
                let store = &self.weights_and_stores[store_idx].store;
                let mut inner_results = vec![None; keys.len()];
                store
                    .has_with_results(&keys, &mut inner_results)
                    .await
                    .err_tip(|| "In ShardStore::has_with_results() for store {store_idx}")?;
                Result::<_, Error>::Ok((key_idxs, inner_results))
            })
            .collect();

        // Wait for all the stores to finish and populate our output results.
        while let Some((key_idxs, inner_results)) = future_stream.try_next().await? {
            for (key_idx, inner_result) in key_idxs.into_iter().zip(inner_results) {
                results[key_idx] = inner_result;
            }
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let store = self.get_store(&key);
        store
            .update(key, reader, size_info)
            .await
            .err_tip(|| "In ShardStore::update()")
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let store = self.get_store(&key);
        store
            .get_part(key, writer, offset, length)
            .await
            .err_tip(|| "In ShardStore::get_part()")
    }

    fn inner_store(&self, key: Option<StoreKey>) -> &'_ dyn StoreDriver {
        let Some(key) = key else {
            return self;
        };
        let index = self.get_store_index(&key);
        self.weights_and_stores[index].store.inner_store(Some(key))
    }

    fn as_any<'a>(&'a self) -> &'a (dyn std::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send + 'static> {
        self
    }
}

default_health_status_indicator!(ShardStore);
