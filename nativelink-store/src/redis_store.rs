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
use std::cmp;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use fred::clients::{RedisClient, RedisPool, SubscriberClient};
use fred::interfaces::{ClientLike, KeysInterface, PubsubInterface};
use fred::types::{Builder, ConnectionConfig, PerformanceConfig, ReconnectPolicy, RedisConfig};
use nativelink_config::stores::RedisMode;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::health_utils::{HealthRegistryBuilder, HealthStatus, HealthStatusIndicator};
use nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use uuid::Uuid;

use crate::cas_utils::is_zero_digest;

// TODO(caass): These (and other settings) should be made configurable via nativelink-config.
pub const READ_CHUNK_SIZE: usize = 64 * 1024;
const CONNECTION_POOL_SIZE: usize = 3;

/// A [`StoreDriver`] implementation that uses Redis as a backing store.
#[derive(MetricsComponent)]
pub struct RedisStore {
    /// The client pool connecting to the backing Redis instance(s).
    client_pool: RedisPool,

    /// A channel to publish updates to when a key is added, removed, or modified.
    pub_sub_channel: Option<String>,

    /// A redis client for managing subscriptions.
    /// TODO: This should be moved into the store in followups once a standard use pattern has been determined.
    subscriber_client: SubscriberClient,

    /// A function used to generate names for temporary keys.
    temp_name_generator_fn: fn() -> String,

    /// A common prefix to append to all keys before they are sent to Redis.
    ///
    /// See [`RedisStore::key_prefix`](`nativelink_config::stores::RedisStore::key_prefix`).
    #[metric(help = "Prefix to append to all keys before sending to Redis")]
    key_prefix: String,
}

impl RedisStore {
    /// Create a new `RedisStore` from the given configuration.
    pub async fn new(config: &nativelink_config::stores::RedisStore) -> Result<Arc<Self>, Error> {
        if config.addresses.is_empty() {
            return Err(make_err!(
                Code::InvalidArgument,
                "No addresses were specified in redis store configuration."
            ));
        };
        let [addr] = config.addresses.as_slice() else {
            return Err(make_err!(Code::Unimplemented, "Connecting directly to multiple redis nodes in a cluster is currently unsupported. Please specify a single URL to a single node, and nativelink will use cluster discover to find the other nodes."));
        };
        let redis_config = match config.mode {
            RedisMode::Cluster => RedisConfig::from_url_clustered(addr),
            RedisMode::Sentinel => RedisConfig::from_url_sentinel(addr),
            RedisMode::Standard => RedisConfig::from_url_centralized(addr),
        }
        .err_tip_with_code(|e| {
            (
                Code::InvalidArgument,
                format!("while parsing redis node address: {e}"),
            )
        })?;

        let mut builder = Builder::from_config(redis_config);
        builder
            .set_performance_config(PerformanceConfig {
                default_command_timeout: Duration::from_secs(config.response_timeout_s),
                ..Default::default()
            })
            .set_connection_config(ConnectionConfig {
                connection_timeout: Duration::from_secs(config.connection_timeout_s),
                internal_command_timeout: Duration::from_secs(config.response_timeout_s),
                ..Default::default()
            })
            // TODO(caass): Make this configurable.
            .set_policy(ReconnectPolicy::new_constant(1, 0));

        Self::new_from_builder_and_parts(
            builder,
            config.experimental_pub_sub_channel.clone(),
            || Uuid::new_v4().to_string(),
            config.key_prefix.clone(),
        )
        .await
        .map(Arc::new)
    }

    /// Used for testing when determinism is required.
    pub async fn new_from_builder_and_parts(
        builder: Builder,
        pub_sub_channel: Option<String>,
        temp_name_generator_fn: fn() -> String,
        key_prefix: String,
    ) -> Result<Self, Error> {
        let client_pool = builder
            .build_pool(CONNECTION_POOL_SIZE)
            .err_tip(|| "while creating redis connection pool")?;

        let subscriber_client = builder
            .build_subscriber_client()
            .err_tip(|| "while creating redis subscriber client")?;
        // Fires off a background task using `tokio::spawn`.
        client_pool.init().await?;
        subscriber_client.connect();

        Ok(Self {
            client_pool,
            pub_sub_channel,
            subscriber_client,
            temp_name_generator_fn,
            key_prefix,
        })
    }

    /// Encode a [`StoreKey`] so it can be sent to Redis.
    fn encode_key<'a>(&self, key: &'a StoreKey<'a>) -> Cow<'a, str> {
        let key_body = key.as_str();
        if self.key_prefix.is_empty() {
            key_body
        } else {
            // This is in the hot path for all redis operations, so we try to reuse the allocation
            // from `key.as_str()` if possible.
            match key_body {
                Cow::Owned(mut encoded_key) => {
                    encoded_key.insert_str(0, &self.key_prefix);
                    Cow::Owned(encoded_key)
                }
                Cow::Borrowed(body) => {
                    let mut encoded_key = String::with_capacity(self.key_prefix.len() + body.len());
                    encoded_key.push_str(&self.key_prefix);
                    encoded_key.push_str(body);
                    Cow::Owned(encoded_key)
                }
            }
        }
    }

    // TODO: These helpers eventually should not be necessary, as they are only used for functionality
    // that could hypothetically be moved behind this API with some non-trivial logic adjustments
    // and the addition of one or two new endpoints.
    pub fn get_subscriber_client(&self) -> SubscriberClient {
        self.subscriber_client.clone()
    }

    pub fn get_client(&self) -> RedisClient {
        self.client_pool.next().clone()
    }
}

#[async_trait]
impl StoreDriver for RedisStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<usize>],
    ) -> Result<(), Error> {
        // TODO(caass): Optimize for the case where `keys.len() == 1`
        let pipeline = self.client_pool.next().pipeline();

        results.iter_mut().for_each(|result| *result = None);

        for (idx, key) in keys.iter().enumerate() {
            // Don't bother with zero-length digests.
            if is_zero_digest(key.borrow()) {
                results[idx] = Some(0);
                continue;
            }

            let encoded_key = self.encode_key(key);

            // This command is queued in memory, but not yet sent down the pipeline; the `await` returns instantly.
            pipeline
                .strlen::<(), _>(encoded_key.as_ref())
                .await
                .err_tip(|| "In RedisStore::has_with_results")?;
        }

        // Send the queued commands.
        let mut responses = pipeline.all::<Vec<_>>().await?.into_iter();
        let mut remaining_results = results.iter_mut().filter(|option| {
            // Anything that's `Some` was already set from `is_zero_digest`.
            option.is_none()
        });

        // Similar to `Iterator::zip`, but with some verification at the end that the lengths were equal.
        while let (Some(response), Some(result_slot)) = (responses.next(), remaining_results.next())
        {
            if response == 0 {
                // Redis returns 0 when the key doesn't exist AND when the key exists with value of length 0.
                // Since we already checked zero-lengths with `is_zero_digest`, this means the value doesn't exist.
                continue;
            }

            *result_slot = Some(response);
        }

        if responses.next().is_some() {
            Err(make_err!(
                Code::Internal,
                "Received more responses than expected in RedisStore::has_with_results"
            ))
        } else if remaining_results.next().is_some() {
            Err(make_err!(
                Code::Internal,
                "Received fewer responses than expected in RedisStore::has_with_results"
            ))
        } else {
            Ok(())
        }
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let final_key = self.encode_key(&key);

        // While the name generation function can be supplied by the user, we need to have the curly
        // braces in place in order to manage redis' hashing behavior and make sure that the temporary
        // key name and the final key name are directed to the same cluster node. See
        // https://redis.io/blog/redis-clustering-best-practices-with-keys/
        //
        // The TL;DR is that if we're in cluster mode and the names hash differently, we can't use request
        // pipelining. By using these braces, we tell redis to only hash the part of the temporary key that's
        // identical to the final key -- so they will always hash to the same node.
        let temp_key = format!(
            "temp-{}-{{{}}}",
            (self.temp_name_generator_fn)(),
            &final_key
        );

        let client = self.client_pool.next();

        // This loop is a little confusing at first glance, but essentially the process is:
        // - Get as much data from the reader as possible
        // - When the reader is empty, but the writer isn't done sending data, write that data to redis
        // - When the writer is done sending data, write the data and break from the loop
        //
        // At one extreme, we could append data in redis every time we read some bytes -- that is, make one TCP request
        // per channel read. This is wasteful since we anticipate reading many small chunks of bytes from the reader.
        //
        // At the other extreme, we could make a single TCP request to write all of the data all at once.
        // This could also be an issue if we read loads of data, since we'd send one massive TCP request
        // rather than a few moderately-sized requests.
        //
        // To compromise, we buffer opportunistically -- when the reader doesn't have any data ready to read, but it's
        // not done getting data, we flush the data we _have_ read to redis before waiting for the reader to get more.
        //
        // As a result of this, there will be a span of time where a key in Redis has only partial data. We want other
        // observers to notice atomic updates to keys, rather than partial updates, so we first write to a temporary key
        // and then rename that key once we're done appending data.
        //
        // TODO(caass): Remove potential for infinite loop (https://reviewable.io/reviews/TraceMachina/nativelink/1188#-O2pu9LV5ux4ILuT6MND)
        'outer: loop {
            let mut expecting_first_chunk = true;
            let pipe = client.pipeline();

            while expecting_first_chunk || !reader.is_empty() {
                let chunk = reader
                    .recv()
                    .await
                    .err_tip(|| "Failed to reach chunk in update in redis store")?;

                if chunk.is_empty() {
                    if is_zero_digest(key.borrow()) {
                        return Ok(());
                    }

                    // Reader sent empty chunk, we're done here.
                    break 'outer;
                }

                // Queue the append, but don't execute until we've received all the chunks.
                pipe.append::<(), _, _>(&temp_key, chunk)
                    .await
                    .err_tip(|| "Failed to append to temp key in RedisStore::update")?;
                expecting_first_chunk = false;

                // Give other tasks a chance to run to populate the reader's
                // buffer if possible.
                tokio::task::yield_now().await;
            }

            // Here the reader is empty but more data is expected.
            // Executing the queued commands appends the data we just received to the temp key.
            pipe.all::<()>()
                .await
                .err_tip(|| "Failed to append to temporary key in RedisStore::update")?;
        }

        // Rename the temp key so that the data appears under the real key. Any data already present in the real key is lost.
        client
            .rename::<(), _, _>(&temp_key, final_key.as_ref())
            .await
            .err_tip(|| "While renaming key in RedisStore::update()")?;

        // If we have a publish channel configured, send a notice that the key has been set.
        if let Some(pub_sub_channel) = &self.pub_sub_channel {
            return Ok(client.publish(pub_sub_channel, final_key.as_ref()).await?);
        };

        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        // To follow RBE spec we need to consider any digest's with
        // zero size to be existing.
        if is_zero_digest(key.borrow()) {
            return writer
                .send_eof()
                .err_tip(|| "Failed to send zero EOF in redis store get_part");
        }

        let client = self.client_pool.next();
        let encoded_key = self.encode_key(&key);
        let encoded_key = encoded_key.as_ref();

        if length == Some(0) {
            // We're supposed to read 0 bytes, so just check if the key exists.
            let exists = client
                .exists(encoded_key)
                .await
                .err_tip(|| "In RedisStore::get_part::zero_exists")?;

            return match exists {
                0u8 => Err(make_err!(
                    Code::NotFound,
                    "Data not found in Redis store for digest: {key:?}"
                )),
                1 => writer
                    .send_eof()
                    .err_tip(|| "Failed to write EOF in redis store get_part"),
                _ => unreachable!("only checked for existence of a single key"),
            };
        }

        // N.B. the `-1`'s you see here are because redis GETRANGE is inclusive at both the start and end, so when we
        // do math with indices we change them to be exclusive at the end.

        // We want to read the data at the key from `offset` to `offset + length`.
        let data_start = offset;
        let data_end = data_start.saturating_add(length.unwrap_or(isize::MAX as usize)) - 1;

        // And we don't ever want to read more than `READ_CHUNK_SIZE` bytes at a time, so we'll need to iterate.
        let mut chunk_start = data_start;
        let mut chunk_end = cmp::min(data_start.saturating_add(READ_CHUNK_SIZE) - 1, data_end);

        loop {
            let chunk: Bytes = client
                .getrange(encoded_key, chunk_start, chunk_end)
                .await
                .err_tip(|| "In RedisStore::get_part::getrange")?;

            let didnt_receive_full_chunk = chunk.len() < READ_CHUNK_SIZE;
            let reached_end_of_data = chunk_end == data_end;

            if didnt_receive_full_chunk || reached_end_of_data {
                if !chunk.is_empty() {
                    writer
                        .send(chunk)
                        .await
                        .err_tip(|| "Failed to write data in RedisStore::get_part")?;
                }

                return writer
                    .send_eof()
                    .err_tip(|| "Failed to write EOF in redis store get_part");
            }

            // We received a full chunk's worth of data, so write it...
            writer
                .send(chunk)
                .await
                .err_tip(|| "Failed to write data in RedisStore::get_part")?;

            // ...and go grab the next chunk.
            chunk_start = chunk_end + 1;
            chunk_end = cmp::min(chunk_start.saturating_add(READ_CHUNK_SIZE) - 1, data_end);
        }
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn std::any::Any + Sync + Send) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send> {
        self
    }

    fn register_health(self: Arc<Self>, registry: &mut HealthRegistryBuilder) {
        registry.register_indicator(self);
    }
}

#[async_trait]
impl HealthStatusIndicator for RedisStore {
    fn get_name(&self) -> &'static str {
        "RedisStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}
