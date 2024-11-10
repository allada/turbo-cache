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
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use const_format::formatcp;
use fred::clients::{RedisPool, SubscriberClient};
use fred::interfaces::{ClientLike, KeysInterface, PubsubInterface};
use fred::prelude::{EventInterface, HashesInterface, RediSearchInterface};
use fred::types::{
    Builder, ConnectionConfig, FtCreateOptions, PerformanceConfig, ReconnectPolicy, RedisConfig,
    RedisKey, RedisMap, RedisValue, Script, SearchSchema, SearchSchemaKind, UnresponsiveConfig,
};
use futures::stream::FuturesUnordered;
use futures::{future, FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use nativelink_config::stores::RedisMode;
use nativelink_error::{make_err, make_input_err, Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::health_utils::{HealthRegistryBuilder, HealthStatus, HealthStatusIndicator};
use nativelink_util::spawn;
use nativelink_util::store_trait::{
    BoolValue, SchedulerCurrentVersionProvider, SchedulerIndexProvider, SchedulerStore,
    SchedulerStoreDataProvider, SchedulerStoreDecodeTo, SchedulerStoreKeyProvider,
    SchedulerSubscription, SchedulerSubscriptionManager, StoreDriver, StoreKey, UploadSizeInfo,
};
use nativelink_util::task::JoinHandleDropGuard;
use parking_lot::{Mutex, RwLock};
use patricia_tree::StringPatriciaMap;
use tokio::select;
use tokio::time::sleep;
use tracing::{event, Level};
use uuid::Uuid;

use crate::cas_utils::is_zero_digest;
use crate::redis_utils::ft_aggregate;

/// The default size of the read chunk when reading data from Redis.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_READ_CHUNK_SIZE: usize = 64 * 1024;

/// The default size of the connection pool if not specified.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_CONNECTION_POOL_SIZE: usize = 3;

/// The default delay between retries if not specified.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_RETRY_DELAY: f32 = 0.1;
/// The amount of jitter to add to the retry delay if not specified.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_RETRY_JITTER: f32 = 0.5;

/// The default maximum capacity of the broadcast channel if not specified.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_BROADCAST_CHANNEL_CAPACITY: usize = 4096;

/// The default connection timeout in milliseconds if not specified.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 3000;

/// The default command timeout in milliseconds if not specified.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 10_000;

/// The default maximum number of chunk uploads per update.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE: usize = 10;

#[allow(clippy::trivially_copy_pass_by_ref)]
fn to_hex(value: &u32) -> String {
    format!("{value:08x}")
}

/// A [`StoreDriver`] implementation that uses Redis as a backing store.
#[derive(MetricsComponent)]
pub struct RedisStore {
    /// The client pool connecting to the backing Redis instance(s).
    client_pool: RedisPool,

    /// A channel to publish updates to when a key is added, removed, or modified.
    #[metric(
        help = "The pubsub channel to publish updates to when a key is added, removed, or modified"
    )]
    pub_sub_channel: Option<String>,

    /// A redis client for managing subscriptions.
    /// TODO: This should be moved into the store in followups once a standard use pattern has been determined.
    subscriber_client: SubscriberClient,

    /// For metrics only.
    #[metric(
        help = "A unique identifier for the FT.CREATE command used to create the index template",
        handler = to_hex
    )]
    fingerprint_create_index: u32,

    /// A function used to generate names for temporary keys.
    temp_name_generator_fn: fn() -> String,

    /// A common prefix to append to all keys before they are sent to Redis.
    ///
    /// See [`RedisStore::key_prefix`](`nativelink_config::stores::RedisStore::key_prefix`).
    #[metric(help = "Prefix to append to all keys before sending to Redis")]
    key_prefix: String,

    /// The amount of data to read from Redis at a time.
    #[metric(help = "The amount of data to read from Redis at a time")]
    read_chunk_size: usize,

    /// The maximum number of chunk uploads per update.
    /// This is used to limit the number of chunk uploads per update to prevent
    #[metric(help = "The maximum number of chunk uploads per update")]
    max_chunk_uploads_per_update: usize,

    /// Redis script used to update a value in redis if the version matches.
    /// This is done by incrementing the version number and then setting the new data
    /// only if the version number matches the existing version number.
    update_if_version_matches_script: Script,

    /// A manager for subscriptions to keys in Redis.
    subscription_manager: Mutex<Option<Arc<RedisSubscriptionManager>>>,
}

impl RedisStore {
    /// Create a new `RedisStore` from the given configuration.
    pub fn new(mut config: nativelink_config::stores::RedisSpec) -> Result<Arc<Self>, Error> {
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

        let reconnect_policy = {
            if config.retry.delay == 0.0 {
                config.retry.delay = DEFAULT_RETRY_DELAY;
            }
            if config.retry.jitter == 0.0 {
                config.retry.jitter = DEFAULT_RETRY_JITTER;
            }

            let max_retries = u32::try_from(config.retry.max_retries)
                .err_tip(|| "max_retries could not be converted to u32 in RedisStore::new")?;
            let min_delay_ms = (config.retry.delay * 1000.0) as u32;
            let max_delay_ms = 8000;
            let jitter = (config.retry.jitter * config.retry.delay * 1000.0) as u32;

            let mut reconnect_policy = ReconnectPolicy::new_exponential(
                max_retries,  /* max_retries, 0 is unlimited */
                min_delay_ms, /* min_delay */
                max_delay_ms, /* max_delay */
                2,            /* mult */
            );
            reconnect_policy.set_jitter(jitter);
            reconnect_policy
        };

        {
            if config.broadcast_channel_capacity == 0 {
                config.broadcast_channel_capacity = DEFAULT_BROADCAST_CHANNEL_CAPACITY;
            }
            if config.connection_timeout_ms == 0 {
                config.connection_timeout_ms = DEFAULT_CONNECTION_TIMEOUT_MS;
            }
            if config.command_timeout_ms == 0 {
                config.command_timeout_ms = DEFAULT_COMMAND_TIMEOUT_MS;
            }
            if config.connection_pool_size == 0 {
                config.connection_pool_size = DEFAULT_CONNECTION_POOL_SIZE;
            }
            if config.read_chunk_size == 0 {
                config.read_chunk_size = DEFAULT_READ_CHUNK_SIZE;
            }
            if config.max_chunk_uploads_per_update == 0 {
                config.max_chunk_uploads_per_update = DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE;
            }
        }
        let connection_timeout = Duration::from_millis(config.connection_timeout_ms);
        let command_timeout = Duration::from_millis(config.command_timeout_ms);

        let mut builder = Builder::from_config(redis_config);
        builder
            .set_performance_config(PerformanceConfig {
                default_command_timeout: command_timeout,
                broadcast_channel_capacity: config.broadcast_channel_capacity,
                ..Default::default()
            })
            .set_connection_config(ConnectionConfig {
                connection_timeout,
                internal_command_timeout: command_timeout,
                unresponsive: UnresponsiveConfig {
                    max_timeout: Some(connection_timeout),
                    // This number needs to be less than the connection timeout.
                    // We use 4 as it is a good balance between not spamming the server
                    // and not waiting too long.
                    interval: connection_timeout / 4,
                },
                ..Default::default()
            })
            .set_policy(reconnect_policy);

        let client_pool = builder
            .build_pool(config.connection_pool_size)
            .err_tip(|| "while creating redis connection pool")?;

        let subscriber_client = builder
            .build_subscriber_client()
            .err_tip(|| "while creating redis subscriber client")?;

        Self::new_from_builder_and_parts(
            client_pool,
            subscriber_client,
            config.experimental_pub_sub_channel.clone(),
            || Uuid::new_v4().to_string(),
            config.key_prefix.clone(),
            config.read_chunk_size,
            config.max_chunk_uploads_per_update,
        )
        .map(Arc::new)
    }

    /// Used for testing when determinism is required.
    pub fn new_from_builder_and_parts(
        client_pool: RedisPool,
        subscriber_client: SubscriberClient,
        pub_sub_channel: Option<String>,
        temp_name_generator_fn: fn() -> String,
        key_prefix: String,
        read_chunk_size: usize,
        max_chunk_uploads_per_update: usize,
    ) -> Result<Self, Error> {
        // Start connection pool (this will retry forever by default).
        client_pool.connect();
        subscriber_client.connect();

        Ok(Self {
            client_pool,
            pub_sub_channel,
            subscriber_client,
            fingerprint_create_index: fingerprint_create_index_template(),
            temp_name_generator_fn,
            key_prefix,
            read_chunk_size,
            max_chunk_uploads_per_update,
            update_if_version_matches_script: Script::from_lua(LUA_VERSION_SET_SCRIPT),
            subscription_manager: Mutex::new(None),
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
}

#[async_trait]
impl StoreDriver for RedisStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // TODO(allada) We could use pipeline here, but it makes retry more
        // difficult and it doesn't work very well in cluster mode.
        // If we wanted to optimize this with pipeline be careful to
        // implement retry and to support cluster mode.
        let client = self.client_pool.next();
        keys.iter()
            .zip(results.iter_mut())
            .map(|(key, result)| async move {
                // We need to do a special pass to ensure our zero key exist.
                if is_zero_digest(key.borrow()) {
                    *result = Some(0);
                    return Ok::<_, Error>(());
                }
                let encoded_key = self.encode_key(key);
                let pipeline = client.pipeline();
                pipeline
                    .strlen::<(), _>(encoded_key.as_ref())
                    .await
                    .err_tip(|| {
                        format!("In RedisStore::has_with_results::strlen for {encoded_key}")
                    })?;
                // Redis returns 0 when the key doesn't exist
                // AND when the key exists with value of length 0.
                // Therefore, we need to check both length and existence
                // and do it in a pipeline for efficiency.
                pipeline
                    .exists::<(), _>(encoded_key.as_ref())
                    .await
                    .err_tip(|| {
                        format!("In RedisStore::has_with_results::exists for {encoded_key}")
                    })?;
                let (blob_len, exists) = pipeline
                    .all::<(u64, bool)>()
                    .await
                    .err_tip(|| "In RedisStore::has_with_results::query")?;

                *result = if exists { Some(blob_len) } else { None };

                Ok::<_, Error>(())
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect()
            .await
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

        if is_zero_digest(key.borrow()) {
            let chunk = reader
                .peek()
                .await
                .err_tip(|| "Failed to peek in RedisStore::update")?;
            if chunk.is_empty() {
                reader
                    .drain()
                    .await
                    .err_tip(|| "Failed to drain in RedisStore::update")?;
                // Zero-digest keys are special -- we don't need to do anything with it.
                return Ok(());
            }
        }

        let client = self.client_pool.next();

        let mut read_stream = reader
            .scan(0u32, |bytes_read, chunk_res| {
                future::ready(Some(
                    chunk_res
                        .err_tip(|| "Failed to read chunk in update in redis store")
                        .and_then(|chunk| {
                            let offset = *bytes_read;
                            let chunk_len = u32::try_from(chunk.len()).err_tip(|| {
                                "Could not convert chunk length to u32 in RedisStore::update"
                            })?;
                            let new_bytes_read = bytes_read
                                .checked_add(chunk_len)
                                .err_tip(|| "Overflow protection in RedisStore::update")?;
                            *bytes_read = new_bytes_read;
                            Ok::<_, Error>((offset, *bytes_read, chunk))
                        }),
                ))
            })
            .map(|res| {
                let (offset, end_pos, chunk) = res?;
                let temp_key_ref = &temp_key;
                Ok(async move {
                    client
                        .setrange::<(), _, _>(temp_key_ref, offset, chunk)
                        .await
                        .err_tip(|| {
                            "While appending to append to temp key in RedisStore::update"
                        })?;
                    Ok::<u32, Error>(end_pos)
                })
            })
            .try_buffer_unordered(self.max_chunk_uploads_per_update);

        let mut total_len: u32 = 0;
        while let Some(last_pos) = read_stream.try_next().await? {
            if last_pos > total_len {
                total_len = last_pos;
            }
        }

        let blob_len = client
            .strlen::<u64, _>(&temp_key)
            .await
            .err_tip(|| format!("In RedisStore::update strlen check for {temp_key}"))?;
        // This is a safety check to ensure that in the event some kind of retry was to happen
        // and the data was appended to the key twice, we reject the data.
        if blob_len != u64::from(total_len) {
            return Err(make_input_err!(
                "Data length mismatch in RedisStore::update for {}({}) - expected {} bytes, got {} bytes",
                key.borrow().as_str(),
                temp_key,
                total_len,
                blob_len,
            ));
        }

        // Rename the temp key so that the data appears under the real key. Any data already present in the real key is lost.
        client
            .rename::<(), _, _>(&temp_key, final_key.as_ref())
            .await
            .err_tip(|| "While queueing key rename in RedisStore::update()")?;

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
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let offset = usize::try_from(offset).err_tip(|| "Could not convert offset to usize")?;
        let length = length
            .map(|v| usize::try_from(v).err_tip(|| "Could not convert length to usize"))
            .transpose()?;

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

        // N.B. the `-1`'s you see here are because redis GETRANGE is inclusive at both the start and end, so when we
        // do math with indices we change them to be exclusive at the end.

        // We want to read the data at the key from `offset` to `offset + length`.
        let data_start = offset;
        let data_end = data_start
            .saturating_add(length.unwrap_or(isize::MAX as usize))
            .saturating_sub(1);

        // And we don't ever want to read more than `read_chunk_size` bytes at a time, so we'll need to iterate.
        let mut chunk_start = data_start;
        let mut chunk_end = cmp::min(
            data_start.saturating_add(self.read_chunk_size) - 1,
            data_end,
        );

        loop {
            let chunk: Bytes = client
                .getrange(encoded_key, chunk_start, chunk_end)
                .await
                .err_tip(|| "In RedisStore::get_part::getrange")?;

            let didnt_receive_full_chunk = chunk.len() < self.read_chunk_size;
            let reached_end_of_data = chunk_end == data_end;

            if didnt_receive_full_chunk || reached_end_of_data {
                if !chunk.is_empty() {
                    writer
                        .send(chunk)
                        .await
                        .err_tip(|| "Failed to write data in RedisStore::get_part")?;
                }

                break; // No more data to read.
            }

            // We received a full chunk's worth of data, so write it...
            writer
                .send(chunk)
                .await
                .err_tip(|| "Failed to write data in RedisStore::get_part")?;

            // ...and go grab the next chunk.
            chunk_start = chunk_end + 1;
            chunk_end = cmp::min(
                chunk_start.saturating_add(self.read_chunk_size) - 1,
                data_end,
            );
        }

        // If we didn't write any data, check if the key exists, if not return a NotFound error.
        // This is required by spec.
        if writer.get_bytes_written() == 0 {
            // We're supposed to read 0 bytes, so just check if the key exists.
            let exists = client
                .exists::<bool, _>(encoded_key)
                .await
                .err_tip(|| "In RedisStore::get_part::zero_exists")?;

            if !exists {
                return Err(make_err!(
                    Code::NotFound,
                    "Data not found in Redis store for digest: {key:?}"
                ));
            }
        }

        writer
            .send_eof()
            .err_tip(|| "Failed to write EOF in redis store get_part")
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

/// -------------------------------------------------------------------
/// Below this line are specific to the redis scheduler implementation.
/// -------------------------------------------------------------------

/// The maximum number of results to return per cursor.
const MAX_COUNT_PER_CURSOR: u64 = 256;
/// The time in milliseconds that a redis cursor can be idle before it is closed.
const CURSOR_IDLE_MS: u64 = 2_000;
/// The name of the field in the Redis hash that stores the data.
const DATA_FIELD_NAME: &str = "data";
/// The name of the field in the Redis hash that stores the version.
const VERSION_FIELD_NAME: &str = "version";
/// The time to live of indexes in seconds. After this time redis may delete the index.
const INDEX_TTL_S: u64 = 60 * 60 * 24; // 24 hours.

/// String of the `FT.CREATE` command used to create the index template. It is done this
/// way so we can use it in both const (compile time) functions and runtime functions.
/// This is a macro because we need to use it in multiple places that sometimes require the
/// data as different data types (specifically for rust's format_args! macro).
macro_rules! get_create_index_template {
    () => {
        "FT.CREATE {} ON HASH PREFIX 1 {} NOOFFSETS NOHL NOFIELDS NOFREQS SCHEMA {} TAG CASESENSITIVE SORTABLE"
    }
}

/// Lua script to set a key if the version matches.
/// Args:
///   KEYS[1]: The key where the version is stored.
///   ARGV[1]: The expected version.
///   ARGV[2]: The new data.
///   ARGV[3*]: Key-value pairs of additional data to include.
/// Returns:
///   The new version if the version matches. nil is returned if the
///   value was not set.
const LUA_VERSION_SET_SCRIPT: &str = formatcp!(
    r"
local key = KEYS[1]
local expected_version = tonumber(ARGV[1])
local new_data = ARGV[2]
local new_version = redis.call('HINCRBY', key, '{VERSION_FIELD_NAME}', 1)
local i
local indexes = {{}}

if new_version-1 ~= expected_version then
    redis.call('HINCRBY', key, '{VERSION_FIELD_NAME}', -1)
    return 0
end
-- Skip first 2 argvs, as they are known inputs.
-- Remember: Lua is 1-indexed.
for i=3, #ARGV do
    indexes[i-2] = ARGV[i]
end

-- In testing we witnessed redis sometimes not update our FT indexes
-- resulting in stale data. It appears if we delete our keys then insert
-- them again it works and reduces risk significantly.
redis.call('DEL', key)
redis.call('HSET', key, '{DATA_FIELD_NAME}', new_data, '{VERSION_FIELD_NAME}', new_version, unpack(indexes))

return new_version
"
);

/// Compile-time fingerprint of the `FT.CREATE` command used to create the index template.
/// This is a simple CRC32 checksum of the command string. We don't care about it actually
/// being a valid CRC32 checksum, just that it's a unique identifier with a low chance of
/// collision.
const fn fingerprint_create_index_template() -> u32 {
    const POLY: u32 = 0xEDB8_8320;
    const DATA: &[u8] = get_create_index_template!().as_bytes();
    let mut crc = 0xFFFF_FFFF;
    let mut i = 0;
    while i < DATA.len() {
        let byte = DATA[i];
        crc ^= byte as u32;

        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            j += 1;
        }
        i += 1;
    }
    crc
}

/// Get the name of the index to create for the given field.
/// This will add some prefix data to the name to try and ensure
/// if the index definition changes, the name will get a new name.
macro_rules! get_index_name {
    ($prefix:expr, $field:expr, $maybe_sort:expr) => {
        format_args!(
            "{}_{}_{}_{:08x}",
            $prefix,
            $field,
            $maybe_sort.unwrap_or(""),
            fingerprint_create_index_template(),
        )
    };
}

/// Try to sanitize a string to be used as a Redis key.
/// We don't actually modify the string, just check if it's valid.
const fn try_sanitize(s: &str) -> Option<&str> {
    // Note: We cannot use for loops or iterators here because they are not const.
    // Allowing us to use a const function here gives the compiler the ability to
    // optimize this function away entirely in the case where the input is constant.
    let chars = s.as_bytes();
    let mut i: usize = 0;
    let len = s.len();
    loop {
        if i >= len {
            break;
        }
        let c = chars[i];
        if !c.is_ascii_alphanumeric() && c != b'_' {
            return None;
        }
        i += 1;
    }
    Some(s)
}

/// An individual subscription to a key in Redis.
pub struct RedisSubscription {
    receiver: Option<tokio::sync::watch::Receiver<String>>,
    weak_subscribed_keys: Weak<RwLock<StringPatriciaMap<RedisSubscriptionPublisher>>>,
}

impl SchedulerSubscription for RedisSubscription {
    /// Wait for the subscription key to change.
    async fn changed(&mut self) -> Result<(), Error> {
        let receiver = self
            .receiver
            .as_mut()
            .ok_or_else(|| make_err!(Code::Internal, "In RedisSubscription::changed::as_mut"))?;
        receiver
            .changed()
            .await
            .map_err(|_| make_err!(Code::Internal, "In RedisSubscription::changed::changed"))
    }
}

// If the subscription is dropped, we need to possibly remove the key from the
// subscribed keys map.
impl Drop for RedisSubscription {
    fn drop(&mut self) {
        let Some(receiver) = self.receiver.take() else {
            event!(
                Level::WARN,
                "RedisSubscription has already been dropped, nothing to do."
            );
            return; // Already dropped, nothing to do.
        };
        let key = receiver.borrow().clone();
        // IMPORTANT: This must be dropped before receiver_count() is called.
        drop(receiver);
        let Some(subscribed_keys) = self.weak_subscribed_keys.upgrade() else {
            return; // Already dropped, nothing to do.
        };
        let mut subscribed_keys = subscribed_keys.write();
        let Some(value) = subscribed_keys.get(&key) else {
            event!(
                Level::ERROR,
                "Key {key} was not found in subscribed keys when checking if it should be removed."
            );
            return;
        };
        // If we have no receivers, cleanup the entry from our map.
        if value.receiver_count() == 0 {
            subscribed_keys.remove(key);
        }
    }
}

/// A publisher for a key in Redis.
struct RedisSubscriptionPublisher {
    sender: Mutex<tokio::sync::watch::Sender<String>>,
}

impl RedisSubscriptionPublisher {
    fn new(
        key: String,
        weak_subscribed_keys: Weak<RwLock<StringPatriciaMap<RedisSubscriptionPublisher>>>,
    ) -> (Self, RedisSubscription) {
        let (sender, receiver) = tokio::sync::watch::channel(key);
        let publisher = Self {
            sender: Mutex::new(sender),
        };
        let subscription = RedisSubscription {
            receiver: Some(receiver),
            weak_subscribed_keys,
        };
        (publisher, subscription)
    }

    fn subscribe(
        &self,
        weak_subscribed_keys: Weak<RwLock<StringPatriciaMap<RedisSubscriptionPublisher>>>,
    ) -> RedisSubscription {
        let receiver = self.sender.lock().subscribe();
        RedisSubscription {
            receiver: Some(receiver),
            weak_subscribed_keys,
        }
    }

    fn receiver_count(&self) -> usize {
        self.sender.lock().receiver_count()
    }

    fn notify(&self) {
        // TODO(https://github.com/sile/patricia_tree/issues/40) When this is addressed
        // we can remove the `Mutex` and use the mutable iterator directly.
        self.sender.lock().send_modify(|_| {});
    }
}

pub struct RedisSubscriptionManager {
    subscribed_keys: Arc<RwLock<StringPatriciaMap<RedisSubscriptionPublisher>>>,
    tx_for_test: tokio::sync::mpsc::UnboundedSender<String>,
    _subscription_spawn: JoinHandleDropGuard<()>,
}

impl RedisSubscriptionManager {
    pub fn new(subscribe_client: SubscriberClient, pub_sub_channel: String) -> Self {
        let subscribed_keys = Arc::new(RwLock::new(StringPatriciaMap::new()));
        let subscribed_keys_weak = Arc::downgrade(&subscribed_keys);
        let (tx_for_test, mut rx_for_test) = tokio::sync::mpsc::unbounded_channel();
        Self {
            subscribed_keys,
            tx_for_test,
            _subscription_spawn: spawn!("redis_subscribe_spawn", async move {
                let mut rx = subscribe_client.message_rx();
                loop {
                    if let Err(e) = subscribe_client.subscribe(&pub_sub_channel).await {
                        event!(Level::ERROR, "Error subscribing to pattern - {e}");
                        return;
                    }
                    let mut reconnect_rx = subscribe_client.reconnect_rx();
                    let reconnect_fut = reconnect_rx.recv().fuse();
                    tokio::pin!(reconnect_fut);
                    loop {
                        let key = select! {
                            value = rx_for_test.recv() => {
                                let Some(value) = value else {
                                    unreachable!("Channel should never close");
                                };
                                value.into()
                            },
                            msg = rx.recv() => {
                                match msg {
                                    Ok(msg) => {
                                        if let RedisValue::String(s) = msg.value {
                                            s
                                        } else {
                                            event!(Level::ERROR, "Received non-string message in RedisSubscriptionManager");
                                            continue;
                                        }
                                    },
                                    Err(e) => {
                                        // Check to see if our parent has been dropped and if so kill spawn.
                                        if subscribed_keys_weak.upgrade().is_none() {
                                            event!(Level::WARN, "It appears our parent has been dropped, exiting RedisSubscriptionManager spawn");
                                            return;
                                        };
                                        event!(Level::ERROR, "Error receiving message in RedisSubscriptionManager reconnecting and flagging everything changed - {e}");
                                        break;
                                    }
                                }
                            },
                            _ = &mut reconnect_fut => {
                                event!(Level::WARN, "Redis reconnected flagging all subscriptions as changed and resuming");
                                break;
                            }
                        };
                        let Some(subscribed_keys) = subscribed_keys_weak.upgrade() else {
                            event!(Level::WARN, "It appears our parent has been dropped, exiting RedisSubscriptionManager spawn");
                            return;
                        };
                        let subscribed_keys_mux = subscribed_keys.read();
                        subscribed_keys_mux
                            .common_prefix_values(&*key)
                            .for_each(RedisSubscriptionPublisher::notify);
                    }
                    // Sleep for a small amount of time to ensure we don't reconnect too quickly.
                    sleep(Duration::from_secs(1)).await;
                    // If we reconnect or lag behind we might have had dirty keys, so we need to
                    // flag all of them as changed.
                    let Some(subscribed_keys) = subscribed_keys_weak.upgrade() else {
                        event!(Level::WARN, "It appears our parent has been dropped, exiting RedisSubscriptionManager spawn");
                        return;
                    };
                    let subscribed_keys_mux = subscribed_keys.read();
                    // Just in case also get a new receiver.
                    rx = subscribe_client.message_rx();
                    // Drop all buffered messages, then flag everything as changed.
                    rx.resubscribe();
                    for publisher in subscribed_keys_mux.values() {
                        publisher.notify();
                    }
                }
            }),
        }
    }
}

impl SchedulerSubscriptionManager for RedisSubscriptionManager {
    type Subscription = RedisSubscription;

    fn notify_for_test(&self, value: String) {
        self.tx_for_test.send(value).unwrap();
    }

    fn subscribe<K>(&self, key: K) -> Result<Self::Subscription, Error>
    where
        K: SchedulerStoreKeyProvider,
    {
        let weak_subscribed_keys = Arc::downgrade(&self.subscribed_keys);
        let mut subscribed_keys = self.subscribed_keys.write();
        let key = key.get_key();
        let key_str = key.as_str();
        let mut subscription = if let Some(publisher) = subscribed_keys.get(&key_str) {
            publisher.subscribe(weak_subscribed_keys)
        } else {
            let (publisher, subscription) =
                RedisSubscriptionPublisher::new(key_str.to_string(), weak_subscribed_keys);
            subscribed_keys.insert(key_str, publisher);
            subscription
        };
        subscription
            .receiver
            .as_mut()
            .ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "Receiver should be set in RedisSubscriptionManager::subscribe"
                )
            })?
            .mark_changed();

        Ok(subscription)
    }
}

impl SchedulerStore for RedisStore {
    type SubscriptionManager = RedisSubscriptionManager;

    fn subscription_manager(&self) -> Result<Arc<RedisSubscriptionManager>, Error> {
        let mut subscription_manager = self.subscription_manager.lock();
        match &*subscription_manager {
            Some(subscription_manager) => Ok(subscription_manager.clone()),
            None => {
                let Some(pub_sub_channel) = &self.pub_sub_channel else {
                    return Err(make_input_err!("RedisStore must have a pubsub channel for a Redis Scheduler if using subscriptions"));
                };
                let sub = Arc::new(RedisSubscriptionManager::new(
                    self.subscriber_client.clone(),
                    pub_sub_channel.clone(),
                ));
                *subscription_manager = Some(sub.clone());
                Ok(sub)
            }
        }
    }

    async fn update_data<T>(&self, data: T) -> Result<Option<u64>, Error>
    where
        T: SchedulerStoreDataProvider
            + SchedulerStoreKeyProvider
            + SchedulerCurrentVersionProvider
            + Send,
    {
        let key = data.get_key();
        let key = self.encode_key(&key);
        let client = self.client_pool.next();
        let maybe_index = data.get_indexes().err_tip(|| {
            format!("Err getting index in RedisStore::update_data::versioned for {key:?}")
        })?;
        if <T as SchedulerStoreKeyProvider>::Versioned::VALUE {
            let current_version = data.current_version();
            let data = data.try_into_bytes().err_tip(|| {
                format!("Could not convert value to bytes in RedisStore::update_data::versioned for {key:?}")
            })?;
            let mut argv = Vec::with_capacity(3 + maybe_index.len() * 2);
            argv.push(Bytes::from(format!("{current_version}")));
            argv.push(data);
            for (name, value) in maybe_index {
                argv.push(Bytes::from_static(name.as_bytes()));
                argv.push(value);
            }
            let new_version = self
                .update_if_version_matches_script
                .evalsha_with_reload::<u64, _, Vec<Bytes>>(client, vec![key.as_ref()], argv)
                .await
                .err_tip(|| format!("In RedisStore::update_data::versioned for {key:?}"))?;
            if new_version == 0 {
                return Ok(None);
            }
            // If we have a publish channel configured, send a notice that the key has been set.
            if let Some(pub_sub_channel) = &self.pub_sub_channel {
                return Ok(client.publish(pub_sub_channel, key.as_ref()).await?);
            };
            Ok(Some(new_version))
        } else {
            let data = data.try_into_bytes().err_tip(|| {
                format!("Could not convert value to bytes in RedisStore::update_data::noversion for {key:?}")
            })?;
            let mut fields = RedisMap::new();
            fields.reserve(1 + maybe_index.len());
            fields.insert(DATA_FIELD_NAME.into(), data.into());
            for (name, value) in maybe_index {
                fields.insert(name.into(), value.into());
            }
            client
                .hset::<(), _, _>(key.as_ref(), fields)
                .await
                .err_tip(|| format!("In RedisStore::update_data::noversion for {key:?}"))?;
            // If we have a publish channel configured, send a notice that the key has been set.
            if let Some(pub_sub_channel) = &self.pub_sub_channel {
                return Ok(client.publish(pub_sub_channel, key.as_ref()).await?);
            };
            Ok(Some(0)) // Always use "0" version since this is not a versioned request.
        }
    }

    async fn search_by_index_prefix<K>(
        &self,
        index: K,
    ) -> Result<
        impl Stream<Item = Result<<K as SchedulerStoreDecodeTo>::DecodeOutput, Error>> + Send,
        Error,
    >
    where
        K: SchedulerIndexProvider + SchedulerStoreDecodeTo + Send,
    {
        let index_value = index.index_value();
        let run_ft_aggregate = || {
            let client = self.client_pool.next().clone();
            let sanitized_field = try_sanitize(index_value.as_ref()).err_tip(|| {
                format!("In RedisStore::search_by_index_prefix::try_sanitize - {index_value:?}")
            })?;
            Ok::<_, Error>(async move {
                ft_aggregate(
                    client,
                    format!(
                        "{}",
                        get_index_name!(K::KEY_PREFIX, K::INDEX_NAME, K::MAYBE_SORT_KEY)
                    ),
                    format!("@{}:{{ {} }}", K::INDEX_NAME, sanitized_field),
                    fred::types::FtAggregateOptions {
                        load: Some(fred::types::Load::Some(vec![
                            fred::types::SearchField {
                                identifier: DATA_FIELD_NAME.into(),
                                property: None,
                            },
                            fred::types::SearchField {
                                identifier: VERSION_FIELD_NAME.into(),
                                property: None,
                            },
                        ])),
                        cursor: Some(fred::types::WithCursor {
                            count: Some(MAX_COUNT_PER_CURSOR),
                            max_idle: Some(CURSOR_IDLE_MS),
                        }),
                        pipeline: vec![fred::types::AggregateOperation::SortBy {
                            properties: K::MAYBE_SORT_KEY.map_or_else(Vec::new, |v| {
                                vec![(format!("@{v}").into(), fred::types::SortOrder::Asc)]
                            }),
                            max: None,
                        }],
                        ..Default::default()
                    },
                )
                .await
            })
        };
        let stream = run_ft_aggregate()?
            .or_else(|_| async move {
                let mut schema = vec![SearchSchema {
                    field_name: K::INDEX_NAME.into(),
                    alias: None,
                    kind: SearchSchemaKind::Tag {
                        sortable: false,
                        unf: false,
                        separator: None,
                        casesensitive: false,
                        withsuffixtrie: false,
                        noindex: false,
                    },
                }];
                if let Some(sort_key) = K::MAYBE_SORT_KEY {
                    schema.push(SearchSchema {
                        field_name: sort_key.into(),
                        alias: None,
                        kind: SearchSchemaKind::Tag {
                            sortable: true,
                            unf: false,
                            separator: None,
                            casesensitive: false,
                            withsuffixtrie: false,
                            noindex: false,
                        },
                    });
                }
                let create_result = self
                    .client_pool
                    .next()
                    .ft_create::<(), _>(
                        format!(
                            "{}",
                            get_index_name!(K::KEY_PREFIX, K::INDEX_NAME, K::MAYBE_SORT_KEY)
                        ),
                        FtCreateOptions {
                            on: Some(fred::types::IndexKind::Hash),
                            prefixes: vec![K::KEY_PREFIX.into()],
                            nohl: true,
                            nofields: true,
                            nofreqs: true,
                            nooffsets: true,
                            temporary: Some(INDEX_TTL_S),
                            ..Default::default()
                        },
                        schema,
                    )
                    .await
                    .err_tip(|| {
                        format!(
                            "Error with ft_create in RedisStore::search_by_index_prefix({})",
                            get_index_name!(K::KEY_PREFIX, K::INDEX_NAME, K::MAYBE_SORT_KEY),
                        )
                    });
                let run_result = run_ft_aggregate()?.await.err_tip(|| {
                    format!(
                        "Error with second ft_aggregate in RedisStore::search_by_index_prefix({})",
                        get_index_name!(K::KEY_PREFIX, K::INDEX_NAME, K::MAYBE_SORT_KEY),
                    )
                });
                // Creating the index will race which is ok. If it fails to create, we only
                // error if the second ft_aggregate call fails and fails to create.
                run_result.or_else(move |e| create_result.merge(Err(e)))
            })
            .await?;
        Ok(stream.map(|result| {
            let mut redis_map =
                result.err_tip(|| "Error in stream of in RedisStore::search_by_index_prefix")?;
            let bytes_data = redis_map
                .remove(&RedisKey::from_static_str(DATA_FIELD_NAME))
                .err_tip(|| "Missing data field in RedisStore::search_by_index_prefix")?
                .into_bytes()
                .err_tip(|| {
                    formatcp!("'{DATA_FIELD_NAME}' is not Bytes in RedisStore::search_by_index_prefix::into_bytes")
                })?;
            let version = if <K as SchedulerIndexProvider>::Versioned::VALUE {
                redis_map
                    .remove(&RedisKey::from_static_str(VERSION_FIELD_NAME))
                    .err_tip(|| "Missing version field in RedisStore::search_by_index_prefix")?
                    .as_u64()
                    .err_tip(|| {
                        formatcp!("'{VERSION_FIELD_NAME}' is not u64 in RedisStore::search_by_index_prefix::as_u64")
                    })?
            } else {
                0
            };
            K::decode(version, bytes_data)
                .err_tip(|| "In RedisStore::search_by_index_prefix::decode")
        }))
    }

    async fn get_and_decode<K>(
        &self,
        key: K,
    ) -> Result<Option<<K as SchedulerStoreDecodeTo>::DecodeOutput>, Error>
    where
        K: SchedulerStoreKeyProvider + SchedulerStoreDecodeTo + Send,
    {
        let key = key.get_key();
        let key = self.encode_key(&key);
        let client = self.client_pool.next();
        let (maybe_version, maybe_data) = client
            .hmget::<(Option<u64>, Option<Bytes>), _, _>(
                key.as_ref(),
                vec![
                    RedisKey::from(VERSION_FIELD_NAME),
                    RedisKey::from(DATA_FIELD_NAME),
                ],
            )
            .await
            .err_tip(|| format!("In RedisStore::get_without_version::notversioned {key}"))?;
        let Some(data) = maybe_data else {
            return Ok(None);
        };
        Ok(Some(K::decode(maybe_version.unwrap_or(0), data).err_tip(
            || format!("In RedisStore::get_with_version::notversioned::decode {key}"),
        )?))
    }
}
