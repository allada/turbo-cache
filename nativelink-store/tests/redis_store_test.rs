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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::panicking;

use bytes::{Bytes, BytesMut};
use fred::bytes_utils::string::Str;
use fred::clients::SubscriberClient;
use fred::error::Error as RedisError;
use fred::mocks::{MockCommand, Mocks};
use fred::prelude::{Builder, Pool as RedisPool};
use fred::types::config::{Config as RedisConfig, PerformanceConfig};
use fred::types::Value as RedisValue;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_metric::{MetricFieldData, MetricKind, MetricsComponent, RootMetricsComponent};
use nativelink_metric_collector::MetricsCollectorLayer;
use nativelink_store::cas_utils::ZERO_BYTE_DIGESTS;
use nativelink_store::redis_store::RedisStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike, UploadSizeInfo};
use parking_lot::RwLock;
use pretty_assertions::assert_eq;
use serde_json::{from_str, to_string, Value};
use tokio::sync::watch;
use tracing_subscriber::layer::SubscriberExt;

const VALID_HASH1: &str = "3031323334353637383961626364656630303030303030303030303030303030";
const TEMP_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

const DEFAULT_READ_CHUNK_SIZE: usize = 1024;
const DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE: usize = 10;

fn mock_uuid_generator() -> String {
    uuid::Uuid::parse_str(TEMP_UUID).unwrap().to_string()
}

fn make_temp_key(final_name: &str) -> String {
    format!("temp-{TEMP_UUID}-{{{final_name}}}")
}

#[derive(Debug)]
struct MockRedisBackend {
    /// Commands we expect to encounter, and results we to return to the client.
    // Commands are pushed from the back and popped from the front.
    expected: Mutex<VecDeque<(MockCommand, Result<RedisValue, RedisError>)>>,

    tx: watch::Sender<MockCommand>,
    rx: watch::Receiver<MockCommand>,

    failing: AtomicBool,
}

impl Default for MockRedisBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MockRedisBackend {
    fn new() -> Self {
        let (tx, rx) = watch::channel(MockCommand {
            cmd: "".into(),
            subcommand: None,
            args: vec![],
        });
        Self {
            expected: Mutex::default(),
            tx,
            rx,
            failing: AtomicBool::new(false),
        }
    }

    fn expect(&self, command: MockCommand, result: Result<RedisValue, RedisError>) -> &Self {
        self.expected.lock().unwrap().push_back((command, result));
        self
    }

    async fn wait_for(&self, command: MockCommand) {
        self.rx
            .clone()
            .wait_for(|cmd| *cmd == command)
            .await
            .expect("the channel isn't closed while the struct exists");
    }
}

impl Mocks for MockRedisBackend {
    fn process_command(&self, actual: MockCommand) -> Result<RedisValue, RedisError> {
        self.tx
            .send(actual.clone())
            .expect("the channel isn't closed while the struct exists");

        let Some((expected, result)) = self.expected.lock().unwrap().pop_front() else {
            // panic here -- this isn't a redis error, it's a test failure
            self.failing.store(true, Ordering::Relaxed);
            panic!("Didn't expect any more commands, but received {actual:?}");
        };

        if actual != expected {
            self.failing.store(true, Ordering::Relaxed);
            assert_eq!(
                actual, expected,
                "mismatched command, received (left) but expected (right)"
            );
        };

        result
    }

    fn process_transaction(&self, commands: Vec<MockCommand>) -> Result<RedisValue, RedisError> {
        static MULTI: MockCommand = MockCommand {
            cmd: Str::from_static("MULTI"),
            subcommand: None,
            args: Vec::new(),
        };
        static EXEC: MockCommand = MockCommand {
            cmd: Str::from_static("EXEC"),
            subcommand: None,
            args: Vec::new(),
        };

        let results = std::iter::once(MULTI.clone())
            .chain(commands)
            .chain([EXEC.clone()])
            .map(|command| self.process_command(command))
            .collect::<Result<Vec<_>, RedisError>>()?;

        Ok(RedisValue::Array(results))
    }
}

impl Drop for MockRedisBackend {
    fn drop(&mut self) {
        if panicking() || self.failing.load(Ordering::Relaxed) {
            // We're already failing, let's make debugging easier and let future devs solve problems one at a time.
            return;
        }

        let expected = self.expected.get_mut().unwrap();

        if expected.is_empty() {
            return;
        }

        assert_eq!(
            *expected,
            VecDeque::new(),
            "Didn't receive all expected commands, expected (left)"
        );

        // Panicking isn't enough inside a tokio task, we need to `exit(1)`
        std::process::exit(1)
    }
}

fn make_clients(mut builder: Builder) -> (RedisPool, SubscriberClient) {
    const CONNECTION_POOL_SIZE: usize = 1;
    let client_pool = builder
        .set_performance_config(PerformanceConfig {
            broadcast_channel_capacity: 4096,
            ..Default::default()
        })
        .build_pool(CONNECTION_POOL_SIZE)
        .unwrap();

    let subscriber_client = builder.build_subscriber_client().unwrap();
    (client_pool, subscriber_client)
}

#[nativelink_test]
async fn upload_and_get_data() -> Result<(), Error> {
    // Construct the data we want to send. Since it's small, we expect it to be sent in a single chunk.
    let data = Bytes::from_static(b"14");
    let chunk_data = RedisValue::Bytes(data.clone());

    // Construct a digest for our data and create a key based on that digest.
    let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
    let packed_hash_hex = format!("{digest}");

    // Construct our Redis store with a mocked out backend.
    let temp_key = RedisValue::Bytes(make_temp_key(&packed_hash_hex).into());
    let real_key = RedisValue::Bytes(packed_hash_hex.into());

    let mocks = Arc::new(MockRedisBackend::new());

    // The first set of commands are for setting the data.
    mocks
        // Append the real value to the temp key.
        .expect(
            MockCommand {
                cmd: Str::from_static("SETRANGE"),
                subcommand: None,
                args: vec![temp_key.clone(), 0.into(), chunk_data],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("STRLEN"),
                subcommand: None,
                args: vec![temp_key.clone()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Integer(
                data.len() as i64
            )])),
        )
        // Move the data from the fake key to the real key.
        .expect(
            MockCommand {
                cmd: Str::from_static("RENAME"),
                subcommand: None,
                args: vec![temp_key, real_key.clone()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        );

    // The second set of commands are for retrieving the data from the key.
    mocks
        // Check that the key exists.
        .expect(
            MockCommand {
                cmd: Str::from_static("STRLEN"),
                subcommand: None,
                args: vec![real_key.clone()],
            },
            Ok(RedisValue::Integer(2)),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("EXISTS"),
                subcommand: None,
                args: vec![real_key.clone()],
            },
            Ok(RedisValue::Integer(1)),
        )
        // Retrieve the data from the real key.
        .expect(
            MockCommand {
                cmd: Str::from_static("GETRANGE"),
                subcommand: None,
                args: vec![real_key, RedisValue::Integer(0), RedisValue::Integer(1)],
            },
            Ok(RedisValue::String(Str::from_static("14"))),
        );

    let store = {
        let mut builder = Builder::default_centralized();
        builder.set_config(RedisConfig {
            mocks: Some(Arc::clone(&mocks) as Arc<dyn Mocks>),
            ..Default::default()
        });
        let (client_pool, subscriber_client) = make_clients(builder);
        RedisStore::new_from_builder_and_parts(
            client_pool,
            subscriber_client,
            None,
            mock_uuid_generator,
            String::new(),
            DEFAULT_READ_CHUNK_SIZE,
            DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
        )
        .unwrap()
    };

    store.update_oneshot(digest, data.clone()).await.unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let result = store
        .get_part_unchunked(digest, 0, Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(result, data, "Expected redis store to have updated value",);

    Ok(())
}

#[nativelink_test]
async fn upload_and_get_data_with_prefix() -> Result<(), Error> {
    let data = Bytes::from_static(b"14");
    let chunk_data = RedisValue::Bytes(data.clone());

    let prefix = "TEST_PREFIX-";

    let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
    let packed_hash_hex = format!("{prefix}{digest}");

    let temp_key = RedisValue::Bytes(make_temp_key(&packed_hash_hex).into());
    let real_key = RedisValue::Bytes(packed_hash_hex.into());

    let mocks = Arc::new(MockRedisBackend::new());
    mocks
        .expect(
            MockCommand {
                cmd: Str::from_static("SETRANGE"),
                subcommand: None,
                args: vec![temp_key.clone(), 0.into(), chunk_data],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("STRLEN"),
                subcommand: None,
                args: vec![temp_key.clone()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Integer(
                data.len() as i64
            )])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("RENAME"),
                subcommand: None,
                args: vec![temp_key, real_key.clone()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("STRLEN"),
                subcommand: None,
                args: vec![real_key.clone()],
            },
            Ok(RedisValue::Integer(2)),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("EXISTS"),
                subcommand: None,
                args: vec![real_key.clone()],
            },
            Ok(RedisValue::Integer(1)),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("GETRANGE"),
                subcommand: None,
                args: vec![real_key, RedisValue::Integer(0), RedisValue::Integer(1)],
            },
            Ok(RedisValue::String(Str::from_static("14"))),
        );

    let store = {
        let mut builder = Builder::default_centralized();
        builder.set_config(RedisConfig {
            mocks: Some(Arc::clone(&mocks) as Arc<dyn Mocks>),
            ..Default::default()
        });

        let (client_pool, subscriber_client) = make_clients(builder);
        RedisStore::new_from_builder_and_parts(
            client_pool,
            subscriber_client,
            None,
            mock_uuid_generator,
            prefix.to_string(),
            DEFAULT_READ_CHUNK_SIZE,
            DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
        )
        .unwrap()
    };

    store.update_oneshot(digest, data.clone()).await.unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let result = store
        .get_part_unchunked(digest, 0, Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(result, data, "Expected redis store to have updated value",);

    Ok(())
}

#[nativelink_test]
async fn upload_empty_data() -> Result<(), Error> {
    let data = Bytes::from_static(b"");
    let digest = ZERO_BYTE_DIGESTS[0];

    let (client_pool, subscriber_client) = make_clients(Builder::default_centralized());
    // We expect to skip both uploading and downloading when the digest is known zero.
    let store = RedisStore::new_from_builder_and_parts(
        client_pool,
        subscriber_client,
        None,
        mock_uuid_generator,
        String::new(),
        DEFAULT_READ_CHUNK_SIZE,
        DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
    )
    .unwrap();

    store.update_oneshot(digest, data).await.unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    Ok(())
}

#[nativelink_test]
async fn upload_empty_data_with_prefix() -> Result<(), Error> {
    let data = Bytes::from_static(b"");
    let digest = ZERO_BYTE_DIGESTS[0];
    let prefix = "TEST_PREFIX-";

    let (client_pool, subscriber_client) = make_clients(Builder::default_centralized());
    let store = RedisStore::new_from_builder_and_parts(
        client_pool,
        subscriber_client,
        None,
        mock_uuid_generator,
        prefix.to_string(),
        DEFAULT_READ_CHUNK_SIZE,
        DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
    )
    .unwrap();

    store.update_oneshot(digest, data).await.unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    Ok(())
}

#[nativelink_test]
async fn test_large_downloads_are_chunked() -> Result<(), Error> {
    const READ_CHUNK_SIZE: usize = 1024;
    let data = Bytes::from(vec![0u8; READ_CHUNK_SIZE + 128]);

    let digest = DigestInfo::try_new(VALID_HASH1, 1)?;
    let packed_hash_hex = format!("{digest}");

    let temp_key = RedisValue::Bytes(make_temp_key(&packed_hash_hex).into());
    let real_key = RedisValue::Bytes(packed_hash_hex.into());

    let mocks = Arc::new(MockRedisBackend::new());

    mocks
        .expect(
            MockCommand {
                cmd: Str::from_static("SETRANGE"),
                subcommand: None,
                args: vec![temp_key.clone(), 0.into(), data.clone().into()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("STRLEN"),
                subcommand: None,
                args: vec![temp_key.clone()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Integer(
                data.len() as i64
            )])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("RENAME"),
                subcommand: None,
                args: vec![temp_key, real_key.clone()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("STRLEN"),
                subcommand: None,
                args: vec![real_key.clone()],
            },
            Ok(RedisValue::Integer(data.len().try_into().unwrap())),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("EXISTS"),
                subcommand: None,
                args: vec![real_key.clone()],
            },
            Ok(RedisValue::Integer(1)),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("GETRANGE"),
                subcommand: None,
                args: vec![
                    real_key.clone(),
                    RedisValue::Integer(0),
                    // We expect to be asked for data from `0..READ_CHUNK_SIZE`, but since GETRANGE is inclusive
                    // the actual call should be from `0..=(READ_CHUNK_SIZE - 1)`.
                    RedisValue::Integer(READ_CHUNK_SIZE as i64 - 1),
                ],
            },
            Ok(RedisValue::Bytes(data.slice(..READ_CHUNK_SIZE))),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("GETRANGE"),
                subcommand: None,
                args: vec![
                    real_key,
                    RedisValue::Integer(READ_CHUNK_SIZE as i64),
                    // Similar GETRANCE index shenanigans here.
                    RedisValue::Integer(data.len() as i64 - 1),
                ],
            },
            Ok(RedisValue::Bytes(data.slice(READ_CHUNK_SIZE..))),
        );

    let store = {
        let mut builder = Builder::default_centralized();
        builder.set_config(RedisConfig {
            mocks: Some(Arc::clone(&mocks) as Arc<dyn Mocks>),
            ..Default::default()
        });

        let (client_pool, subscriber_client) = make_clients(builder);
        RedisStore::new_from_builder_and_parts(
            client_pool,
            subscriber_client,
            None,
            mock_uuid_generator,
            String::new(),
            READ_CHUNK_SIZE,
            DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
        )
        .unwrap()
    };

    store.update_oneshot(digest, data.clone()).await.unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let get_result: Bytes = store
        .get_part_unchunked(digest, 0, Some(data.clone().len() as u64))
        .await
        .unwrap();

    assert_eq!(
        get_result, data,
        "Expected redis store to have updated value",
    );

    Ok(())
}

#[nativelink_test]
async fn yield_between_sending_packets_in_update() -> Result<(), Error> {
    let data_p1 = Bytes::from(vec![b'A'; DEFAULT_READ_CHUNK_SIZE + 512]);
    let data_p2 = Bytes::from(vec![b'B'; DEFAULT_READ_CHUNK_SIZE]);

    let mut data = BytesMut::new();
    data.extend_from_slice(&data_p1);
    data.extend_from_slice(&data_p2);
    let data = data.freeze();

    let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
    let packed_hash_hex = format!("{digest}");

    let temp_key = RedisValue::Bytes(make_temp_key(&packed_hash_hex).into());
    let real_key = RedisValue::Bytes(packed_hash_hex.into());

    let mocks = Arc::new(MockRedisBackend::new());
    let first_append = MockCommand {
        cmd: Str::from_static("SETRANGE"),
        subcommand: None,
        args: vec![temp_key.clone(), 0.into(), data_p1.clone().into()],
    };

    mocks
        // We expect multiple `"SETRANGE"`s as we send data in multiple chunks
        .expect(
            first_append.clone(),
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("SETRANGE"),
                subcommand: None,
                args: vec![
                    temp_key.clone(),
                    data_p1.len().try_into().unwrap(),
                    data_p2.clone().into(),
                ],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("STRLEN"),
                subcommand: None,
                args: vec![temp_key.clone()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Integer(
                data.len() as i64
            )])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("RENAME"),
                subcommand: None,
                args: vec![temp_key, real_key.clone()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("STRLEN"),
                subcommand: None,
                args: vec![real_key.clone()],
            },
            Ok(RedisValue::Integer(2)),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("EXISTS"),
                subcommand: None,
                args: vec![real_key.clone()],
            },
            Ok(RedisValue::Integer(1)),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("GETRANGE"),
                subcommand: None,
                args: vec![
                    real_key.clone(),
                    RedisValue::Integer(0),
                    RedisValue::Integer((DEFAULT_READ_CHUNK_SIZE - 1) as i64),
                ],
            },
            Ok(RedisValue::Bytes(data.clone())),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("GETRANGE"),
                subcommand: None,
                args: vec![
                    real_key.clone(),
                    RedisValue::Integer(DEFAULT_READ_CHUNK_SIZE as i64),
                    RedisValue::Integer((DEFAULT_READ_CHUNK_SIZE * 2 - 1) as i64),
                ],
            },
            Ok(RedisValue::Bytes(data.clone())),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("GETRANGE"),
                subcommand: None,
                args: vec![
                    real_key,
                    RedisValue::Integer((DEFAULT_READ_CHUNK_SIZE * 2) as i64),
                    RedisValue::Integer((data_p1.len() + data_p2.len() - 1) as i64),
                ],
            },
            Ok(RedisValue::Bytes(data.clone())),
        );

    let store = {
        let mut builder = Builder::default_centralized();
        builder.set_config(RedisConfig {
            mocks: Some(Arc::clone(&mocks) as Arc<dyn Mocks>),
            ..Default::default()
        });

        let (client_pool, subscriber_client) = make_clients(builder);
        RedisStore::new_from_builder_and_parts(
            client_pool,
            subscriber_client,
            None,
            mock_uuid_generator,
            String::new(),
            DEFAULT_READ_CHUNK_SIZE,
            DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
        )
        .unwrap()
    };

    let (mut tx, rx) = make_buf_channel_pair();

    tokio::try_join!(
        async {
            store
                .update(digest, rx, UploadSizeInfo::ExactSize(data.len() as u64))
                .await
                .unwrap();

            Ok::<_, Error>(())
        },
        async {
            tx.send(data_p1).await.unwrap();
            mocks.wait_for(first_append).await;
            tx.send(data_p2).await.unwrap();
            tx.send_eof().unwrap();
            Ok::<_, Error>(())
        },
    )
    .unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let result = store
        .get_part_unchunked(digest, 0, Some(data.clone().len() as u64))
        .await
        .unwrap();

    assert_eq!(result, data, "Expected redis store to have updated value",);

    Ok(())
}

// Regression test for: https://github.com/TraceMachina/nativelink/issues/1286
#[nativelink_test]
async fn zero_len_items_exist_check() -> Result<(), Error> {
    let mocks = Arc::new(MockRedisBackend::new());

    let digest = DigestInfo::try_new(VALID_HASH1, 0)?;
    let packed_hash_hex = format!("{digest}");
    let real_key = RedisValue::Bytes(packed_hash_hex.into());

    mocks
        .expect(
            MockCommand {
                cmd: Str::from_static("GETRANGE"),
                subcommand: None,
                args: vec![
                    real_key.clone(),
                    RedisValue::Integer(0),
                    // We expect to be asked for data from `0..READ_CHUNK_SIZE`, but since GETRANGE is inclusive
                    // the actual call should be from `0..=(READ_CHUNK_SIZE - 1)`.
                    RedisValue::Integer(DEFAULT_READ_CHUNK_SIZE as i64 - 1),
                ],
            },
            Ok(RedisValue::String(Str::from_static(""))),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("EXISTS"),
                subcommand: None,
                args: vec![real_key],
            },
            Ok(RedisValue::Integer(0)),
        );

    let store = {
        let mut builder = Builder::default_centralized();
        builder.set_config(RedisConfig {
            mocks: Some(Arc::clone(&mocks) as Arc<dyn Mocks>),
            ..Default::default()
        });

        let (client_pool, subscriber_client) = make_clients(builder);
        RedisStore::new_from_builder_and_parts(
            client_pool,
            subscriber_client,
            None,
            mock_uuid_generator,
            String::new(),
            DEFAULT_READ_CHUNK_SIZE,
            DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
        )
        .unwrap()
    };

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert_eq!(result.unwrap_err().code, Code::NotFound);

    Ok(())
}

// Prevent regressions to https://reviewable.io/reviews/TraceMachina/nativelink/1188#-O2pu9LV5ux4ILuT6MND
#[nativelink_test]
async fn dont_loop_forever_on_empty() -> Result<(), Error> {
    let store = {
        let mut builder = Builder::default_centralized();
        builder.set_config(RedisConfig {
            mocks: Some(Arc::new(MockRedisBackend::new()) as Arc<dyn Mocks>),
            ..Default::default()
        });

        let (client_pool, subscriber_client) = make_clients(builder);
        RedisStore::new_from_builder_and_parts(
            client_pool,
            subscriber_client,
            None,
            mock_uuid_generator,
            String::new(),
            DEFAULT_READ_CHUNK_SIZE,
            DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
        )
        .unwrap()
    };

    let digest = DigestInfo::try_new(VALID_HASH1, 2).unwrap();
    let (tx, rx) = make_buf_channel_pair();

    tokio::join!(
        async {
            store
                .update(digest, rx, UploadSizeInfo::MaxSize(0))
                .await
                .unwrap_err();
        },
        async {
            drop(tx);
        },
    );

    Ok(())
}

#[nativelink_test]
async fn test_redis_fingerprint_metric() -> Result<(), Error> {
    let expected_fingerprint_value: String = String::from("3e762c15");

    let store_manager = Arc::new(StoreManager::new());

    {
        let store = {
            let mut builder = Builder::default_centralized();
            builder.set_config(RedisConfig {
                mocks: Some(Arc::new(MockRedisBackend::new()) as Arc<dyn Mocks>),
                ..Default::default()
            });

            let (client_pool, subscriber_client) = make_clients(builder);
            Store::new(Arc::new(
                RedisStore::new_from_builder_and_parts(
                    client_pool,
                    subscriber_client,
                    None,
                    mock_uuid_generator,
                    String::new(),
                    DEFAULT_READ_CHUNK_SIZE,
                    DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
                )
                .unwrap(),
            ))
        };

        store_manager.add_store("redis_store", store).unwrap();
    };

    let root_metrics = Arc::new(RwLock::new(RootMetricsTest {
        stores: store_manager.clone(),
    }));

    let (layer, output_metrics) = MetricsCollectorLayer::new();

    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
        let metrics_component = root_metrics.read();
        MetricsComponent::publish(
            &*metrics_component,
            MetricKind::Component,
            MetricFieldData::default(),
        )
    })
    .unwrap();

    let output_json_data = to_string(&*output_metrics.lock()).unwrap();

    let parsed_output: Value = from_str(&output_json_data).unwrap();

    let fingerprint_create_index = parsed_output["stores"]["redis_store"]
        ["fingerprint_create_index"]
        .as_str()
        .expect("fingerprint_create_index should be a hex string");

    assert_eq!(fingerprint_create_index, expected_fingerprint_value);

    Ok(())
}

#[derive(MetricsComponent)]
struct RootMetricsTest {
    #[metric(group = "stores")]
    stores: Arc<dyn RootMetricsComponent>,
}

impl RootMetricsComponent for RootMetricsTest {}
