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
use std::sync::{Arc, Mutex};
use std::thread::panicking;

use bytes::Bytes;
use fred::bytes_utils::string::Str;
use fred::error::RedisError;
use fred::mocks::{MockCommand, Mocks};
use fred::prelude::Builder;
use fred::types::{RedisConfig, RedisValue};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::cas_utils::ZERO_BYTE_DIGESTS;
use nativelink_store::redis_store::{RedisStore, READ_CHUNK_SIZE};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{StoreKey, StoreLike, UploadSizeInfo};
use pretty_assertions::assert_eq;

const VALID_HASH1: &str = "3031323334353637383961626364656630303030303030303030303030303030";
const TEMP_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

fn mock_uuid_generator() -> String {
    uuid::Uuid::parse_str(TEMP_UUID).unwrap().to_string()
}

fn make_temp_key(final_name: &str) -> String {
    format!("temp-{TEMP_UUID}-{{{final_name}}}")
}

#[derive(Debug, Default)]
struct MockRedisBackend {
    /// Commands we expect to encounter, and results we to return to the client.
    // Commands are pushed from the back and popped from the front.
    expected: Mutex<VecDeque<(MockCommand, Result<RedisValue, RedisError>)>>,
}

impl MockRedisBackend {
    fn new() -> Self {
        Self::default()
    }

    fn expect(&self, command: MockCommand, result: Result<RedisValue, RedisError>) -> &Self {
        self.expected.lock().unwrap().push_back((command, result));
        self
    }
}

impl Mocks for MockRedisBackend {
    fn process_command(&self, actual: MockCommand) -> Result<RedisValue, RedisError> {
        let Some((expected, result)) = self.expected.lock().unwrap().pop_front() else {
            // panic here -- this isn't a redis error, it's a test failure
            panic!("Didn't expect any more commands, but received {actual:?}");
        };

        assert_eq!(actual, expected);

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
        if panicking() {
            // We're already panicking, let's make debugging easier and let future devs solve problems one at a time.
            return;
        }

        let expected = self.expected.get_mut().unwrap();

        if expected.is_empty() {
            return;
        }

        assert_eq!(
            *expected,
            VecDeque::new(),
            "Didn't receive all expected commands."
        );

        // Panicking isn't enough inside a tokio task, we need to `exit(1)`
        std::process::exit(1)
    }
}

#[nativelink_test]
async fn upload_and_get_data() -> Result<(), Error> {
    // Construct the data we want to send. Since it's small, we expect it to be sent in a single chunk.
    let data = Bytes::from_static(b"14");
    let chunk_data = RedisValue::Bytes(data.clone());

    // Construct a digest for our data and create a key based on that digest.
    let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
    let packed_hash_hex = format!("{}-{}", digest.hash_str(), digest.size_bytes);

    // Construct our Redis store with a mocked out backend.
    let temp_key = RedisValue::Bytes(make_temp_key(&packed_hash_hex).into());
    let real_key = RedisValue::Bytes(packed_hash_hex.into());

    let mocks = Arc::new(MockRedisBackend::new());

    // The first set of commands are for setting the data.
    mocks
        // Append the real value to the temp key.
        .expect(
            MockCommand {
                cmd: Str::from_static("APPEND"),
                subcommand: None,
                args: vec![temp_key.clone(), chunk_data],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
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

        RedisStore::new_from_builder_and_parts(builder, None, mock_uuid_generator, String::new())
            .await?
    };

    store.update_oneshot(digest, data.clone()).await?;

    let result = store.has(digest).await?;
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let result = store
        .get_part_unchunked(digest, 0, Some(data.len()))
        .await?;

    assert_eq!(result, data, "Expected redis store to have updated value",);

    Ok(())
}

#[nativelink_test]
async fn upload_and_get_data_with_prefix() -> Result<(), Error> {
    let data = Bytes::from_static(b"14");
    let chunk_data = RedisValue::Bytes(data.clone());

    let prefix = "TEST_PREFIX-";

    let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
    let packed_hash_hex = format!("{prefix}{}-{}", digest.hash_str(), digest.size_bytes);

    let temp_key = RedisValue::Bytes(make_temp_key(&packed_hash_hex).into());
    let real_key = RedisValue::Bytes(packed_hash_hex.into());

    let mocks = Arc::new(MockRedisBackend::new());
    mocks
        .expect(
            MockCommand {
                cmd: Str::from_static("APPEND"),
                subcommand: None,
                args: vec![temp_key.clone(), chunk_data],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
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

        RedisStore::new_from_builder_and_parts(
            builder,
            None,
            mock_uuid_generator,
            prefix.to_string(),
        )
        .await?
    };

    store.update_oneshot(digest, data.clone()).await?;

    let result = store.has(digest).await?;
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let result = store
        .get_part_unchunked(digest, 0, Some(data.len()))
        .await?;

    assert_eq!(result, data, "Expected redis store to have updated value",);

    Ok(())
}

#[nativelink_test]
async fn upload_empty_data() -> Result<(), Error> {
    let data = Bytes::from_static(b"");
    let digest = ZERO_BYTE_DIGESTS[0];

    // We expect to skip both uploading and downloading when the digest is known zero.
    let store = RedisStore::new_from_builder_and_parts(
        Builder::default_centralized(),
        None,
        mock_uuid_generator,
        String::new(),
    )
    .await?;

    store.update_oneshot(digest, data).await?;

    let result = store.has(digest).await?;
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

    let store = RedisStore::new_from_builder_and_parts(
        Builder::default_centralized(),
        None,
        mock_uuid_generator,
        prefix.to_string(),
    )
    .await?;

    store.update_oneshot(digest, data).await?;

    let result = store.has(digest).await?;
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    Ok(())
}

#[nativelink_test]
async fn unsucessfull_redis_connection() -> Result<(), Error> {
    let store = {
        let builder = Builder::default_centralized();

        RedisStore::new_from_builder_and_parts(builder, None, mock_uuid_generator, String::new())
            .await?
    };

    let keys: Vec<StoreKey> = vec!["abc".into()];
    let mut sizes = vec![];

    let has = store.has_with_results(&keys, &mut sizes).await;

    assert!(has.is_ok());

    Ok(())
}

#[nativelink_test]
async fn test_large_downloads_are_chunked() -> Result<(), Error> {
    // Requires multiple chunks as data is larger than 64K.
    let data = Bytes::from(vec![0u8; READ_CHUNK_SIZE + 128]);

    let digest = DigestInfo::try_new(VALID_HASH1, 1)?;
    let packed_hash_hex = format!("{}-{}", digest.hash_str(), digest.size_bytes);

    let temp_key = RedisValue::Bytes(make_temp_key(&packed_hash_hex).into());
    let real_key = RedisValue::Bytes(packed_hash_hex.into());

    let mocks = Arc::new(MockRedisBackend::new());

    mocks
        .expect(
            MockCommand {
                cmd: Str::from_static("APPEND"),
                subcommand: None,
                args: vec![temp_key.clone(), data.clone().into()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
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

        RedisStore::new_from_builder_and_parts(builder, None, mock_uuid_generator, String::new())
            .await?
    };

    store.update_oneshot(digest, data.clone()).await?;

    let result = store.has(digest).await?;
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let get_result: Bytes = store
        .get_part_unchunked(digest, 0, Some(data.clone().len()))
        .await?;

    assert_eq!(
        get_result,
        data.clone(),
        "Expected redis store to have updated value",
    );

    Ok(())
}

#[nativelink_test]
async fn yield_between_sending_packets_in_update() -> Result<(), Error> {
    let data = Bytes::from(vec![0u8; 10 * 1024]);
    let data_p1 = Bytes::from(vec![0u8; 6 * 1024]);
    let data_p2 = Bytes::from(vec![0u8; 4 * 1024]);

    let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
    let packed_hash_hex = format!("{}-{}", digest.hash_str(), digest.size_bytes);

    let temp_key = RedisValue::Bytes(make_temp_key(&packed_hash_hex).into());
    let real_key = RedisValue::Bytes(packed_hash_hex.into());

    let mocks = Arc::new(MockRedisBackend::new());
    mocks
        // We expect multiple `"APPEND"`s as we send data in multiple chunks
        .expect(
            MockCommand {
                cmd: Str::from_static("APPEND"),
                subcommand: None,
                args: vec![temp_key.clone(), data_p1.clone().into()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        )
        .expect(
            MockCommand {
                cmd: Str::from_static("APPEND"),
                subcommand: None,
                args: vec![temp_key.clone(), data_p2.clone().into()],
            },
            Ok(RedisValue::Array(vec![RedisValue::Null])),
        )
        // The rest of the process looks the same.
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
                cmd: Str::from_static("GETRANGE"),
                subcommand: None,
                args: vec![real_key, RedisValue::Integer(0), RedisValue::Integer(10239)],
            },
            Ok(RedisValue::Bytes(data.clone())),
        );

    let store = {
        let mut builder = Builder::default_centralized();
        builder.set_config(RedisConfig {
            mocks: Some(Arc::clone(&mocks) as Arc<dyn Mocks>),
            ..Default::default()
        });

        RedisStore::new_from_builder_and_parts(builder, None, mock_uuid_generator, String::new())
            .await?
    };

    let (mut tx, rx) = make_buf_channel_pair();
    tx.send(data_p1).await?;
    tokio::task::yield_now().await;
    tx.send(data_p2).await?;
    tx.send_eof()?;
    store
        .update(digest, rx, UploadSizeInfo::ExactSize(data.len()))
        .await?;

    let result = store.has(digest).await?;
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let result = store
        .get_part_unchunked(digest, 0, Some(data.clone().len()))
        .await?;

    assert_eq!(result, data, "Expected redis store to have updated value",);

    Ok(())
}
