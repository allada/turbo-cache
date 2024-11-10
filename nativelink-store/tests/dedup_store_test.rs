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

use nativelink_config::stores::{MemorySpec, StoreRef};
use nativelink_error::{Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::cas_utils::ZERO_BYTE_DIGESTS;
use nativelink_store::dedup_store::DedupStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

fn make_default_config() -> nativelink_config::stores::DedupSpec {
    nativelink_config::stores::DedupSpec {
        index_store: StoreRef::new("index", MemorySpec::default()),
        content_store: StoreRef::new("content", MemorySpec::default()),
        min_size: 8 * 1024,
        normal_size: 32 * 1024,
        max_size: 128 * 1024,
        max_concurrent_fetch_per_get: 10,
    }
}

fn make_random_data(sz: usize) -> Vec<u8> {
    let mut value = vec![0u8; sz];
    let mut rng = SmallRng::seed_from_u64(1);
    rng.fill(&mut value[..]);
    value
}

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const MEGABYTE_SZ: usize = 1024 * 1024;

#[nativelink_test]
async fn simple_round_trip_test() -> Result<(), Error> {
    let store = DedupStore::new(
        &make_default_config(),
        Store::new(MemoryStore::new(
            &nativelink_config::stores::MemorySpec::default(),
        )), // Index store.
        Store::new(MemoryStore::new(
            &nativelink_config::stores::MemorySpec::default(),
        )), // Content store.
    )?;

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH1, MEGABYTE_SZ).unwrap();

    store
        .update_oneshot(digest, original_data.clone().into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    let rt_data = store
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "Failed to get_part from dedup store")?;

    assert_eq!(rt_data, original_data, "Expected round trip data to match");
    Ok(())
}

#[nativelink_test]
async fn check_missing_last_chunk_test() -> Result<(), Error> {
    // This is the hash & size of the last chunk item in the content_store.
    const LAST_CHUNK_HASH: &str =
        "7c8608f5b079bef66c45bd67f7d8ede15d2e1830ea38fd8ad4c6de08b6f21a0c";
    const LAST_CHUNK_SIZE: usize = 25779;

    let content_store = MemoryStore::new(&nativelink_config::stores::MemorySpec::default());
    let store = DedupStore::new(
        &make_default_config(),
        Store::new(MemoryStore::new(
            &nativelink_config::stores::MemorySpec::default(),
        )), // Index store.
        Store::new(content_store.clone()),
    )?;

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH1, MEGABYTE_SZ).unwrap();

    store
        .update_oneshot(digest, original_data.into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    let did_delete = content_store
        .remove_entry(
            DigestInfo::try_new(LAST_CHUNK_HASH, LAST_CHUNK_SIZE)
                .unwrap()
                .into(),
        )
        .await;

    assert_eq!(did_delete, true, "Expected item to exist in store");

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err(), "Expected result to be an error");
    assert_eq!(
        result.unwrap_err().code,
        Code::NotFound,
        "Expected result to not be found"
    );
    Ok(())
}

/// Test to ensure if we upload a bit of data then request just a slice of it, we get the
/// proper data out. Internal to DedupStore we only download the slices that contain the
/// requested data; this test covers that use case.
#[nativelink_test]
async fn fetch_part_test() -> Result<(), Error> {
    const DATA_SIZE: usize = MEGABYTE_SZ / 4;
    const ONE_THIRD_SZ: usize = DATA_SIZE / 3;

    let store = DedupStore::new(
        &make_default_config(),
        Store::new(MemoryStore::new(
            &nativelink_config::stores::MemorySpec::default(),
        )), // Index store.
        Store::new(MemoryStore::new(
            &nativelink_config::stores::MemorySpec::default(),
        )), // Content store.
    )?;

    let original_data = make_random_data(DATA_SIZE);
    let digest = DigestInfo::try_new(VALID_HASH1, DATA_SIZE).unwrap();

    store
        .update_oneshot(digest, original_data.clone().into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    let rt_data = store
        .get_part_unchunked(digest, ONE_THIRD_SZ as u64, Some(ONE_THIRD_SZ as u64))
        .await
        .err_tip(|| "Failed to get_part from dedup store")?;

    assert_eq!(
        rt_data.len(),
        ONE_THIRD_SZ,
        "Expected round trip sizes to match"
    );
    assert_eq!(
        rt_data,
        original_data[ONE_THIRD_SZ..(ONE_THIRD_SZ * 2)],
        "Expected round trip data to match"
    );
    Ok(())
}

#[nativelink_test]
async fn check_length_not_set_with_chunk_read_beyond_first_chunk_regression_test(
) -> Result<(), Error> {
    const DATA_SIZE: usize = 30;
    const START_READ_BYTE: usize = 7;

    let store = DedupStore::new(
        &nativelink_config::stores::DedupSpec {
            index_store: StoreRef::new("index", MemorySpec::default()),
            content_store: StoreRef::new("index", MemorySpec::default()),
            min_size: 5,
            normal_size: 6,
            max_size: 7,
            max_concurrent_fetch_per_get: 10,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())), // Index store.
        Store::new(MemoryStore::new(&MemorySpec::default())), // Content store.
    )?;

    let original_data = make_random_data(DATA_SIZE);
    let digest = DigestInfo::try_new(VALID_HASH1, DATA_SIZE).unwrap();

    store
        .update_oneshot(digest, original_data.clone().into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    // This value must be larger than `max_size` in the config above.
    let rt_data = store
        .get_part_unchunked(digest, START_READ_BYTE as u64, None)
        .await
        .err_tip(|| "Failed to get_part from dedup store")?;

    assert_eq!(
        rt_data.len(),
        DATA_SIZE - START_READ_BYTE,
        "Expected round trip sizes to match"
    );
    assert_eq!(
        rt_data,
        original_data[START_READ_BYTE..],
        "Expected round trip data to match"
    );
    Ok(())
}

#[nativelink_test]
async fn check_chunk_boundary_reads_test() -> Result<(), Error> {
    const DATA_SIZE: usize = 30;
    const START_READ_BYTE: usize = 10;

    let store = DedupStore::new(
        &nativelink_config::stores::DedupSpec {
            index_store: StoreRef::new("index", MemorySpec::default()),
            content_store: StoreRef::new("content", MemorySpec::default()),
            min_size: 5,
            normal_size: 6,
            max_size: 7,
            max_concurrent_fetch_per_get: 10,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())), // Index store.
        Store::new(MemoryStore::new(&MemorySpec::default())), // Content store.
    )?;

    let original_data = make_random_data(DATA_SIZE);
    let digest = DigestInfo::try_new(VALID_HASH1, DATA_SIZE).unwrap();
    store
        .update_oneshot(digest, original_data.clone().into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    for offset in 0..=DATA_SIZE {
        for len in 0..DATA_SIZE {
            // If reading at DATA_SIZE, we will set len to None to check that edge case.
            let maybe_len = if offset == DATA_SIZE {
                None
            } else {
                Some(len as u64)
            };
            let len = if maybe_len.is_none() { DATA_SIZE } else { len };

            let rt_data = store
                .get_part_unchunked(digest, offset as u64, maybe_len)
                .await
                .err_tip(|| "Failed to get_part from dedup store")?;

            let len_fenced = std::cmp::min(len, rt_data.len());
            assert_eq!(
                rt_data.len(),
                len_fenced,
                "Expected round trip sizes to match"
            );
            assert_eq!(
                rt_data,
                original_data[offset..(offset + len_fenced)],
                "Expected round trip data to match"
            );
        }
    }

    // This value must be larger than `max_size` in the config above.
    let rt_data = store
        .get_part_unchunked(digest, START_READ_BYTE as u64, None)
        .await
        .err_tip(|| "Failed to get_part from dedup store")?;

    assert_eq!(
        rt_data.len(),
        DATA_SIZE - START_READ_BYTE,
        "Expected round trip sizes to match"
    );
    assert_eq!(
        rt_data,
        original_data[START_READ_BYTE..],
        "Expected round trip data to match"
    );
    Ok(())
}

/// Ensure that when we run a `.has()` on a dedup store it will check to ensure all indexed
/// content items exist instead of just checking the entry in the index store.
#[nativelink_test]
async fn has_checks_content_store() -> Result<(), Error> {
    const DATA_SIZE: usize = MEGABYTE_SZ / 4;

    let index_store = MemoryStore::new(&nativelink_config::stores::MemorySpec::default());
    let content_store = MemoryStore::new(&nativelink_config::stores::MemorySpec {
        eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
            max_count: 10,
            ..Default::default()
        }),
    });

    let store = DedupStore::new(
        &make_default_config(),
        Store::new(index_store.clone()),
        Store::new(content_store.clone()),
    )?;

    let original_data = make_random_data(DATA_SIZE);
    let digest1 = DigestInfo::try_new(VALID_HASH1, DATA_SIZE).unwrap();

    store
        .update_oneshot(digest1, original_data.clone().into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    {
        // Check to ensure we our baseline `.has()` succeeds.
        let size_info = store.has(digest1).await.err_tip(|| "Failed to run .has")?;
        assert_eq!(size_info, Some(DATA_SIZE as u64), "Expected sizes to match");
    }
    {
        // There will be exactly 10 entries in our content_store based on our random seed data.
        // If we add one more it will evict a single item from that will still be indexed.
        // By doing this, we now check that it returns false when we call `.has()`.
        const DATA2: &str = "1234";
        let digest2 = DigestInfo::try_new(VALID_HASH2, DATA2.len()).unwrap();
        store
            .update_oneshot(digest2, DATA2.into())
            .await
            .err_tip(|| "Failed to write data to dedup store")?;

        {
            // Check our recently added entry is still valid.
            let size_info = store.has(digest2).await.err_tip(|| "Failed to run .has")?;
            assert_eq!(
                size_info,
                Some(DATA2.len() as u64),
                "Expected sizes to match"
            );
        }
        {
            // Check our first added entry is now invalid (because part of it was evicted).
            let size_info = store.has(digest1).await.err_tip(|| "Failed to run .has")?;
            assert_eq!(
                size_info, None,
                "Expected .has() to return None (not found)"
            );
        }
    }

    Ok(())
}

/// Ensure that when we run a `.has()` on a dedup store and the index does not exist it will
/// properly return None.
#[nativelink_test]
async fn has_with_no_existing_index_returns_none_test() -> Result<(), Error> {
    const DATA_SIZE: usize = 10;

    let index_store = MemoryStore::new(&nativelink_config::stores::MemorySpec::default());
    let content_store = MemoryStore::new(&nativelink_config::stores::MemorySpec {
        eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
            max_count: 10,
            ..Default::default()
        }),
    });

    let store = DedupStore::new(
        &make_default_config(),
        Store::new(index_store.clone()),
        Store::new(content_store.clone()),
    )?;

    let digest = DigestInfo::try_new(VALID_HASH1, DATA_SIZE).unwrap();

    {
        let size_info = store.has(digest).await.err_tip(|| "Failed to run .has")?;
        assert_eq!(
            size_info, None,
            "Expected None to be returned, got {:?}",
            size_info
        );
    }
    Ok(())
}

/// Ensure that when we run a `.has()` on a dedup store to check for empty blobs it will
/// properly return Some(0).
#[nativelink_test]
async fn has_with_zero_digest_returns_some_test() -> Result<(), Error> {
    let index_store = MemoryStore::new(&nativelink_config::stores::MemorySpec::default());
    let content_store = MemoryStore::new(&nativelink_config::stores::MemorySpec {
        eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
            max_count: 10,
            ..Default::default()
        }),
    });

    let store = DedupStore::new(
        &make_default_config(),
        Store::new(index_store.clone()),
        Store::new(content_store.clone()),
    )?;

    let digest = ZERO_BYTE_DIGESTS[0];

    {
        let size_info = store.has(digest).await.err_tip(|| "Failed to run .has")?;
        assert_eq!(
            size_info,
            Some(0),
            "Expected Sone(0) to be returned, got {:?}",
            size_info
        );
    }
    Ok(())
}
