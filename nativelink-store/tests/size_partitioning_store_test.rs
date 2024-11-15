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

use std::sync::Arc;

use nativelink_config::stores::{MemorySpec, SizePartitioningSpec, StoreRef};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::size_partitioning_store::SizePartitioningStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;

const BASE_SIZE_PART: u64 = 5;

const SMALL_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const SMALL_VALUE: &str = "99";

const BIG_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const BIG_VALUE: &str = "123456789";

fn setup_stores(
    size: u64,
) -> (
    Arc<SizePartitioningStore>,
    Arc<MemoryStore>,
    Arc<MemoryStore>,
) {
    let lower_memory_store = MemoryStore::new(&MemorySpec::default());
    let upper_memory_store = MemoryStore::new(&MemorySpec::default());

    let size_part_store = SizePartitioningStore::new(
        &SizePartitioningSpec {
            size,
            lower_store: StoreRef::new("lower", MemorySpec::default()),
            upper_store: StoreRef::new("upper", MemorySpec::default()),
        },
        Store::new(lower_memory_store.clone()),
        Store::new(upper_memory_store.clone()),
    );
    (size_part_store, lower_memory_store, upper_memory_store)
}

#[nativelink_test]
async fn has_test() -> Result<(), Error> {
    let (size_part_store, lower_memory_store, upper_memory_store) = setup_stores(BASE_SIZE_PART);

    {
        // Insert data into lower store.
        lower_memory_store
            .update_oneshot(
                DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?,
                SMALL_VALUE.into(),
            )
            .await?;

        // Insert data into upper store.
        upper_memory_store
            .update_oneshot(
                DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?,
                BIG_VALUE.into(),
            )
            .await?;
    }
    {
        // Check if our partition store has small data.
        let small_has_result = size_part_store
            .has(DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?)
            .await;
        assert_eq!(
            small_has_result,
            Ok(Some(SMALL_VALUE.len() as u64)),
            "Expected size part store to have data in ref store : {}",
            SMALL_HASH
        );
    }
    {
        // Check if our partition store has big data.
        let small_has_result = size_part_store
            .has(DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?)
            .await;
        assert_eq!(
            small_has_result,
            Ok(Some(BIG_VALUE.len() as u64)),
            "Expected size part store to have data in ref store : {}",
            BIG_HASH
        );
    }
    Ok(())
}

#[nativelink_test]
async fn get_test() -> Result<(), Error> {
    let (size_part_store, lower_memory_store, upper_memory_store) = setup_stores(BASE_SIZE_PART);

    {
        // Insert data into lower store.
        lower_memory_store
            .update_oneshot(
                DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?,
                SMALL_VALUE.into(),
            )
            .await?;

        // Insert data into upper store.
        upper_memory_store
            .update_oneshot(
                DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?,
                BIG_VALUE.into(),
            )
            .await?;
    }
    {
        // Read the partition store small data.
        let data = size_part_store
            .get_part_unchunked(DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            SMALL_VALUE.as_bytes(),
            "Expected size part store to have data in ref store : {}",
            SMALL_HASH
        );
    }
    {
        // Read the partition store big data.
        let data = size_part_store
            .get_part_unchunked(DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            BIG_VALUE.as_bytes(),
            "Expected size part store to have data in ref store : {}",
            BIG_HASH
        );
    }
    Ok(())
}

#[nativelink_test]
async fn update_test() -> Result<(), Error> {
    let (size_part_store, lower_memory_store, upper_memory_store) = setup_stores(BASE_SIZE_PART);

    {
        // Insert small data into ref_store.
        size_part_store
            .update_oneshot(
                DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?,
                SMALL_VALUE.into(),
            )
            .await?;

        // Insert small data into ref_store.
        size_part_store
            .update_oneshot(
                DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?,
                BIG_VALUE.into(),
            )
            .await?;
    }
    {
        // Check if we read small data from size_partition_store it has same data.
        let data = lower_memory_store
            .get_part_unchunked(DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            SMALL_VALUE.as_bytes(),
            "Expected size part store to have data in memory store : {}",
            SMALL_HASH
        );
    }
    {
        // Check if we read big data from size_partition_store it has same data.
        let data = upper_memory_store
            .get_part_unchunked(DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            BIG_VALUE.as_bytes(),
            "Expected size part store to have data in memory store : {}",
            BIG_HASH
        );
    }
    Ok(())
}
