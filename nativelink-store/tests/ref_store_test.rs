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

use std::ptr::from_ref;
use std::sync::Arc;

use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::ref_store::RefStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreDriver, StoreLike};
use pretty_assertions::assert_eq;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

fn setup_stores() -> (Arc<StoreManager>, Store, Store) {
    let store_manager = Arc::new(StoreManager::new());

    let memory_store = Store::new(MemoryStore::new(
        &nativelink_config::stores::MemorySpec::default(),
    ));
    store_manager.add_store("foo", memory_store.clone());

    let ref_store = Store::new(RefStore::new(
        &nativelink_config::stores::RefSpec {
            name: "foo".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));
    store_manager.add_store("bar", ref_store.clone());
    (store_manager, memory_store, ref_store)
}

#[nativelink_test]
async fn has_test() -> Result<(), Error> {
    const VALUE1: &str = "13";

    let (_store_manager, memory_store, ref_store) = setup_stores();

    {
        // Insert data into memory store.
        memory_store
            .update_oneshot(
                DigestInfo::try_new(VALID_HASH1, VALUE1.len())?,
                VALUE1.into(),
            )
            .await?;
    }
    {
        // Now check if we check of ref_store has the data.
        let has_result = ref_store
            .has(DigestInfo::try_new(VALID_HASH1, VALUE1.len())?)
            .await;
        assert_eq!(
            has_result,
            Ok(Some(VALUE1.len() as u64)),
            "Expected ref store to have data in ref store : {}",
            VALID_HASH1
        );
    }
    Ok(())
}

#[nativelink_test]
async fn get_test() -> Result<(), Error> {
    const VALUE1: &str = "13";

    let (_store_manager, memory_store, ref_store) = setup_stores();

    {
        // Insert data into memory store.
        memory_store
            .update_oneshot(
                DigestInfo::try_new(VALID_HASH1, VALUE1.len())?,
                VALUE1.into(),
            )
            .await?;
    }
    {
        // Now check if we read it from ref_store it has same data.
        let data = ref_store
            .get_part_unchunked(DigestInfo::try_new(VALID_HASH1, VALUE1.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            VALUE1.as_bytes(),
            "Expected ref store to have data in ref store : {}",
            VALID_HASH1
        );
    }
    Ok(())
}

#[nativelink_test]
async fn update_test() -> Result<(), Error> {
    const VALUE1: &str = "13";

    let (_store_manager, memory_store, ref_store) = setup_stores();

    {
        // Insert data into ref_store.
        ref_store
            .update_oneshot(
                DigestInfo::try_new(VALID_HASH1, VALUE1.len())?,
                VALUE1.into(),
            )
            .await?;
    }
    {
        // Now check if we read it from memory_store it has same data.
        let data = memory_store
            .get_part_unchunked(DigestInfo::try_new(VALID_HASH1, VALUE1.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            VALUE1.as_bytes(),
            "Expected ref store to have data in memory store : {}",
            VALID_HASH1
        );
    }
    Ok(())
}

#[nativelink_test]
async fn inner_store_test() -> Result<(), Error> {
    let store_manager = Arc::new(StoreManager::new());

    let memory_store = Store::new(MemoryStore::new(
        &nativelink_config::stores::MemorySpec::default(),
    ));
    store_manager.add_store("mem_store", memory_store.clone());

    let ref_store_inner = Store::new(RefStore::new(
        &nativelink_config::stores::RefSpec {
            name: "mem_store".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));
    store_manager.add_store("ref_store_inner", ref_store_inner.clone());

    let ref_store_outer = Store::new(RefStore::new(
        &nativelink_config::stores::RefSpec {
            name: "ref_store_inner".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));
    store_manager.add_store("ref_store_outer", ref_store_outer.clone());

    // Ensure the result of inner_store() points to exact same memory store.
    assert_eq!(
        from_ref::<dyn StoreDriver>(ref_store_outer.inner_store(Option::<DigestInfo>::None))
            .cast::<()>(),
        from_ref::<dyn StoreDriver>(memory_store.into_inner().as_ref()).cast::<()>(),
        "Expected inner store to be memory store"
    );
    Ok(())
}
