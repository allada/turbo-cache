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

use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::default_store_factory::make_and_add_store_to_manager;
use nativelink_store::store_manager::StoreManager;

#[nativelink_test]
async fn same_datasource_disallowed_simple() -> Result<(), Error> {
    let store_manager = Arc::new(StoreManager::new());
    assert!(make_and_add_store_to_manager(
        "main_cas",
        &StoreSpec::memory(MemorySpec::default()),
        &store_manager,
        None,
    )
    .await
    .is_ok());

    assert!(make_and_add_store_to_manager(
        "main_ac",
        &StoreSpec::memory(MemorySpec::default()),
        &store_manager,
        None,
    )
    .await
    .is_ok());

    let existing_cas = store_manager.get_store("main_cas").unwrap();
    store_manager.add_store("different_store", existing_cas)?;

    Ok(())
}
