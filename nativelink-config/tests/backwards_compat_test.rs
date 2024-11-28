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

use std::collections::HashMap;

use nativelink_config::NamedConfigs;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize, PartialEq)]
struct DummySpec {
    memory: HashMap<String, String>,
}

#[test]
fn test_configs_deserialization() {
    let expected = json!([
        {
            "name": "store1",
            "memory": {}
        },
        {
            "name": "store2",
            "memory": {}
        }
    ]);

    let config: NamedConfigs<DummySpec> = serde_json::from_value(expected.clone()).unwrap();
    let mut new_stores: Vec<_> = config.into_iter().collect();
    new_stores.sort_by(|a, b| a.name.cmp(&b.name));

    let old_format = json!({
        "store1": { "memory": {} },
        "store2": { "memory": {} }
    });

    let config: NamedConfigs<DummySpec> = serde_json::from_value(old_format).unwrap();
    let mut transformed_stores: Vec<_> = config.into_iter().collect();
    transformed_stores.sort_by(|a, b| a.name.cmp(&b.name));

    assert_eq!(new_stores.len(), transformed_stores.len());
    assert_eq!(new_stores[0].name, transformed_stores[0].name);
    assert_eq!(new_stores[1].name, transformed_stores[1].name);
}
