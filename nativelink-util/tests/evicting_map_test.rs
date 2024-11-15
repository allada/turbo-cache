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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mock_instant::thread_local::MockClock;
use nativelink_config::stores::EvictionPolicy;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_util::common::DigestInfo;
use nativelink_util::evicting_map::{EvictingMap, LenEntry};
use nativelink_util::instant_wrapper::MockInstantWrapped;
use pretty_assertions::assert_eq;

#[derive(Clone, PartialEq, Debug)]
pub struct BytesWrapper(Bytes);

impl LenEntry for BytesWrapper {
    #[inline]
    fn len(&self) -> u64 {
        Bytes::len(&self.0) as u64
    }

    #[inline]
    fn is_empty(&self) -> bool {
        Bytes::is_empty(&self.0)
    }
}

impl From<Bytes> for BytesWrapper {
    #[inline]
    fn from(bytes: Bytes) -> BytesWrapper {
        BytesWrapper(bytes)
    }
}

const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
const HASH2: &str = "123456789abcdef000000000000000000000000000000000123456789abcdef1";
const HASH3: &str = "23456789abcdef000000000000000000000000000000000123456789abcdef12";
const HASH4: &str = "3456789abcdef000000000000000000000000000000000123456789abcdef012";

#[nativelink_test]
async fn insert_purges_at_max_count() -> Result<(), Error> {
    let evicting_map = EvictingMap::<DigestInfo, BytesWrapper, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 3,
            max_seconds: 0,
            max_bytes: 0,
            evict_bytes: 0,
        },
        MockInstantWrapped::default(),
    );
    evicting_map
        .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::new().into())
        .await;
    evicting_map
        .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::new().into())
        .await;
    evicting_map
        .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::new().into())
        .await;
    evicting_map
        .insert(DigestInfo::try_new(HASH4, 0)?, Bytes::new().into())
        .await;

    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH1, 0)?)
            .await,
        None,
        "Expected map to not have item 1"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH2, 0)?)
            .await,
        Some(0),
        "Expected map to have item 2"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH3, 0)?)
            .await,
        Some(0),
        "Expected map to have item 3"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH4, 0)?)
            .await,
        Some(0),
        "Expected map to have item 4"
    );

    Ok(())
}

#[nativelink_test]
async fn insert_purges_at_max_bytes() -> Result<(), Error> {
    const DATA: &str = "12345678";
    let evicting_map = EvictingMap::<DigestInfo, BytesWrapper, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 0,
            max_seconds: 0,
            max_bytes: 17,
            evict_bytes: 0,
        },
        MockInstantWrapped::default(),
    );
    evicting_map
        .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::from(DATA).into())
        .await;
    evicting_map
        .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::from(DATA).into())
        .await;
    evicting_map
        .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::from(DATA).into())
        .await;
    evicting_map
        .insert(DigestInfo::try_new(HASH4, 0)?, Bytes::from(DATA).into())
        .await;

    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH1, 0)?)
            .await,
        None,
        "Expected map to not have item 1"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH2, 0)?)
            .await,
        None,
        "Expected map to not have item 2"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH3, 0)?)
            .await,
        Some(DATA.len() as u64),
        "Expected map to have item 3"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH4, 0)?)
            .await,
        Some(DATA.len() as u64),
        "Expected map to have item 4"
    );

    Ok(())
}

#[nativelink_test]
async fn insert_purges_to_low_watermark_at_max_bytes() -> Result<(), Error> {
    const DATA: &str = "12345678";
    let evicting_map = EvictingMap::<DigestInfo, BytesWrapper, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 0,
            max_seconds: 0,
            max_bytes: 17,
            evict_bytes: 9,
        },
        MockInstantWrapped::default(),
    );
    evicting_map
        .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::from(DATA).into())
        .await;
    evicting_map
        .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::from(DATA).into())
        .await;
    evicting_map
        .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::from(DATA).into())
        .await;
    evicting_map
        .insert(DigestInfo::try_new(HASH4, 0)?, Bytes::from(DATA).into())
        .await;

    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH1, 0)?)
            .await,
        None,
        "Expected map to not have item 1"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH2, 0)?)
            .await,
        None,
        "Expected map to not have item 2"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH3, 0)?)
            .await,
        None,
        "Expected map to not have item 3"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH4, 0)?)
            .await,
        Some(DATA.len() as u64),
        "Expected map to have item 4"
    );

    Ok(())
}

#[nativelink_test]
async fn insert_purges_at_max_seconds() -> Result<(), Error> {
    const DATA: &str = "12345678";

    let evicting_map = EvictingMap::<DigestInfo, BytesWrapper, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 0,
            max_seconds: 5,
            max_bytes: 0,
            evict_bytes: 0,
        },
        MockInstantWrapped::default(),
    );

    evicting_map
        .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::from(DATA).into())
        .await;
    MockClock::advance(Duration::from_secs(2));
    evicting_map
        .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::from(DATA).into())
        .await;
    MockClock::advance(Duration::from_secs(2));
    evicting_map
        .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::from(DATA).into())
        .await;
    MockClock::advance(Duration::from_secs(2));
    evicting_map
        .insert(DigestInfo::try_new(HASH4, 0)?, Bytes::from(DATA).into())
        .await;

    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH1, 0)?)
            .await,
        None,
        "Expected map to not have item 1"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH2, 0)?)
            .await,
        Some(DATA.len() as u64),
        "Expected map to have item 2"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH3, 0)?)
            .await,
        Some(DATA.len() as u64),
        "Expected map to have item 3"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH4, 0)?)
            .await,
        Some(DATA.len() as u64),
        "Expected map to have item 4"
    );

    Ok(())
}

#[nativelink_test]
async fn get_refreshes_time() -> Result<(), Error> {
    const DATA: &str = "12345678";

    let evicting_map = EvictingMap::<DigestInfo, BytesWrapper, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 0,
            max_seconds: 3,
            max_bytes: 0,
            evict_bytes: 0,
        },
        MockInstantWrapped::default(),
    );

    evicting_map
        .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::from(DATA).into())
        .await;
    MockClock::advance(Duration::from_secs(2));
    evicting_map
        .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::from(DATA).into())
        .await;
    MockClock::advance(Duration::from_secs(2));
    evicting_map.get(&DigestInfo::try_new(HASH1, 0)?).await; // HASH1 should now be last to be evicted.
    MockClock::advance(Duration::from_secs(2));
    evicting_map
        .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::from(DATA).into())
        .await; // This will trigger an eviction.

    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH1, 0)?)
            .await,
        None,
        "Expected map to not have item 1"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH2, 0)?)
            .await,
        None,
        "Expected map to not have item 2"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH3, 0)?)
            .await,
        Some(DATA.len() as u64),
        "Expected map to have item 3"
    );

    Ok(())
}

#[nativelink_test]
async fn unref_called_on_replace() -> Result<(), Error> {
    #[derive(Debug)]
    struct MockEntry {
        data: Bytes,
        unref_called: AtomicBool,
    }

    impl LenEntry for MockEntry {
        fn len(&self) -> u64 {
            // Note: We are not testing this functionality.
            0
        }

        fn is_empty(&self) -> bool {
            unreachable!("We are not testing this functionality");
        }

        async fn unref(&self) {
            self.unref_called.store(true, Ordering::Relaxed);
        }
    }

    const DATA1: &str = "12345678";
    const DATA2: &str = "87654321";

    let evicting_map = EvictingMap::<DigestInfo, Arc<MockEntry>, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 1,
            max_seconds: 0,
            max_bytes: 0,
            evict_bytes: 0,
        },
        MockInstantWrapped::default(),
    );

    let (entry1, entry2) = {
        let entry1 = Arc::new(MockEntry {
            data: Bytes::from(DATA1),
            unref_called: AtomicBool::new(false),
        });
        evicting_map
            .insert(DigestInfo::try_new(HASH1, 0)?, entry1.clone())
            .await;

        let entry2 = Arc::new(MockEntry {
            data: Bytes::from(DATA2),
            unref_called: AtomicBool::new(false),
        });
        evicting_map
            .insert(DigestInfo::try_new(HASH1, 0)?, entry2.clone())
            .await;
        (entry1, entry2)
    };

    let existing_entry = evicting_map
        .get(&DigestInfo::try_new(HASH1, 0)?)
        .await
        .unwrap();
    assert_eq!(existing_entry.data, DATA2);

    assert!(entry1.unref_called.load(Ordering::Relaxed));
    assert!(!entry2.unref_called.load(Ordering::Relaxed));

    Ok(())
}

#[nativelink_test]
async fn contains_key_refreshes_time() -> Result<(), Error> {
    const DATA: &str = "12345678";

    let evicting_map = EvictingMap::<DigestInfo, BytesWrapper, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 0,
            max_seconds: 3,
            max_bytes: 0,
            evict_bytes: 0,
        },
        MockInstantWrapped::default(),
    );

    evicting_map
        .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::from(DATA).into())
        .await;
    MockClock::advance(Duration::from_secs(2));
    evicting_map
        .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::from(DATA).into())
        .await;
    MockClock::advance(Duration::from_secs(2));
    evicting_map
        .size_for_key(&DigestInfo::try_new(HASH1, 0)?)
        .await; // HASH1 should now be last to be evicted.
    MockClock::advance(Duration::from_secs(2));
    evicting_map
        .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::from(DATA).into())
        .await; // This will trigger an eviction.

    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH1, 0)?)
            .await,
        None,
        "Expected map to not have item 1"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH2, 0)?)
            .await,
        None,
        "Expected map to not have item 2"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH3, 0)?)
            .await,
        Some(8),
        "Expected map to have item 3"
    );

    Ok(())
}

#[nativelink_test]
async fn hashes_equal_sizes_different_doesnt_override() -> Result<(), Error> {
    let evicting_map = EvictingMap::<DigestInfo, BytesWrapper, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 0,
            max_seconds: 0,
            max_bytes: 0,
            evict_bytes: 0,
        },
        MockInstantWrapped::default(),
    );

    let value1 = BytesWrapper(Bytes::from_static(b"12345678"));
    let value2 = BytesWrapper(Bytes::from_static(b"87654321"));
    evicting_map
        .insert(DigestInfo::try_new(HASH1, 0)?, value1.clone())
        .await;
    evicting_map
        .insert(DigestInfo::try_new(HASH1, 1)?, value2.clone())
        .await;
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH1, 0)?)
            .await,
        Some(value1.len()),
        "HASH1/0 should exist"
    );
    assert_eq!(
        evicting_map
            .size_for_key(&DigestInfo::try_new(HASH1, 1)?)
            .await,
        Some(value2.len()),
        "HASH1/1 should exist"
    );

    assert_eq!(
        evicting_map
            .get(&DigestInfo::try_new(HASH1, 0)?)
            .await
            .unwrap(),
        value1
    );
    assert_eq!(
        evicting_map
            .get(&DigestInfo::try_new(HASH1, 1)?)
            .await
            .unwrap(),
        value2
    );

    Ok(())
}

#[nativelink_test]
async fn get_evicts_on_time() -> Result<(), Error> {
    const DATA: &str = "12345678";

    let evicting_map = EvictingMap::<DigestInfo, BytesWrapper, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 0,
            max_seconds: 5,
            max_bytes: 0,
            evict_bytes: 0,
        },
        MockInstantWrapped::default(),
    );

    let digest_info1: DigestInfo = DigestInfo::try_new(HASH1, 0)?;
    evicting_map
        .insert(digest_info1, Bytes::from(DATA).into())
        .await;

    // Getting from map before time has expired should return the value.
    assert_eq!(
        evicting_map.get(&digest_info1).await,
        Some(Bytes::from(DATA).into())
    );

    MockClock::advance(Duration::from_secs(10));

    // Getting from map after time has expired should return None.
    assert_eq!(evicting_map.get(&digest_info1).await, None);

    Ok(())
}

#[nativelink_test]
async fn remove_evicts_on_time() -> Result<(), Error> {
    const DATA: &str = "12345678";

    let evicting_map = EvictingMap::<DigestInfo, BytesWrapper, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 0,
            max_seconds: 5,
            max_bytes: 0,
            evict_bytes: 0,
        },
        MockInstantWrapped::default(),
    );

    let digest_info1: DigestInfo = DigestInfo::try_new(HASH1, 0)?;
    evicting_map
        .insert(digest_info1, Bytes::from(DATA).into())
        .await;

    let digest_info2: DigestInfo = DigestInfo::try_new(HASH2, 0)?;
    evicting_map
        .insert(digest_info2, Bytes::from(DATA).into())
        .await;

    // Removing digest before time has expired should return true.
    assert!(evicting_map.remove(&digest_info2).await);

    MockClock::advance(Duration::from_secs(10));

    // Removing digest after time has expired should return false.
    assert!(!evicting_map.remove(&digest_info1).await);

    Ok(())
}

#[nativelink_test]
async fn range_multiple_items_test() -> Result<(), Error> {
    async fn get_map_range(
        evicting_map: &EvictingMap<String, BytesWrapper, MockInstantWrapped>,
        range: impl std::ops::RangeBounds<String>,
    ) -> Vec<(String, Bytes)> {
        let mut found_values = Vec::new();
        evicting_map
            .range(range, |k, v: &BytesWrapper| {
                found_values.push((k.clone(), v.0.clone()));
                true
            })
            .await;
        found_values
    }

    const KEY1: &str = "key-123";
    const DATA1: &str = "123";

    const KEY2: &str = "key-234";
    const DATA2: &str = "234";

    const KEY3: &str = "key-345";
    const DATA3: &str = "345";

    let evicting_map = EvictingMap::<String, BytesWrapper, MockInstantWrapped>::new(
        &EvictionPolicy {
            max_count: 0,
            max_seconds: 0,
            max_bytes: 0,
            evict_bytes: 0,
        },
        MockInstantWrapped::default(),
    );

    evicting_map
        .insert(KEY1.into(), Bytes::from(DATA1).into())
        .await;

    evicting_map
        .insert(KEY2.into(), Bytes::from(DATA2).into())
        .await;

    evicting_map
        .insert(KEY3.into(), Bytes::from(DATA3).into())
        .await;

    {
        // Ensure all range works.
        let expected_values = vec![
            (KEY1.to_string(), Bytes::from(DATA1)),
            (KEY2.to_string(), Bytes::from(DATA2)),
            (KEY3.to_string(), Bytes::from(DATA3)),
        ];
        let found_values = get_map_range(&evicting_map, ..).await;
        assert_eq!(expected_values, found_values);
    }
    {
        // Ensure prefix but everything range works.
        let expected_values = vec![
            (KEY1.to_string(), Bytes::from(DATA1)),
            (KEY2.to_string(), Bytes::from(DATA2)),
            (KEY3.to_string(), Bytes::from(DATA3)),
        ];
        let found_values = get_map_range(&evicting_map, "key-".to_string()..).await;
        assert_eq!(expected_values, found_values);
    }
    {
        // Ensure prefix range with everything after "key-2" works.
        let expected_values = vec![
            (KEY2.to_string(), Bytes::from(DATA2)),
            (KEY3.to_string(), Bytes::from(DATA3)),
        ];
        let found_values = get_map_range(&evicting_map, "key-2".to_string()..).await;
        assert_eq!(expected_values, found_values);
    }
    {
        // Ensure prefix range with only KEY2.
        let expected_values = vec![(KEY2.to_string(), Bytes::from(DATA2))];
        let found_values = get_map_range(&evicting_map, KEY2.to_string()..KEY3.to_string()).await;
        assert_eq!(expected_values, found_values);
    }

    Ok(())
}
