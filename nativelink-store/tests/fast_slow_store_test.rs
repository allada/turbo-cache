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

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::noop_store::NoopStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{default_health_status_indicator, HealthStatusIndicator};
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey, StoreLike};
use pretty_assertions::assert_eq;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const MEGABYTE_SZ: usize = 1024 * 1024;

fn make_stores() -> (Store, Store, Store) {
    let fast_store = Store::new(MemoryStore::new(
        &nativelink_config::stores::MemoryStore::default(),
    ));
    let slow_store = Store::new(MemoryStore::new(
        &nativelink_config::stores::MemoryStore::default(),
    ));
    let fast_slow_store = Store::new(FastSlowStore::new(
        &nativelink_config::stores::FastSlowStore {
            fast: nativelink_config::stores::StoreConfig::memory(
                nativelink_config::stores::MemoryStore::default(),
            ),
            slow: nativelink_config::stores::StoreConfig::memory(
                nativelink_config::stores::MemoryStore::default(),
            ),
        },
        fast_store.clone(),
        slow_store.clone(),
    ));
    (fast_slow_store, fast_store, slow_store)
}

fn make_random_data(sz: usize) -> Vec<u8> {
    let mut value = vec![0u8; sz];
    let mut rng = SmallRng::seed_from_u64(1);
    rng.fill(&mut value[..]);
    value
}

async fn check_data(
    check_store: &Store,
    digest: DigestInfo,
    original_data: &Vec<u8>,
    debug_name: &str,
) -> Result<(), Error> {
    assert!(
        check_store.has(digest).await?.is_some(),
        "Expected data to exist in {debug_name} store"
    );

    let store_data = check_store.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        store_data, original_data,
        "Expected data to match in {debug_name} store"
    );
    Ok(())
}

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

#[nativelink_test]
async fn write_large_amount_to_both_stores_test() -> Result<(), Error> {
    let (store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(20 * MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    check_data(&store, digest, &original_data, "fast_slow").await?;
    check_data(&fast_store, digest, &original_data, "fast").await?;
    check_data(&slow_store, digest, &original_data, "slow").await?;

    Ok(())
}

#[nativelink_test]
async fn fetch_slow_store_puts_in_fast_store_test() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    assert_eq!(
        fast_slow_store.has(digest).await,
        Ok(Some(original_data.len() as u64))
    );
    assert_eq!(fast_store.has(digest).await, Ok(None));
    assert_eq!(
        slow_store.has(digest).await,
        Ok(Some(original_data.len() as u64))
    );

    // This get() request should place the data in fast_store too.
    fast_slow_store.get_part_unchunked(digest, 0, None).await?;

    // Now the data should exist in all the stores.
    check_data(&fast_store, digest, &original_data, "fast_store").await?;
    check_data(&slow_store, digest, &original_data, "slow_store").await?;

    Ok(())
}

#[nativelink_test]
async fn partial_reads_copy_full_to_fast_store_test() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    // This get() request should place the data in fast_store too.
    assert_eq!(
        original_data[10..60],
        fast_slow_store
            .get_part_unchunked(digest, 10, Some(50))
            .await?
    );

    // Full data should exist in the fast store even though only partially
    // read.
    check_data(&slow_store, digest, &original_data, "slow_store").await?;
    check_data(&fast_store, digest, &original_data, "fast_store").await?;

    Ok(())
}

#[test]
fn calculate_range_test() {
    let test =
        |start_range, end_range| FastSlowStore::calculate_range(&start_range, &end_range).unwrap();
    {
        // Exact match.
        let received_range = 0..1;
        let send_range = 0..1;
        let expected_results = Some(0..1);
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Minus one on received_range.
        let received_range = 1..4;
        let send_range = 1..5;
        let expected_results = Some(0..3);
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Minus one on send_range.
        let received_range = 1..5;
        let send_range = 1..4;
        let expected_results = Some(0..3);
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Should have already sent all data (start fence post).
        let received_range = 1..2;
        let send_range = 0..1;
        let expected_results = None;
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Definiltly already sent data.
        let received_range = 2..3;
        let send_range = 0..1;
        let expected_results = None;
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // All data should be sent (inside range).
        let received_range = 3..4;
        let send_range = 0..100;
        let expected_results = Some(0..1); // Note: This is relative received_range.start.
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Subset of received data should be sent.
        let received_range = 1..100;
        let send_range = 3..4;
        let expected_results = Some(2..3); // Note: This is relative received_range.start.
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // We are clearly not at the offset yet.
        let received_range = 0..1;
        let send_range = 3..4;
        let expected_results = None;
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Not at offset yet (fence post).
        let received_range = 0..1;
        let send_range = 1..2;
        let expected_results = None;
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Head part of the received data should be sent.
        let received_range = 1..3;
        let send_range = 2..5;
        let expected_results = Some(1..2);
        assert_eq!(test(received_range, send_range), expected_results);
    }
}

#[nativelink_test]
async fn drop_on_eof_completes_store_futures() -> Result<(), Error> {
    #[derive(MetricsComponent)]
    struct DropCheckStore {
        drop_flag: Arc<AtomicBool>,
        read_rx: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        eof_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        digest: Option<DigestInfo>,
    }

    #[async_trait]
    impl StoreDriver for DropCheckStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            if let Some(has_digest) = self.digest {
                for (digest, result) in digests.iter().zip(results.iter_mut()) {
                    if *digest == has_digest.into() {
                        *result = Some(has_digest.size_bytes());
                    }
                }
            }
            Ok(())
        }

        async fn update(
            self: Pin<&Self>,
            _digest: StoreKey<'_>,
            mut reader: nativelink_util::buf_channel::DropCloserReadHalf,
            _size_info: nativelink_util::store_trait::UploadSizeInfo,
        ) -> Result<(), Error> {
            // Gets called in the fast store and we don't need to do
            // anything.  Should only complete when drain has finished.
            reader.drain().await?;
            let eof_tx = self.eof_tx.lock().unwrap().take();
            if let Some(tx) = eof_tx {
                tx.send(())
                    .map_err(|e| make_err!(Code::Internal, "{:?}", e))?;
            }
            let read_rx = self.read_rx.lock().unwrap().take();
            if let Some(rx) = read_rx {
                rx.await.map_err(|e| make_err!(Code::Internal, "{:?}", e))?;
            }
            Ok(())
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            // Gets called in the slow store and we provide the data that's
            // sent to the upstream and the fast store.
            let bytes = length.unwrap_or(key.into_digest().size_bytes()) - offset;
            let data = vec![0_u8; usize::try_from(bytes).expect("Invalid length")];
            writer.send(Bytes::copy_from_slice(&data)).await?;
            writer.send_eof()
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn std::any::Any + Sync + Send + 'static) {
            self
        }

        fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send + 'static> {
            self
        }
    }

    impl Drop for DropCheckStore {
        fn drop(&mut self) {
            self.drop_flag.store(true, Ordering::Release);
        }
    }

    default_health_status_indicator!(DropCheckStore);

    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    let (fast_store_read_tx, fast_store_read_rx) = tokio::sync::oneshot::channel();
    let (fast_store_eof_tx, fast_store_eof_rx) = tokio::sync::oneshot::channel();
    let fast_store_dropped = Arc::new(AtomicBool::new(false));
    let fast_store = Store::new(Arc::new(DropCheckStore {
        drop_flag: fast_store_dropped.clone(),
        eof_tx: Mutex::new(Some(fast_store_eof_tx)),
        read_rx: Mutex::new(Some(fast_store_read_rx)),
        digest: None,
    }));
    let slow_store_dropped = Arc::new(AtomicBool::new(false));
    let slow_store = Store::new(Arc::new(DropCheckStore {
        drop_flag: slow_store_dropped,
        eof_tx: Mutex::new(None),
        read_rx: Mutex::new(None),
        digest: Some(digest),
    }));

    let fast_slow_store = FastSlowStore::new(
        &nativelink_config::stores::FastSlowStore {
            fast: nativelink_config::stores::StoreConfig::memory(
                nativelink_config::stores::MemoryStore::default(),
            ),
            slow: nativelink_config::stores::StoreConfig::memory(
                nativelink_config::stores::MemoryStore::default(),
            ),
        },
        fast_store,
        slow_store,
    );

    let (tx, mut rx) = make_buf_channel_pair();
    let (get_res, read_res) = tokio::join!(
        async move {
            // Drop get_part as soon as rx.drain() completes
            tokio::select!(
                res = rx.drain() => res,
                res = fast_slow_store.get_part(digest, tx, 0, Some(digest.size_bytes())) => res,
            )
        },
        async move {
            fast_store_eof_rx
                .await
                .map_err(|e| make_err!(Code::Internal, "{:?}", e))?;
            // Give a couple of cycles for dropping to occur if it's going to.
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            if fast_store_dropped.load(Ordering::Acquire) {
                return Err(make_err!(Code::Internal, "Fast store was dropped!"));
            }
            fast_store_read_tx
                .send(())
                .map_err(|e| make_err!(Code::Internal, "{:?}", e))?;
            Ok::<_, Error>(())
        }
    );
    get_res.merge(read_res)
}

#[nativelink_test]
async fn ignore_value_in_fast_store() -> Result<(), Error> {
    let fast_store = Store::new(MemoryStore::new(
        &nativelink_config::stores::MemoryStore::default(),
    ));
    let slow_store = Store::new(MemoryStore::new(
        &nativelink_config::stores::MemoryStore::default(),
    ));
    let fast_slow_store = Arc::new(FastSlowStore::new(
        &nativelink_config::stores::FastSlowStore {
            fast: nativelink_config::stores::StoreConfig::memory(
                nativelink_config::stores::MemoryStore::default(),
            ),
            slow: nativelink_config::stores::StoreConfig::memory(
                nativelink_config::stores::MemoryStore::default(),
            ),
        },
        fast_store.clone(),
        slow_store,
    ));
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    fast_store
        .update_oneshot(digest, make_random_data(100).into())
        .await?;
    assert!(
        fast_slow_store.has(digest).await?.is_none(),
        "Expected data to not exist in store"
    );
    Ok(())
}

// Regression test for https://github.com/TraceMachina/nativelink/issues/665
#[nativelink_test]
async fn has_checks_fast_store_when_noop() -> Result<(), Error> {
    let fast_store = Store::new(MemoryStore::new(
        &nativelink_config::stores::MemoryStore::default(),
    ));
    let slow_store = Store::new(NoopStore::new());
    let fast_slow_store_config = nativelink_config::stores::FastSlowStore {
        fast: nativelink_config::stores::StoreConfig::memory(
            nativelink_config::stores::MemoryStore::default(),
        ),
        slow: nativelink_config::stores::StoreConfig::noop,
    };
    let fast_slow_store = Arc::new(FastSlowStore::new(
        &fast_slow_store_config,
        fast_store.clone(),
        slow_store.clone(),
    ));

    let data = make_random_data(100);
    let digest = DigestInfo::try_new(VALID_HASH, data.len()).unwrap();

    assert_eq!(
        fast_slow_store.has(digest).await,
        Ok(None),
        "Expected data to not exist in store"
    );

    // Upload some dummy data.
    fast_store
        .update_oneshot(digest, data.clone().into())
        .await?;

    assert_eq!(
        fast_slow_store.has(digest).await,
        Ok(Some(data.len() as u64)),
        "Expected data to exist in store"
    );

    assert_eq!(
        fast_slow_store.get_part_unchunked(digest, 0, None).await,
        Ok(data.into()),
        "Data read from store is not correct"
    );
    Ok(())
}
