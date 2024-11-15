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
use std::sync::Arc;

use async_trait::async_trait;
use nativelink_error::{make_input_err, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{
    make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf,
};
use nativelink_util::common::PackedHash;
use nativelink_util::digest_hasher::{
    default_digest_hasher_func, DigestHasher, ACTIVE_HASHER_FUNC,
};
use nativelink_util::health_utils::{default_health_status_indicator, HealthStatusIndicator};
use nativelink_util::metrics_utils::CounterWithTime;
use nativelink_util::origin_context::ActiveOriginContext;
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo};

#[derive(MetricsComponent)]
pub struct VerifyStore {
    #[metric(group = "inner_store")]
    inner_store: Store,
    #[metric(help = "If the verification store is verifying the size of the data")]
    verify_size: bool,
    #[metric(help = "If the verification store is verifying the hash of the data")]
    verify_hash: bool,

    // Metrics.
    #[metric(help = "Number of failures the verification store had due to size mismatches")]
    size_verification_failures: CounterWithTime,
    #[metric(help = "Number of failures the verification store had due to hash mismatches")]
    hash_verification_failures: CounterWithTime,
}

impl VerifyStore {
    pub fn new(config: &nativelink_config::stores::VerifySpec, inner_store: Store) -> Arc<Self> {
        Arc::new(VerifyStore {
            inner_store,
            verify_size: config.verify_size,
            verify_hash: config.verify_hash,
            size_verification_failures: CounterWithTime::default(),
            hash_verification_failures: CounterWithTime::default(),
        })
    }

    async fn inner_check_update<D: DigestHasher>(
        &self,
        mut tx: DropCloserWriteHalf,
        mut rx: DropCloserReadHalf,
        maybe_expected_digest_size: Option<u64>,
        original_hash: &PackedHash,
        mut maybe_hasher: Option<&mut D>,
    ) -> Result<(), Error> {
        let mut sum_size: u64 = 0;
        loop {
            let chunk = rx
                .recv()
                .await
                .err_tip(|| "Failed to read chunk in check_update in verify store")?;
            sum_size += chunk.len() as u64;

            // Ensure if a user sends us too much data we fail quickly.
            if let Some(expected_size) = maybe_expected_digest_size {
                match sum_size.cmp(&expected_size) {
                    std::cmp::Ordering::Greater => {
                        self.size_verification_failures.inc();
                        return Err(make_input_err!(
                            "Expected size {} but already received {} on insert",
                            expected_size,
                            sum_size
                        ));
                    }
                    std::cmp::Ordering::Equal => {
                        // Ensure our next chunk is the EOF chunk.
                        // If this was an error it'll be caught on the .recv()
                        // on next cycle.
                        if let Ok(eof_chunk) = rx.peek().await {
                            if !eof_chunk.is_empty() {
                                self.size_verification_failures.inc();
                                return Err(make_input_err!(
                                    "Expected EOF chunk when exact size was hit on insert in verify store - {}",
                                    expected_size,
                                ));
                            }
                        }
                    }
                    std::cmp::Ordering::Less => {}
                }
            }

            // If is EOF.
            if chunk.is_empty() {
                if let Some(expected_size) = maybe_expected_digest_size {
                    if sum_size != expected_size {
                        self.size_verification_failures.inc();
                        return Err(make_input_err!(
                            "Expected size {} but got size {} on insert",
                            expected_size,
                            sum_size
                        ));
                    }
                }
                if let Some(hasher) = maybe_hasher.as_mut() {
                    let digest = hasher.finalize_digest();
                    let hash_result = digest.packed_hash();
                    if original_hash != hash_result {
                        self.hash_verification_failures.inc();
                        return Err(make_input_err!(
                            "Hashes do not match, got: {original_hash} but digest hash was {hash_result}",
                        ));
                    }
                }
                tx.send_eof().err_tip(|| "In verify_store::check_update")?;
                break;
            }

            // This will allows us to hash while sending data to another thread.
            let write_future = tx.send(chunk.clone());

            if let Some(hasher) = maybe_hasher.as_mut() {
                hasher.update(chunk.as_ref());
            }

            write_future
                .await
                .err_tip(|| "Failed to write chunk to inner store in verify store")?;
        }
        Ok(())
    }
}

#[async_trait]
impl StoreDriver for VerifyStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner_store.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let StoreKey::Digest(digest) = key else {
            return Err(make_input_err!(
                "Only digests are supported in VerifyStore. Got {key:?}"
            ));
        };
        let digest_size = digest.size_bytes();
        if let UploadSizeInfo::ExactSize(expected_size) = size_info {
            if self.verify_size && expected_size != digest_size {
                self.size_verification_failures.inc();
                return Err(make_input_err!(
                    "Expected size to match. Got {} but digest says {} on update",
                    expected_size,
                    digest_size
                ));
            }
        }

        let mut hasher = if self.verify_hash {
            Some(
                ActiveOriginContext::get_value(&ACTIVE_HASHER_FUNC)
                    .err_tip(|| "In verify_store::update")?
                    .map_or_else(default_digest_hasher_func, |v| *v)
                    .hasher(),
            )
        } else {
            None
        };

        let maybe_digest_size = if self.verify_size {
            Some(digest_size)
        } else {
            None
        };
        let (tx, rx) = make_buf_channel_pair();

        let update_fut = self.inner_store.update(digest, rx, size_info);
        let check_fut = self.inner_check_update(
            tx,
            reader,
            maybe_digest_size,
            digest.packed_hash(),
            hasher.as_mut(),
        );

        let (update_res, check_res) = tokio::join!(update_fut, check_fut);

        update_res.merge(check_res)
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.inner_store.get_part(key, writer, offset, length).await
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn std::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send + 'static> {
        self
    }
}

default_health_status_indicator!(VerifyStore);
