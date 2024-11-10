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

use std::cmp;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bincode::config::{FixintEncoding, WithOtherIntEncoding};
use bincode::{DefaultOptions, Options};
use futures::stream::{self, FuturesOrdered, StreamExt, TryStreamExt};
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::fastcdc::FastCDC;
use nativelink_util::health_utils::{default_health_status_indicator, HealthStatusIndicator};
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo};
use serde::{Deserialize, Serialize};
use tokio_util::codec::FramedRead;
use tokio_util::io::StreamReader;
use tracing::{event, Level};

use crate::cas_utils::is_zero_digest;

// NOTE: If these change update the comments in `stores.rs` to reflect
// the new defaults.
const DEFAULT_MIN_SIZE: u64 = 64 * 1024;
const DEFAULT_NORM_SIZE: u64 = 256 * 1024;
const DEFAULT_MAX_SIZE: u64 = 512 * 1024;
const DEFAULT_MAX_CONCURRENT_FETCH_PER_GET: usize = 10;

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
pub struct DedupIndex {
    pub entries: Vec<DigestInfo>,
}

#[derive(MetricsComponent)]
pub struct DedupStore {
    #[metric(group = "index_store")]
    index_store: Store,
    #[metric(group = "content_store")]
    content_store: Store,
    fast_cdc_decoder: FastCDC,
    #[metric(help = "Maximum number of concurrent fetches per get")]
    max_concurrent_fetch_per_get: usize,
    bincode_options: WithOtherIntEncoding<DefaultOptions, FixintEncoding>,
}

impl DedupStore {
    pub fn new(
        config: &nativelink_config::stores::DedupSpec,
        index_store: Store,
        content_store: Store,
    ) -> Result<Arc<Self>, Error> {
        let min_size = if config.min_size == 0 {
            DEFAULT_MIN_SIZE
        } else {
            u64::from(config.min_size)
        };
        let normal_size = if config.normal_size == 0 {
            DEFAULT_NORM_SIZE
        } else {
            u64::from(config.normal_size)
        };
        let max_size = if config.max_size == 0 {
            DEFAULT_MAX_SIZE
        } else {
            u64::from(config.max_size)
        };
        let max_concurrent_fetch_per_get = if config.max_concurrent_fetch_per_get == 0 {
            DEFAULT_MAX_CONCURRENT_FETCH_PER_GET
        } else {
            config.max_concurrent_fetch_per_get as usize
        };
        Ok(Arc::new(Self {
            index_store,
            content_store,
            fast_cdc_decoder: FastCDC::new(
                usize::try_from(min_size).err_tip(|| "Could not convert min_size to usize")?,
                usize::try_from(normal_size)
                    .err_tip(|| "Could not convert normal_size to usize")?,
                usize::try_from(max_size).err_tip(|| "Could not convert max_size to usize")?,
            ),
            max_concurrent_fetch_per_get,
            bincode_options: DefaultOptions::new().with_fixint_encoding(),
        }))
    }

    async fn has(self: Pin<&Self>, key: StoreKey<'_>) -> Result<Option<u64>, Error> {
        // First we need to load the index that contains where the individual parts actually
        // can be fetched from.
        let index_entries = {
            let maybe_data = self
                .index_store
                .get_part_unchunked(key.borrow(), 0, None)
                .await
                .err_tip(|| "Failed to read index store in dedup store");
            let data = match maybe_data {
                Err(e) => {
                    if e.code == Code::NotFound {
                        return Ok(None);
                    }
                    return Err(e);
                }
                Ok(data) => data,
            };

            match self.bincode_options.deserialize::<DedupIndex>(&data) {
                Err(err) => {
                    event!(
                        Level::WARN,
                        ?key,
                        ?err,
                        "Failed to deserialize index in dedup store",
                    );
                    // We return the equivalent of NotFound here so the client is happy.
                    return Ok(None);
                }
                Ok(v) => v,
            }
        };

        let digests: Vec<_> = index_entries
            .entries
            .into_iter()
            .map(StoreKey::Digest)
            .collect();
        let mut sum = 0;
        for size in self.content_store.has_many(&digests).await? {
            let Some(size) = size else {
                // A part is missing so return None meaning not-found.
                // This will abort all in-flight queries related to this request.
                return Ok(None);
            };
            sum += size;
        }
        Ok(Some(sum))
    }
}

#[async_trait]
impl StoreDriver for DedupStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        digests
            .iter()
            .zip(results.iter_mut())
            .map(|(key, result)| async move {
                if is_zero_digest(key.borrow()) {
                    *result = Some(0);
                    return Ok(());
                }

                match self.has(key.borrow()).await {
                    Ok(maybe_size) => {
                        *result = maybe_size;
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            })
            .collect::<FuturesOrdered<_>>()
            .try_collect()
            .await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let mut bytes_reader = StreamReader::new(reader);
        let frame_reader = FramedRead::new(&mut bytes_reader, self.fast_cdc_decoder.clone());
        let index_entries = frame_reader
            .map(|r| r.err_tip(|| "Failed to decode frame from fast_cdc"))
            .map_ok(|frame| async move {
                let hash = blake3::hash(&frame[..]).into();
                let index_entry = DigestInfo::new(hash, frame.len() as u64);
                if self
                    .content_store
                    .has(index_entry)
                    .await
                    .err_tip(|| "Failed to call .has() in DedupStore::update()")?
                    .is_some()
                {
                    // If our store has this digest, we don't need to upload it.
                    return Result::<_, Error>::Ok(index_entry);
                }
                self.content_store
                    .update_oneshot(index_entry, frame)
                    .await
                    .err_tip(|| "Failed to update content store in dedup_store")?;
                Ok(index_entry)
            })
            .try_buffered(self.max_concurrent_fetch_per_get)
            .try_collect()
            .await?;

        let serialized_index = self
            .bincode_options
            .serialize(&DedupIndex {
                entries: index_entries,
            })
            .map_err(|e| {
                make_err!(
                    Code::Internal,
                    "Failed to serialize index in dedup_store : {:?}",
                    e
                )
            })?;

        self.index_store
            .update_oneshot(key, serialized_index.into())
            .await
            .err_tip(|| "Failed to insert our index entry to index_store in dedup_store")?;

        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // Special case for if a client tries to read zero bytes.
        if length == Some(0) {
            writer
                .send_eof()
                .err_tip(|| "Failed to write EOF out from get_part dedup")?;
            return Ok(());
        }
        // First we need to download the index that contains where the individual parts actually
        // can be fetched from.
        let index_entries = {
            let data = self
                .index_store
                .get_part_unchunked(key, 0, None)
                .await
                .err_tip(|| "Failed to read index store in dedup store")?;

            self.bincode_options
                .deserialize::<DedupIndex>(&data)
                .map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "Failed to deserialize index in dedup_store::get_part : {:?}",
                        e
                    )
                })?
        };

        let mut start_byte_in_stream: u64 = 0;
        let entries = {
            if offset == 0 && length.is_none() {
                index_entries.entries
            } else {
                let mut current_entries_sum = 0;
                let mut entries = Vec::with_capacity(index_entries.entries.len());
                for entry in index_entries.entries {
                    let first_byte = current_entries_sum;
                    let entry_size = entry.size_bytes();
                    current_entries_sum += entry_size;
                    // Filter any items who's end byte is before the first requested byte.
                    if current_entries_sum <= offset {
                        start_byte_in_stream = current_entries_sum;
                        continue;
                    }
                    // If we are not going to read any bytes past the length we are done.
                    if let Some(length) = length {
                        if first_byte >= offset + length {
                            break;
                        }
                    }
                    entries.push(entry);
                }
                entries
            }
        };

        // Second we we create a stream of futures for each chunk, but buffer/limit them so only
        // `max_concurrent_fetch_per_get` will be executed at a time.
        // The results will be streamed out in the same order they are in the entries table.
        // The results will execute in a "window-like" fashion, meaning that if we limit to
        // 5 requests at a time, and request 3 is stalled, request 1 & 2 can be output and
        // request 4 & 5 can be executing (or finished) while waiting for 3 to finish.
        // Note: We will buffer our data here up to:
        // `config.max_size * config.max_concurrent_fetch_per_get` per `get_part()` request.
        let mut entries_stream = stream::iter(entries)
            .map(move |index_entry| async move {
                let data = self
                    .content_store
                    .get_part_unchunked(index_entry, 0, None)
                    .await
                    .err_tip(|| "Failed to get_part in content_store in dedup_store")?;

                Result::<_, Error>::Ok(data)
            })
            .buffered(self.max_concurrent_fetch_per_get);

        // Stream out the buffered data one at a time and write the data to our writer stream.
        // In the event any of these error, we will abort early and abandon all the rest of the
        // streamed data.
        // Note: Need to take special care to ensure we send the proper slice of data requested.
        let mut bytes_to_skip = usize::try_from(offset - start_byte_in_stream)
            .err_tip(|| "Could not convert (offset - start_byte_in_stream) to usize")?;
        let mut bytes_to_send = usize::try_from(length.unwrap_or(u64::MAX - offset))
            .err_tip(|| "Could not convert length to usize")?;
        while let Some(result) = entries_stream.next().await {
            let mut data = result.err_tip(|| "Inner store iterator closed early in DedupStore")?;
            assert!(
                bytes_to_skip <= data.len(),
                "Formula above must be wrong, {} > {}",
                bytes_to_skip,
                data.len()
            );
            let end_pos = cmp::min(data.len(), bytes_to_send + bytes_to_skip);
            if bytes_to_skip != 0 || data.len() > bytes_to_send {
                data = data.slice(bytes_to_skip..end_pos);
            }
            writer
                .send(data)
                .await
                .err_tip(|| "Failed to write data to get_part dedup")?;
            bytes_to_send -= end_pos - bytes_to_skip;
            bytes_to_skip = 0;
        }

        // Finish our stream by writing our EOF and shutdown the stream.
        writer
            .send_eof()
            .err_tip(|| "Failed to write EOF out from get_part dedup")?;
        Ok(())
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn std::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send + 'static> {
        self
    }
}

default_health_status_indicator!(DedupStore);
