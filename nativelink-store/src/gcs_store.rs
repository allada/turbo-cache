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

use std::borrow::Cow;
use std::env;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{unfold, FuturesUnordered};
use futures::{stream, StreamExt, TryStreamExt};
// use tokio_stream::StreamExt;
use googleapis_tonic_google_storage_v2::google::storage::v2::{
    storage_client::StorageClient, write_object_request, ChecksummedData, Object,
    QueryWriteStatusRequest, ReadObjectRequest, StartResumableWriteRequest, WriteObjectRequest,
    WriteObjectSpec,
};
use nativelink_config::stores::GCSSpec;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use rand::rngs::OsRng;
use rand::Rng;
use tokio::time::{sleep, Instant};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Status};

// use tracing::{event, Level};
use crate::cas_utils::is_zero_digest;

// # How is this Different from the S3 Store Implementation
//
// The GCS store implementation differs from the S3 store implementation in several ways, reflecting
// differences in underlying APIs and service capabilities. This section provides a summary of key
// differences relevant to the **store implementation** for maintainability and reviewability:

// TODO: Add more reviewable docs comments
/*
---

### **Rationale for Implementation Differences**

The GCS store implementation adheres to the requirements and limitations of Google Cloud Storage's gRPC API.
Sequential chunk uploads, explicit session handling and checksum validation reflect the service's design.
In contrast, S3's multipart upload API simplifies concurrency, error handling, and session management.

These differences emphasize the need for tailored approaches to storage backends while maintaining a consistent abstraction layer for higher-level operations.
---
*/

// Default Buffer size for reading chunks of data in bytes.
// Note: If you change this, adjust the docs in the config.
const DEFAULT_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

/// `CredentialProvider` manages the authentication token required for GCS operations.
/// It fetches tokens dynamically and ensures they are refreshed periodically.
pub struct CredentialProvider {
    token: Arc<tokio::sync::Mutex<(String, Instant)>>,
}

impl CredentialProvider {
    async fn new() -> Result<Self, Error> {
        let token = Self::fetch_gcs_token().await?;
        let expiry = Instant::now() + Duration::from_secs(3600); // Default expiry duration
        Ok(Self {
            token: Arc::new(tokio::sync::Mutex::new((token, expiry))),
        })
    }

    /// Fetch a new token if the current token is expired.
    pub async fn get_token(&self) -> Result<String, Error> {
        let mut lock = self.token.lock().await;

        // Refresh the token if it has expired
        if Instant::now() >= lock.1 {
            lock.0 = Self::fetch_gcs_token().await?;
            lock.1 = Instant::now() + Duration::from_secs(3600);
        }
        Ok(lock.0.clone())
    }

    // /// Starts a background task to refresh the token periodically.
    // /// This ensures the token remains valid for ongoing operations.
    // pub async fn start_token_refresh(self: Arc<Self>) {
    //     nativelink_util::background_spawn(async move {
    //         loop {
    //             {
    //                 let mut lock = self.token.lock().await;
    //                 lock.0 = Self::fetch_gcs_token()
    //                     .await
    //                     .unwrap_or_else(|_| String::new());
    //                 lock.1 = Instant::now() + Duration::from_secs(3600);
    //             }
    //             sleep(Duration::from_secs(3500)).await; // Refresh before expiry
    //         }
    //     });
    // }

    pub fn get_token_sync(&self) -> String {
        // Returns the current token synchronously
        let lock = self.token.blocking_lock();
        lock.0.clone()
    }

    /// Fetches a GCS token using either an environment variable or the `gcloud` CLI.
    async fn fetch_gcs_token() -> Result<String, Error> {
        if let Ok(token) = env::var("GCS_AUTH_TOKEN") {
            Ok(token.trim().to_string())
        } else {
            Err(make_err!(Code::Unavailable, "GCS_AUTH_TOKEN not found"))
        }
    }
}

#[derive(MetricsComponent)]
pub struct GCSStore<NowFn> {
    // The gRPC client for GCS
    gcs_client: Arc<StorageClient<Channel>>,
    now_fn: NowFn,
    #[metric(help = "The bucket name for the GCS store")]
    bucket: String,
    #[metric(help = "The key prefix for the GCS store")]
    key_prefix: String,
    retrier: Retrier,
    #[metric(help = "The number of seconds to consider an object expired")]
    consider_expired_after_s: i64,
    #[metric(help = "The number of bytes to buffer for retrying requests")]
    max_retry_buffer_per_request: usize,
    #[metric(help = "The size of chunks for resumable uploads")]
    resumable_chunk_size: usize,
    #[metric(help = "The number of concurrent uploads allowed for resumable uploads")]
    resumable_max_concurrent_uploads: usize,
    credential_provider: Arc<CredentialProvider>,
}

impl<I, NowFn> GCSStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    /// Create a GCS client with an interceptor for dynamic token injection.
    pub async fn new(spec: &GCSSpec, now_fn: NowFn) -> Result<Arc<Self>, Error> {
        let jitter_amt = spec.retry.jitter;
        let jitter_fn = Arc::new(move |delay: Duration| {
            if jitter_amt == 0. {
                return delay;
            }
            let min = 1. - (jitter_amt / 2.);
            let max = 1. + (jitter_amt / 2.);
            delay.mul_f32(OsRng.gen_range(min..max))
        });

        let endpoint = env::var("GCS_ENDPOINT")
            .unwrap_or_else(|_| "https://storage.googleapis.com".to_string());
        let channel = tonic::transport::Channel::from_shared(endpoint)
            .map_err(|e| make_err!(Code::InvalidArgument, "Invalid GCS endpoint: {e:?}"))?
            .connect()
            .await
            .map_err(|e| make_err!(Code::Unavailable, "Failed to connect to GCS: {e:?}"))?;

        let credential_provider = Arc::new(CredentialProvider::new().await?);

        Self::new_with_client_and_jitter(spec, channel, credential_provider, jitter_fn, now_fn)
    }

    pub fn new_with_client_and_jitter(
        spec: &GCSSpec,
        channel: Channel,
        credential_provider: Arc<CredentialProvider>,
        jitter_fn: Arc<dyn Fn(Duration) -> Duration + Send + Sync>,
        now_fn: NowFn,
    ) -> Result<Arc<Self>, Error> {
        let gcs_client = StorageClient::new(channel);

        Ok(Arc::new(Self {
            gcs_client: Arc::new(gcs_client),
            now_fn,
            bucket: spec.bucket.to_string(),
            key_prefix: spec.key_prefix.as_ref().unwrap_or(&String::new()).clone(),
            retrier: Retrier::new(
                Arc::new(|duration| Box::pin(sleep(duration))),
                jitter_fn,
                spec.retry.clone(),
            ),
            consider_expired_after_s: i64::from(spec.consider_expired_after_s),
            max_retry_buffer_per_request: spec
                .max_retry_buffer_per_request
                .unwrap_or(DEFAULT_CHUNK_SIZE as usize),
            resumable_chunk_size: spec
                .resumable_chunk_size
                .unwrap_or(DEFAULT_CHUNK_SIZE as usize),
            resumable_max_concurrent_uploads: 0,
            credential_provider,
        }))
    }

    fn make_gcs_path(&self, key: &StoreKey<'_>) -> String {
        format!("{}{}", self.key_prefix, key.as_str())
    }

    async fn inject_auth<F>(&self, mut request: Request<F>) -> Result<Request<F>, Status> {
        let token = self
            .credential_provider
            .get_token()
            .await
            .map_err(|_| Status::unauthenticated("Failed to retrieve auth token"))?;

        let auth_header = format!("Bearer {token}");
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(auth_header).unwrap(),
        );
        Ok(request)
    }

    /// Check if the object exists and is not expired
    pub async fn has(self: Pin<&Self>, digest: &StoreKey<'_>) -> Result<Option<u64>, Error> {
        let client = Arc::clone(&self.gcs_client);

        self.retrier
            .retry(unfold((), move |state| {
                let mut client = (*client).clone();

                async move {
                    let object_path = self.make_gcs_path(digest);
                    let raw_request = ReadObjectRequest {
                        bucket: self.bucket.clone(),
                        object: object_path.clone(),
                        ..Default::default()
                    };

                    let Ok(authenticated_request) =
                        self.inject_auth(Request::new(raw_request)).await
                    else {
                        return Some((
                            RetryResult::Err(make_err!(Code::Unauthenticated, "Auth failed")),
                            state,
                        ));
                    };

                    let result = client.read_object(authenticated_request).await;

                    match result {
                        Ok(response) => {
                            let mut response_stream = response.into_inner();

                            // The first message contains the metadata
                            if let Some(Ok(first_message)) = response_stream.next().await {
                                if let Some(metadata) = first_message.metadata {
                                    if self.consider_expired_after_s != 0 {
                                        if let Some(last_modified) = metadata.update_time {
                                            let now_s = (self.now_fn)().unix_timestamp() as i64;
                                            if last_modified.seconds + self.consider_expired_after_s
                                                <= now_s
                                            {
                                                return Some((RetryResult::Ok(None), state));
                                            }
                                        }
                                    }
                                    let length = metadata.size as u64;
                                    return Some((RetryResult::Ok(Some(length)), state));
                                }
                            }
                            Some((RetryResult::Ok(None), state))
                        }
                        Err(status) => match status.code() {
                            tonic::Code::NotFound => Some((RetryResult::Ok(None), state)),
                            _ => Some((
                                RetryResult::Retry(make_err!(
                                    Code::Unavailable,
                                    "Unhandled ReadObject error: {status:?}"
                                )),
                                state,
                            )),
                        },
                    }
                }
            }))
            .await
    }
}

#[async_trait]
impl<I, NowFn> StoreDriver for GCSStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        keys.iter()
            .zip(results.iter_mut())
            .map(|(key, result)| async move {
                // Check for zero digest as a special case.
                if is_zero_digest(key.borrow()) {
                    *result = Some(0);
                    return Ok::<_, Error>(());
                }
                *result = self.has(key).await?;
                Ok::<_, Error>(())
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect()
            .await
    }

    async fn update(
        self: Pin<&Self>,
        digest: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let gcs_path = self.make_gcs_path(&digest.borrow());

        let max_size = match upload_size {
            UploadSizeInfo::ExactSize(sz) | UploadSizeInfo::MaxSize(sz) => sz,
        };

        // If size is below chunk threshold and is known, use a simple upload
        // Single-chunk upload for small files
        if max_size < DEFAULT_CHUNK_SIZE && matches!(upload_size, UploadSizeInfo::ExactSize(_)) {
            let UploadSizeInfo::ExactSize(sz) = upload_size else {
                unreachable!("upload_size must be UploadSizeInfo::ExactSize here");
            };
            reader.set_max_recent_data_size(
                u64::try_from(self.max_retry_buffer_per_request)
                    .err_tip(|| "Could not convert max_retry_buffer_per_request to u64")?,
            );

            // Read all data and upload in one request
            let data = reader
                .consume(Some(sz as usize))
                .await
                .err_tip(|| "Failed to read data for single upload")?;

            return self
                .retrier
                .retry(unfold((), move |()| {
                    let client = Arc::clone(&self.gcs_client);
                    let mut client = (*client).clone();
                    let gcs_path = gcs_path.clone();
                    let data = data.clone();

                    async move {
                        let write_spec = WriteObjectSpec {
                            resource: Some(Object {
                                name: gcs_path.clone(),
                                ..Default::default()
                            }),
                            object_size: Some(sz as i64),
                            ..Default::default()
                        };

                        let request_stream = stream::iter(vec![WriteObjectRequest {
                            first_message: Some(
                                write_object_request::FirstMessage::WriteObjectSpec(write_spec),
                            ),
                            data: Some(write_object_request::Data::ChecksummedData(
                                ChecksummedData {
                                    content: data.to_vec(),
                                    crc32c: Some(crc32c::crc32c(&data)),
                                },
                            )),
                            finish_write: true,
                            ..Default::default()
                        }]);

                        let result = client
                            .write_object(request_stream)
                            .await
                            .map_err(|e| make_err!(Code::Aborted, "WriteObject failed: {e:?}"));

                        match result {
                            Ok(_) => Some((RetryResult::Ok(()), ())),
                            Err(e) => Some((RetryResult::Retry(e), ())),
                        }
                    }
                }))
                .await;
        }

        // Start a resumable write session for larger files
        let upload_id = self
            .retrier
            .retry(unfold((), move |()| {
                let client = Arc::clone(&self.gcs_client);
                let mut client = (*client).clone();
                let gcs_path = gcs_path.clone();
                async move {
                    let write_spec = WriteObjectSpec {
                        resource: Some(Object {
                            name: gcs_path.clone(),
                            ..Default::default()
                        }),
                        object_size: Some(max_size as i64),
                        ..Default::default()
                    };

                    let request = StartResumableWriteRequest {
                        write_object_spec: Some(write_spec),
                        ..Default::default()
                    };

                    let result = client.start_resumable_write(request).await.map_err(|e| {
                        make_err!(Code::Unavailable, "Failed to start resumable upload: {e:?}")
                    });

                    match result {
                        Ok(response) => {
                            Some((RetryResult::Ok(response.into_inner().upload_id), ()))
                        }
                        Err(e) => Some((RetryResult::Retry(e), ())),
                    }
                }
            }))
            .await?;

        // Chunked upload loop
        let mut offset = 0;
        let chunk_size = self.resumable_chunk_size;
        let upload_id = Arc::new(upload_id);

        while offset < max_size {
            let data = reader
                .consume(Some(chunk_size))
                .await
                .err_tip(|| "Failed to read data for chunked upload")?;

            let is_last_chunk = offset + chunk_size as u64 >= max_size;

            let upload_id = Arc::clone(&upload_id);

            self.retrier
                .retry(unfold(data, move |data| {
                    let client = Arc::clone(&self.gcs_client);
                    let mut client = (*client).clone();
                    let upload_id = Arc::clone(&upload_id);
                    let data = data.clone();
                    let offset = offset;

                    async move {
                        let request_stream = stream::iter(vec![WriteObjectRequest {
                            first_message: Some(write_object_request::FirstMessage::UploadId(
                                (*upload_id).clone(),
                            )),
                            write_offset: offset as i64,
                            finish_write: is_last_chunk,
                            data: Some(write_object_request::Data::ChecksummedData(
                                ChecksummedData {
                                    content: data.to_vec(),
                                    crc32c: Some(crc32c::crc32c(&data)),
                                },
                            )),
                            ..Default::default()
                        }]);

                        let result = client
                            .write_object(request_stream)
                            .await
                            .map_err(|e| make_err!(Code::Aborted, "Failed to upload chunk: {e:?}"));

                        match result {
                            Ok(_) => Some((RetryResult::Ok(()), data)),
                            Err(e) => Some((RetryResult::Retry(e), data)),
                        }
                    }
                }))
                .await?;

            offset += chunk_size as u64;
        }

        // Finalize the upload
        self.retrier
            .retry(unfold((), move |()| {
                let client = Arc::clone(&self.gcs_client);
                let mut client = (*client).clone();
                let upload_id = Arc::clone(&upload_id);
                async move {
                    let request = QueryWriteStatusRequest {
                        upload_id: (*upload_id).clone(),
                        ..Default::default()
                    };

                    let result = client.query_write_status(request).await.map_err(|e| {
                        make_err!(Code::Unavailable, "Failed to finalize upload: {e:?}")
                    });

                    match result {
                        Ok(_) => Some((RetryResult::Ok(()), ())),
                        Err(e) => Some((RetryResult::Retry(e), ())),
                    }
                }
            }))
            .await?;
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        if is_zero_digest(key.borrow()) {
            writer
                .send_eof()
                .err_tip(|| "Failed to send zero EOF in GCS store get_part")?;
            return Ok(());
        }

        let gcs_path = self.make_gcs_path(&key);

        self.retrier
            .retry(unfold(writer, move |writer| {
                let path = gcs_path.clone();
                async move {
                    let request = ReadObjectRequest {
                        bucket: self.bucket.clone(),
                        object: path.clone(),
                        read_offset: offset as i64,
                        read_limit: length.unwrap_or(0) as i64,
                        ..Default::default()
                    };

                    let client = Arc::clone(&self.gcs_client);
                    let mut cloned_client = (*client).clone();

                    let result = cloned_client.read_object(request).await;

                    let mut response_stream = match result {
                        Ok(response) => response.into_inner(),
                        Err(status) if status.code() == tonic::Code::NotFound => {
                            return Some((
                                RetryResult::Err(make_err!(
                                    Code::NotFound,
                                    "GCS object not found: {path}"
                                )),
                                writer,
                            ));
                        }
                        Err(e) => {
                            return Some((
                                RetryResult::Retry(make_err!(
                                    Code::Unavailable,
                                    "Failed to initiate read for GCS object: {e:?}"
                                )),
                                writer,
                            ));
                        }
                    };

                    // Stream data from the GCS response to the writer
                    while let Some(chunk) = response_stream.next().await {
                        match chunk {
                            Ok(data) => {
                                if let Some(checksummed_data) = data.checksummed_data {
                                    if checksummed_data.content.is_empty() {
                                        // Ignore empty chunks
                                        continue;
                                    }
                                    if let Err(e) =
                                        writer.send(checksummed_data.content.into()).await
                                    {
                                        return Some((
                                            RetryResult::Err(make_err!(
                                                Code::Aborted,
                                                "Failed to send bytes to writer in GCS: {e:?}"
                                            )),
                                            writer,
                                        ));
                                    }
                                }
                            }
                            Err(e) => {
                                return Some((
                                    RetryResult::Retry(make_err!(
                                        Code::Aborted,
                                        "Error in GCS response stream: {e:?}"
                                    )),
                                    writer,
                                ));
                            }
                        }
                    }

                    if let Err(e) = writer.send_eof() {
                        return Some((
                            RetryResult::Err(make_err!(
                                Code::Aborted,
                                "Failed to send EOF to writer in GCS: {e:?}"
                            )),
                            writer,
                        ));
                    }

                    Some((RetryResult::Ok(()), writer))
                }
            }))
            .await
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

#[async_trait]
impl<I, NowFn> HealthStatusIndicator for GCSStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    fn get_name(&self) -> &'static str {
        "GCSStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}
