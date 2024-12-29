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
use std::cmp;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use azure_core::auth::Secret;
use azure_core::prelude::Range;
use azure_core::{Body, HttpClient, StatusCode, TransportOptions};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures::future::FusedFuture;
use futures::stream::{unfold, FuturesUnordered};
use futures::{FutureExt, StreamExt, TryStreamExt};
use http_body::{Frame, SizeHint};
use hyper::client::connect::{Connected, Connection};
use hyper::client::HttpConnector;
use hyper::service::Service;
use hyper::{Body as HyperBody, Client, Request as HyperRequest, Uri};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder, MaybeHttpsStream};
use nativelink_config::stores::AzureBlobSpec;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{
    make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf,
};
use nativelink_util::fs;
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use rand::rngs::OsRng;
use rand::Rng;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, SemaphorePermit};
use tokio::time::sleep;
use tokio_stream::Stream;
use tracing::{event, Level};

use crate::cas_utils::is_zero_digest;

// Check the below doc for the limits specific to Azure.
// https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage

// Maximum number of blocks in a block blob or append blob
const MAX_BLOCKS: usize = 50_000;

// Maximum size of a block in a block blob (4,000 MiB)
const MAX_BLOCK_SIZE: u64 = 4_000 * 1024 * 1024; // 4,000 MiB = 4 GiB

// Default block size for uploads (4 MiB)
const DEFAULT_BLOCK_SIZE: u64 = 5 * 1024 * 1024; // 5 MiB

// Default maximum retry buffer per request
const DEFAULT_MAX_RETRY_BUFFER_PER_REQUEST: usize = 5 * 1024 * 1024; // 5 MiB

// Default maximum number of concurrent uploads
const DEFAULT_MAX_CONCURRENT_UPLOADS: usize = 10;

// Default idle timeout
const IDLE_TIMEOUT: Duration = Duration::from_secs(15);

// Maximum number of idle connections per host
const MAX_IDLE_PER_HOST: usize = 32;

pub struct AzureConnectionWithPermit<T: Connection + AsyncRead + AsyncWrite + Unpin> {
    connection: T,
    _permit: SemaphorePermit<'static>,
}

impl<T: Connection + AsyncRead + AsyncWrite + Unpin> Connection for AzureConnectionWithPermit<T> {
    fn connected(&self) -> Connected {
        self.connection.connected()
    }
}

impl<T: Connection + AsyncRead + AsyncWrite + Unpin> AsyncRead for AzureConnectionWithPermit<T> {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), tokio::io::Error>> {
        Pin::new(&mut Pin::get_mut(self).connection).poll_read(cx, buf)
    }
}

impl<T: Connection + AsyncWrite + AsyncRead + Unpin> AsyncWrite for AzureConnectionWithPermit<T> {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, tokio::io::Error>> {
        Pin::new(&mut Pin::get_mut(self).connection).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), tokio::io::Error>> {
        Pin::new(&mut Pin::get_mut(self).connection).poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), tokio::io::Error>> {
        Pin::new(&mut Pin::get_mut(self).connection).poll_shutdown(cx)
    }
}

// Created an AzureClient to handle the custom HttpsConnector.
// Unlike Amazon S3, Azure Blob Storage doesn't support
// specifying a custom http client in the Transport Options.
// So, we have specialized it to use a custom HttpsConnector.
#[derive(Clone)]
pub struct AzureClient {
    client: Client<HttpsConnector<HttpConnector>>,
    config: Arc<AzureBlobSpec>,
    retrier: Retrier,
    connector: HttpsConnector<HttpConnector>,
}

impl AzureClient {
    pub fn new(
        config: AzureBlobSpec,
        jitter_fn: Arc<dyn Fn(Duration) -> Duration + Send + Sync>,
    ) -> Result<Self, Error> {
        let connector = Self::build_connector(&config);
        let client = Self::build_client(connector.clone());

        Ok(Self {
            client,
            retrier: Retrier::new(
                Arc::new(|duration| Box::pin(sleep(duration))),
                jitter_fn,
                config.retry.clone(),
            ),
            config: Arc::new(config),
            connector,
        })
    }

    pub fn get_connector(&self) -> &HttpsConnector<HttpConnector> {
        &self.connector
    }

    fn build_connector(config: &AzureBlobSpec) -> HttpsConnector<HttpConnector> {
        let builder = HttpsConnectorBuilder::new().with_webpki_roots();

        let builder_with_schemes = if config.insecure_allow_http {
            builder.https_or_http()
        } else {
            builder.https_only()
        };

        if config.disable_http2 {
            builder_with_schemes.enable_http1().build()
        } else {
            builder_with_schemes.enable_http1().enable_http2().build()
        }
    }

    fn build_client(
        connector: HttpsConnector<HttpConnector>,
    ) -> Client<HttpsConnector<HttpConnector>> {
        Client::builder()
            .pool_idle_timeout(IDLE_TIMEOUT)
            .pool_max_idle_per_host(MAX_IDLE_PER_HOST)
            .build(connector)
    }

    async fn call_with_retry(
        &self,
        req: &Uri,
    ) -> Result<AzureConnectionWithPermit<MaybeHttpsStream<TcpStream>>, Error> {
        let retry_stream_fn = unfold(self.connector.clone(), move |mut client| async move {
            let _permit = fs::get_permit().await.unwrap();
            match client.call(req.clone()).await {
                Ok(connection) => Some((
                    RetryResult::Ok(AzureConnectionWithPermit {
                        connection,
                        _permit,
                    }),
                    client,
                )),
                Err(e) => Some((
                    RetryResult::Retry(make_err!(
                        Code::Unavailable,
                        "Failed to call Azure Blob Storage: {e:?}"
                    )),
                    client,
                )),
            }
        });
        self.retrier.retry(retry_stream_fn).await
    }
}

impl Debug for AzureClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureClient")
            .field("config", &self.config)
            .finish()
    }
}

#[async_trait::async_trait]
impl HttpClient for AzureClient {
    async fn execute_request(
        &self,
        request: &azure_core::Request,
    ) -> azure_core::Result<azure_core::Response> {
        let mut builder = HyperRequest::builder()
            .method(request.method().as_ref())
            .uri(request.url().as_str());

        // Copy headers from the original request
        for (name, value) in request.headers().iter() {
            builder = builder.header(name.as_str(), value.as_str());
        }

        // Copy the body
        let body = match request.body() {
            Body::Bytes(bytes) if bytes.is_empty() => HyperBody::empty(),
            Body::Bytes(bytes) => HyperBody::from(bytes.to_vec()),
            Body::SeekableStream(_) => {
                return Err(azure_core::Error::new(
                    azure_core::error::ErrorKind::Other,
                    "Unsupported body type: SeekableStream",
                ))
            }
        };

        let hyper_request = builder
            .body(body)
            .map_err(|e| azure_core::Error::new(azure_core::error::ErrorKind::Other, e))?;

        let response = tokio::time::timeout(IDLE_TIMEOUT, self.client.request(hyper_request))
            .await
            .map_err(|_| {
                azure_core::Error::new(azure_core::error::ErrorKind::Other, "Request timeout")
            })?
            .map_err(|e| azure_core::Error::new(azure_core::error::ErrorKind::Other, e))?;

        let (parts, body) = response.into_parts();

        let mapped_stream = body.map(|result| {
            result.map_err(|e| azure_core::Error::new(azure_core::error::ErrorKind::Other, e))
        });

        // Copy headers back to the original request
        let headers: HashMap<_, _> = parts
            .headers
            .iter()
            .filter_map(|(k, v)| {
                Some((
                    azure_core::headers::HeaderName::from(k.as_str().to_owned()),
                    azure_core::headers::HeaderValue::from(v.to_str().ok()?.to_owned()),
                ))
            })
            .collect();

        Ok(azure_core::Response::new(
            azure_core::StatusCode::try_from(parts.status.as_u16()).expect("Invalid status code"),
            azure_core::headers::Headers::from(headers),
            Box::pin(mapped_stream),
        ))
    }
}

impl Service<Uri> for AzureClient {
    type Response = AzureConnectionWithPermit<MaybeHttpsStream<TcpStream>>;
    type Error = Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.client
            .poll_ready(cx)
            .map_err(|e| make_err!(Code::Unavailable, "Failed poll in Azure Blob Storage: {e}"))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let client_clone = self.clone();
        Box::pin(async move { client_clone.call_with_retry(&req).await })
    }
}

pub struct AzureBodyWrapper {
    reader: DropCloserReadHalf,
    size: u64,
}

impl http_body::Body for AzureBodyWrapper {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let reader = Pin::new(&mut Pin::get_mut(self).reader);
        reader
            .poll_next(cx)
            .map(|maybe_bytes_res| maybe_bytes_res.map(|res| res.map(Frame::data)))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.size)
    }
}

#[derive(MetricsComponent)]
pub struct AzureBlobStore<NowFn> {
    client: Arc<ContainerClient>,
    now_fn: NowFn,
    #[metric(help = "The container name for the Azure store")]
    container: String,
    #[metric(help = "The blob prefix for the Azure store")]
    blob_prefix: String,
    retrier: Retrier,
    #[metric(help = "The number of seconds to consider an object expired")]
    consider_expired_after_s: i64,
    #[metric(help = "The number of bytes to buffer for retrying requests")]
    max_retry_buffer_per_request: usize,
    #[metric(help = "The number of concurrent uploads allowed")]
    max_concurrent_uploads: usize,
}

impl<I, NowFn> AzureBlobStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    pub async fn new(spec: &AzureBlobSpec, now_fn: NowFn) -> Result<Arc<Self>, Error> {
        // TODO: Should we use a common jitter function to minimize duplication?
        let jitter_amt = spec.retry.jitter;
        let jitter_fn = Arc::new(move |delay: Duration| {
            if jitter_amt == 0. {
                return delay;
            }
            let min = 1. - (jitter_amt / 2.);
            let max = 1. + (jitter_amt / 2.);
            delay.mul_f32(OsRng.gen_range(min..max))
        });

        let http_client = Arc::new(
            AzureClient::new(spec.clone(), jitter_fn.clone())
                .map_err(|e| make_err!(Code::Unavailable, "Failed to create Azure client: {e}"))?,
        );
        let transport_options = TransportOptions::new(http_client);

        let storage_credentials = StorageCredentials::access_key(
            spec.account_name.clone(),
            Secret::new(spec.account_key.clone()),
        );

        let container_client = BlobServiceClient::builder(&spec.account_name, storage_credentials)
            .transport(transport_options)
            .container_client(&spec.container);

        Self::new_with_client_and_jitter(spec, container_client, jitter_fn, now_fn)
    }

    pub fn new_with_client_and_jitter(
        spec: &AzureBlobSpec,
        client: ContainerClient,
        jitter_fn: Arc<dyn Fn(Duration) -> Duration + Send + Sync>,
        now_fn: NowFn,
    ) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self {
            client: Arc::new(client),
            now_fn,
            container: spec.container.to_string(),
            blob_prefix: spec.blob_prefix.as_ref().unwrap_or(&String::new()).clone(),
            retrier: Retrier::new(
                Arc::new(|duration| Box::pin(sleep(duration))),
                jitter_fn,
                spec.retry.clone(),
            ),
            consider_expired_after_s: i64::from(spec.consider_expired_after_s),
            max_retry_buffer_per_request: spec
                .max_retry_buffer_per_request
                .unwrap_or(DEFAULT_MAX_RETRY_BUFFER_PER_REQUEST),
            max_concurrent_uploads: spec
                .max_concurrent_uploads
                .map_or(DEFAULT_MAX_CONCURRENT_UPLOADS, |v| v),
        }))
    }

    fn make_blob_path(&self, key: &StoreKey<'_>) -> String {
        format!("{}{}", self.blob_prefix, key.as_str())
    }

    async fn has(self: Pin<&Self>, digest: &StoreKey<'_>) -> Result<Option<u64>, Error> {
        let blob_path = self.make_blob_path(digest);

        self.retrier
            .retry(unfold((), move |state| {
                let blob_path = blob_path.clone();
                async move {
                    let result = self.client.blob_client(&blob_path).get_properties().await;

                    match result {
                        Ok(props) => {
                            if self.consider_expired_after_s > 0 {
                                let last_modified = props.blob.properties.last_modified;
                                let now = (self.now_fn)().unix_timestamp() as i64;
                                if last_modified.unix_timestamp() + self.consider_expired_after_s
                                    <= now
                                {
                                    return Some((RetryResult::Ok(None), state));
                                }
                            }
                            let blob_size = props.blob.properties.content_length;
                            Some((RetryResult::Ok(Some(blob_size)), state))
                        }
                        Err(err) => {
                            if err
                                .as_http_error()
                                .is_some_and(|e| e.status() == StatusCode::NotFound)
                            {
                                Some((RetryResult::Ok(None), state))
                            } else if err.to_string().contains("ContainerNotFound") {
                                Some((
                                    RetryResult::Err(make_err!(
                                        Code::InvalidArgument,
                                        "Container not found: {}",
                                        err
                                    )),
                                    state,
                                ))
                            } else {
                                Some((
                                    RetryResult::Retry(make_err!(
                                        Code::Unavailable,
                                        "Failed to get blob properties: {:?}",
                                        err
                                    )),
                                    state,
                                ))
                            }
                        }
                    }
                }
            }))
            .await
    }
}

#[async_trait]
impl<I, NowFn> StoreDriver for AzureBlobStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    // TODO(Aman): This is also duplicated. Should we have a trait or a macro for this?
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        keys.iter()
            .zip(results.iter_mut())
            .map(|(key, result)| async move {
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
        let blob_path = self.make_blob_path(&digest);
        // Handling zero-sized content check
        if let UploadSizeInfo::ExactSize(0) = upload_size {
            return Ok(());
        }

        let max_size = match upload_size {
            UploadSizeInfo::ExactSize(sz) | UploadSizeInfo::MaxSize(sz) => sz,
        };

        // For small files, using single block upload
        if max_size < DEFAULT_BLOCK_SIZE && matches!(upload_size, UploadSizeInfo::ExactSize(_)) {
            let UploadSizeInfo::ExactSize(sz) = upload_size else {
                unreachable!("upload_size must be UploadSizeInfo::ExactSize here");
            };

            reader.set_max_recent_data_size(
                u64::try_from(self.max_retry_buffer_per_request)
                    .err_tip(|| "Could not convert max_retry_buffer_per_request to u64")?,
            );

            return self.retrier
                .retry(unfold(reader, move |mut reader| {
                    let client = Arc::clone(&self.client);
                    let blob_client = client.blob_client(&blob_path);
                    async move {
                        let (mut tx, mut rx) = make_buf_channel_pair();

                        let result = {
                            let reader_ref = &mut reader;
                            let (upload_res, bind_res) = tokio::join!(
                            async {
                                let mut buffer = Vec::with_capacity(sz as usize);
                                while let Ok(Some(chunk)) = rx.try_next().await {
                                    buffer.extend_from_slice(&chunk);
                                }

                                blob_client
                                    .put_block_blob(Body::from(buffer))
                                    .content_type("application/octet-stream")
                                    .into_future()
                                    .await
                                    .map(|_| ())
                                    .map_err(|e| make_err!(Code::Aborted, "{:?}", e))
                            },
                            async {
                                tx.bind_buffered(reader_ref).await
                            }
                        );

                            upload_res
                                .and(bind_res)
                                .err_tip(|| "Failed to upload blob in single chunk")
                        };

                        match result {
                            Ok(()) => Some((RetryResult::Ok(()), reader)),
                            Err(mut err) => {
                                err.code = Code::Aborted;
                                let bytes_received = reader.get_bytes_received();

                                if let Err(try_reset_err) = reader.try_reset_stream() {
                                    event!(
                                    Level::ERROR,
                                    ?bytes_received,
                                    err = ?try_reset_err,
                                    "Unable to reset stream after failed upload in AzureStore::update"
                                );
                                    Some((RetryResult::Err(err
                                        .merge(try_reset_err)
                                        .append(format!("Failed to retry upload with {bytes_received} bytes received in AzureStore::update"))),
                                          reader))
                                } else {
                                    let err = err.append(format!("Retry on upload happened with {bytes_received} bytes received in AzureStore::update"));
                                    event!(
                                    Level::INFO,
                                    ?err,
                                    ?bytes_received,
                                    "Retryable Azure error"
                                );
                                    Some((RetryResult::Retry(err), reader))
                                }
                            }
                        }
                    }
                }))
                .await;
        }

        // For larger files, we'll use block upload strategy
        let block_size =
            cmp::min(max_size / (MAX_BLOCKS as u64 - 1), MAX_BLOCK_SIZE).max(DEFAULT_BLOCK_SIZE);

        let (tx, mut rx) = mpsc::channel(self.max_concurrent_uploads);
        let mut block_ids = Vec::with_capacity(MAX_BLOCKS);
        let retrier = self.retrier.clone();

        let read_stream_fut = {
            let tx = tx.clone();
            let blob_path = blob_path.clone();
            async move {
                for block_id in 0..MAX_BLOCKS {
                    let write_buf = reader
                        .consume(Some(
                            usize::try_from(block_size)
                                .err_tip(|| "Could not convert block_size to usize")?,
                        ))
                        .await
                        .err_tip(|| "Failed to read chunk in azure_store")?;

                    if write_buf.is_empty() {
                        break;
                    }

                    let block_id = format!("{block_id:032}");
                    let blob_path = blob_path.clone();

                    tx.send(async move {
                        self.retrier
                            .retry(unfold(
                                (write_buf, block_id.clone()),
                                move |(write_buf, block_id)| {
                                    let client = Arc::clone(&self.client);
                                    let blob_client = client.blob_client(&blob_path);
                                    async move {
                                        let retry_result = blob_client
                                            .put_block(
                                                block_id.clone(),
                                                Body::from(write_buf.clone()),
                                            )
                                            .into_future()
                                            .await
                                            .map_or_else(
                                                |e| {
                                                    RetryResult::Retry(make_err!(
                                            Code::Aborted,
                                            "Failed to upload block {} in Azure store: {:?}",
                                            block_id,
                                            e
                                        ))
                                                },
                                                |_| RetryResult::Ok(block_id.clone()),
                                            );
                                        Some((retry_result, (write_buf, block_id)))
                                    }
                                },
                            ))
                            .await
                    })
                    .await
                    .map_err(|_| make_err!(Code::Internal, "Failed to send block to channel"))?;
                }
                Ok::<_, Error>(())
            }
            .fuse()
        };

        let mut upload_futures = FuturesUnordered::new();

        tokio::pin!(read_stream_fut);

        loop {
            if read_stream_fut.is_terminated() && rx.is_empty() && upload_futures.is_empty() {
                break;
            }
            tokio::select! {
                result = &mut read_stream_fut => result?,
                Some(block_id) = upload_futures.next() => block_ids.push(block_id?),
                Some(fut) = rx.recv() => upload_futures.push(fut),
            }
        }

        // Sorting block IDs to ensure consistent ordering
        block_ids.sort_unstable();

        // Commit the block list
        let block_list = BlockList {
            blocks: block_ids
                .into_iter()
                .map(|id| BlobBlockType::Latest(BlockId::from(id)))
                .collect(),
        };

        retrier
            .retry(unfold(block_list, move |block_list| {
                let client = Arc::clone(&self.client);
                let blob_client = client.blob_client(&blob_path);

                async move {
                    Some((
                        blob_client
                            .put_block_list(block_list.clone())
                            .content_type("application/octet-stream")
                            .into_future()
                            .await
                            .map_or_else(
                                |e| {
                                    RetryResult::Retry(make_err!(
                                        Code::Aborted,
                                        "Failed to commit block list in Azure store: {e:?}"
                                    ))
                                },
                                |_| RetryResult::Ok(()),
                            ),
                        block_list,
                    ))
                }
            }))
            .await
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
                .err_tip(|| "Failed to send zero EOF in azure store get_part")?;
            return Ok(());
        }

        let blob_path = self.make_blob_path(&key);

        let client = Arc::clone(&self.client);
        let blob_client = client.blob_client(&blob_path);
        let range = match length {
            Some(len) => Range::new(offset, offset + len - 1),
            None => Range::from(offset..),
        };

        self.retrier
            .retry(unfold(writer, move |writer| {
                let range_clone = range.clone();
                let blob_client = blob_client.clone();
                async move {
                    let result = async {
                        let mut stream = blob_client.get().range(range_clone.clone()).into_stream();

                        while let Some(chunk_result) = stream.next().await {
                            match chunk_result {
                                Ok(response) => {
                                    let data = response.data.collect().await.map_err(|e| {
                                        make_err!(
                                            Code::Aborted,
                                            "Failed to collect response data: {:?}",
                                            e
                                        )
                                    })?;
                                    if data.is_empty() {
                                        continue;
                                    }
                                    writer.send(data).await.map_err(|e| {
                                        make_err!(
                                            Code::Aborted,
                                            "Failed to send data to writer: {:?}",
                                            e
                                        )
                                    })?;
                                }
                                Err(e) => {
                                    return match e {
                                        e if e
                                            .as_http_error()
                                            .map(|e| e.status() == StatusCode::NotFound)
                                            .unwrap_or_default() =>
                                        {
                                            Err(make_err!(
                                                Code::NotFound,
                                                "Blob not found in Azure: {:?}",
                                                e
                                            ))
                                        }
                                        _ => Err(make_err!(
                                            Code::Aborted,
                                            "Error reading from Azure stream: {:?}",
                                            e
                                        )),
                                    };
                                }
                            }
                        }

                        writer.send_eof().map_err(|e| {
                            make_err!(Code::Aborted, "Failed to send EOF to writer: {:?}", e)
                        })?;
                        Ok(())
                    }
                    .await;

                    match result {
                        Ok(()) => Some((RetryResult::Ok(()), writer)),
                        Err(e) => {
                            if e.code == Code::NotFound {
                                Some((RetryResult::Err(e), writer))
                            } else {
                                Some((RetryResult::Retry(e), writer))
                            }
                        }
                    }
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
impl<I, NowFn> HealthStatusIndicator for AzureBlobStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    fn get_name(&self) -> &'static str {
        "AzureBlobStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}
