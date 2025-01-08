use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use azure_core::{base64, HttpClient, StatusCode, TransportOptions};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use base64::encode;
use bytes::{BufMut, BytesMut};
use nativelink_config::stores::AzureBlobSpec;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::azure_blob_store::AzureBlobStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::origin_context::OriginContext;
use nativelink_util::store_trait::{StoreKey, StoreLike, UploadSizeInfo};
use sha2::{Digest, Sha256};

// Test constants
const TEST_CONTAINER: &str = "test-container";
const TEST_ACCOUNT: &str = "testaccount";
const TEST_KEY: &str = "dGVzdGtleQ=="; // base64 encoded "testkey"
const TEST_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const TEST_SIZE: u64 = 100;
type AzureBlobStoreTest = Arc<AzureBlobStore<fn() -> MockInstantWrapped>>;

/// Test utilities
mod test_utils {
    use super::*;

    #[derive(Debug)]
    pub struct TestResponse {
        pub status: StatusCode,
        pub headers: Vec<(&'static str, String)>,
        pub body: Vec<u8>,
    }

    impl TestResponse {
        pub fn ok() -> Self {
            Self {
                status: StatusCode::Ok,
                // A sample set of headers.
                headers: vec![
                    ("content-length", "0".to_string()),
                    ("last-modified", "Thu, 01 Jan 1970 00:00:00 GMT".to_string()),
                    ("etag", "\"test-etag\"".to_string()),
                    (
                        "x-ms-creation-time",
                        "Thu, 01 Jan 1970 00:00:00 GMT".to_string(),
                    ),
                    ("x-ms-lease-status", "unlocked".to_string()),
                    ("x-ms-lease-state", "available".to_string()),
                    ("x-ms-blob-type", "BlockBlob".to_string()),
                    ("x-ms-server-encrypted", "true".to_string()),
                    ("x-ms-request-server-encrypted", "true".to_string()),
                    ("x-ms-blob-committed-block-count", "0".to_string()),
                    ("x-ms-blob-content-encoding", "utf-8".to_string()),
                    (
                        "x-ms-request-id",
                        "00000000-0000-0000-0000-000000000000".to_string(),
                    ),
                    ("x-ms-version", "2020-04-08".to_string()),
                    ("date", "Thu, 01 Jan 1970 00:00:00 GMT".to_string()),
                ],
                body: vec![],
            }
        }

        pub fn error(status: StatusCode) -> Self {
            Self {
                status,
                headers: vec![],
                body: vec![],
            }
        }

        pub fn not_found() -> Self {
            Self::error(StatusCode::NotFound)
        }

        pub fn with_content_length(mut self, length: usize) -> Self {
            if let Some(header) = self
                .headers
                .iter_mut()
                .find(|(key, _)| *key == "content-length")
            {
                header.1 = length.to_string();
            } else {
                self.headers.push(("content-length", length.to_string()));
            }
            self
        }

        pub fn with_body(mut self, body: Vec<u8>) -> Self {
            self.body = body;
            self
        }
    }

    #[derive(Debug)]
    pub struct TestRequest {
        pub method: &'static str,
        pub url_pattern: String,
        pub response: TestResponse,
    }

    impl TestRequest {
        pub fn new(method: &'static str, url_pattern: String, response: TestResponse) -> Self {
            Self {
                method,
                url_pattern,
                response,
            }
        }

        pub fn head(digest: &str, size: u64, response: TestResponse) -> Self {
            Self::new(
                "HEAD",
                format!("{TEST_CONTAINER}/{digest}-{size}"),
                response,
            )
        }

        pub fn get(digest: &str, size: u64, response: TestResponse) -> Self {
            Self::new("GET", format!("{TEST_CONTAINER}/{digest}-{size}"), response)
        }

        pub fn put(digest: &str, size: u64, response: TestResponse) -> Self {
            Self::new("PUT", format!("{TEST_CONTAINER}/{digest}-{size}"), response)
        }
    }

    #[derive(Clone)]
    pub struct MockAzureClient {
        expected_requests: Arc<Vec<TestRequest>>,
        current_request: Arc<AtomicUsize>,
        request_log: Arc<Mutex<Vec<azure_core::Request>>>,
    }

    impl Debug for MockAzureClient {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockAzureClient")
                .field("current_request", &self.current_request)
                .field("request_count", &self.request_log.lock().unwrap().len())
                .finish()
        }
    }

    impl MockAzureClient {
        pub fn new(requests: Vec<TestRequest>) -> Self {
            Self {
                expected_requests: Arc::new(requests),
                current_request: Arc::new(AtomicUsize::new(0)),
                request_log: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn request_count(&self) -> usize {
            self.request_log.lock().unwrap().len()
        }

        pub fn single_head_request(response: TestResponse) -> Self {
            Self::new(vec![TestRequest::head(TEST_HASH, TEST_SIZE, response)])
        }
    }

    #[async_trait::async_trait]
    impl HttpClient for MockAzureClient {
        async fn execute_request(
            &self,
            request: &azure_core::Request,
        ) -> azure_core::Result<azure_core::Response> {
            self.request_log.lock().unwrap().push(request.clone());

            let index = self.current_request.fetch_add(1, Ordering::SeqCst);
            let expected = &self.expected_requests[index];

            if request.method().to_string() != expected.method
                || !request.url().as_str().contains(&expected.url_pattern)
            {
                return Err(azure_core::Error::new(
                    azure_core::error::ErrorKind::Other,
                    format!("Unexpected request: {} {}", request.method(), request.url()),
                ));
            }

            let mut headers = azure_core::headers::Headers::new();
            for (key, value) in &expected.response.headers {
                headers.insert(*key, value);
            }

            let body = expected.response.body.clone();
            Ok(azure_core::Response::new(
                expected.response.status,
                headers,
                Box::pin(futures::stream::once(async move {
                    Ok(hyper::body::Bytes::from(body))
                })),
            ))
        }
    }

    pub fn create_test_store(
        client: MockAzureClient,
        retry_config: Option<nativelink_config::stores::Retry>,
        consider_expired_after_s: Option<u32>,
    ) -> Result<AzureBlobStoreTest, Error> {
        let transport = TransportOptions::new(Arc::new(client) as Arc<dyn HttpClient>);
        let credentials = StorageCredentials::access_key(
            TEST_ACCOUNT.to_string(),
            azure_core::auth::Secret::new(TEST_KEY.to_string()),
        );

        let container_client = BlobServiceClient::builder(TEST_ACCOUNT, credentials)
            .transport(transport)
            .container_client(TEST_CONTAINER);

        let spec = AzureBlobSpec {
            account_name: TEST_ACCOUNT.to_string(),
            account_key: TEST_KEY.to_string(),
            container: TEST_CONTAINER.to_string(),
            retry: retry_config.unwrap_or_default(),
            consider_expired_after_s: consider_expired_after_s.unwrap_or(0),
            ..Default::default()
        };

        AzureBlobStore::new_with_client_and_jitter(
            &spec,
            container_client,
            Arc::new(|_| Duration::from_secs(0)),
            MockInstantWrapped::default,
        )
    }

    pub fn create_test_digest() -> Result<DigestInfo, Error> {
        DigestInfo::try_new(TEST_HASH, TEST_SIZE)
    }
}

use test_utils::*;

#[nativelink_test]
async fn test_has_object_found() -> Result<(), Error> {
    let client = MockAzureClient::single_head_request(TestResponse::ok().with_content_length(512));
    let store = create_test_store(client.clone(), None, None)?;

    let result = store.has(create_test_digest()?).await?;

    assert_eq!(result, Some(512));
    assert_eq!(client.request_count(), 1);
    Ok(())
}

#[nativelink_test]
async fn test_has_object_not_found() -> Result<(), Error> {
    let client = MockAzureClient::single_head_request(TestResponse::not_found());
    let store = create_test_store(client.clone(), None, None)?;

    let result = store.has(create_test_digest()?).await?;

    assert_eq!(result, None);
    assert_eq!(client.request_count(), 1);
    Ok(())
}

#[nativelink_test]
async fn test_has_with_retries() -> Result<(), Error> {
    let client = MockAzureClient::new(vec![
        TestRequest::head(
            TEST_HASH,
            TEST_SIZE,
            TestResponse::error(StatusCode::InternalServerError),
        ),
        TestRequest::head(
            TEST_HASH,
            TEST_SIZE,
            TestResponse::ok().with_content_length(111),
        ),
    ]);

    let retry_config = nativelink_config::stores::Retry {
        max_retries: 3,
        delay: 0.0,
        jitter: 0.0,
        ..Default::default()
    };

    let store = create_test_store(client.clone(), Some(retry_config), None)?;
    let result = store.has(create_test_digest()?).await?;

    assert_eq!(result, Some(111));
    assert_eq!(client.request_count(), 2);
    Ok(())
}

#[nativelink_test]
async fn test_get() -> Result<(), Error> {
    const VALUE: &str = "test_content";

    let client = MockAzureClient::new(vec![TestRequest::get(
        TEST_HASH,
        TEST_SIZE,
        TestResponse::ok().with_body(VALUE.as_bytes().to_vec()),
    )]);

    let store = create_test_store(client.clone(), None, None)?;
    let result = store
        .get_part_unchunked(create_test_digest()?, 0, None)
        .await?;

    assert_eq!(result, VALUE.as_bytes());
    assert_eq!(client.request_count(), 1);
    Ok(())
}

#[nativelink_test]
async fn test_get_with_retries() -> Result<(), Error> {
    const VALUE: &str = "test_content";

    let client = MockAzureClient::new(vec![
        TestRequest::get(
            TEST_HASH,
            TEST_SIZE,
            TestResponse::error(StatusCode::InternalServerError),
        ),
        TestRequest::get(
            TEST_HASH,
            TEST_SIZE,
            TestResponse::error(StatusCode::ServiceUnavailable),
        ),
        TestRequest::get(
            TEST_HASH,
            TEST_SIZE,
            TestResponse::error(StatusCode::Conflict),
        ),
        TestRequest::get(
            TEST_HASH,
            TEST_SIZE,
            TestResponse::ok().with_body(VALUE.as_bytes().to_vec()),
        ),
    ]);

    let retry_config = nativelink_config::stores::Retry {
        max_retries: 1024,
        delay: 0.0,
        jitter: 0.0,
        ..Default::default()
    };

    let store = create_test_store(client.clone(), Some(retry_config), None)?;
    let result = store
        .get_part_unchunked(create_test_digest()?, 0, None)
        .await?;

    assert_eq!(result, VALUE.as_bytes());
    assert_eq!(client.request_count(), 4);
    Ok(())
}

#[nativelink_test]
async fn test_update_small_file() -> Result<(), Error> {
    const CONTENT_LENGTH: usize = 1024; // Small enough for single block upload (<5MB).
    let mut send_data = BytesMut::new();
    for i in 0..CONTENT_LENGTH {
        send_data.put_u8(((i % 93) + 33) as u8);
    }
    let send_data = send_data.freeze();

    let client = MockAzureClient::new(vec![TestRequest::put(
        TEST_HASH,
        TEST_SIZE,
        TestResponse::ok(),
    )]);

    let store = create_test_store(client.clone(), None, None)?;
    let (mut tx, rx) = make_buf_channel_pair();

    // Starting the update futures
    let update_fut = store.update(
        create_test_digest()?,
        rx,
        UploadSizeInfo::ExactSize(CONTENT_LENGTH as u64),
    );

    // Sending the data in smaller chunks to test streaming
    let send_data_copy = send_data.clone();
    let send_fut = Box::pin(async move {
        const CHUNK_SIZE: usize = 256;
        for chunk in send_data_copy.chunks(CHUNK_SIZE) {
            tx.send(hyper::body::Bytes::copy_from_slice(chunk)).await?;
        }
        tx.send_eof()
    });

    // Waiting for both futures to complete
    let (update_result, send_result) = tokio::join!(update_fut, send_fut);

    update_result?;
    send_result?;
    assert_eq!(client.request_count(), 1);
    Ok(())
}

#[nativelink_test]
async fn test_update_zero_size() -> Result<(), Error> {
    let client = MockAzureClient::new(vec![]); // Should not make any requests
    let store = create_test_store(client.clone(), None, None)?;
    let (mut tx, rx) = make_buf_channel_pair();

    let update_fut = store.update(create_test_digest()?, rx, UploadSizeInfo::ExactSize(0));

    let send_fut = async move { tx.send_eof() };

    let (update_result, send_result) = tokio::join!(update_fut, send_fut);

    update_result?;
    send_result?;
    assert_eq!(
        client.request_count(),
        0,
        "Zero-size upload should not make any requests"
    );
    Ok(())
}

#[nativelink_test]
async fn test_update_with_retries() -> Result<(), Error> {
    const CONTENT_LENGTH: usize = 512;

    let mut send_data = BytesMut::new();
    for i in 0..CONTENT_LENGTH {
        send_data.put_u8(((i % 93) + 33) as u8);
    }
    let send_data = send_data.freeze();

    let client = MockAzureClient::new(vec![
        TestRequest::put(
            TEST_HASH,
            TEST_SIZE,
            TestResponse::error(StatusCode::InternalServerError),
        ),
        TestRequest::put(TEST_HASH, TEST_SIZE, TestResponse::ok()),
    ]);

    let retry_config = nativelink_config::stores::Retry {
        max_retries: 3,
        delay: 0.0,
        jitter: 0.0,
        ..Default::default()
    };

    let store = create_test_store(client.clone(), Some(retry_config), None)?;
    let (mut tx, rx) = make_buf_channel_pair();

    let update_fut = store.update(
        create_test_digest()?,
        rx,
        UploadSizeInfo::ExactSize(CONTENT_LENGTH as u64),
    );

    let send_data_copy = send_data.clone();
    let send_fut = async move {
        tx.send(send_data_copy).await?;
        tx.send_eof()
    };

    let (update_result, send_result) = tokio::join!(update_fut, send_fut);

    update_result?;
    send_result?;
    assert_eq!(client.request_count(), 2);
    Ok(())
}

#[nativelink_test]
async fn test_multipart_upload_large_file() -> Result<(), Error> {
    const MIN_BLOCK_SIZE: usize = 5 * 1024 * 1024; // 5MB
    const TOTAL_SIZE: usize = MIN_BLOCK_SIZE * 2 + 50;
    const DIGEST_STR: &str = TEST_HASH;

    let mut send_data = Vec::with_capacity(TOTAL_SIZE);
    for i in 0..TOTAL_SIZE {
        send_data.push(((i * 3) % 256) as u8);
    }

    // Generate and manually URL-encode Base64-encoded block IDs
    let block_ids: Vec<String> = (0..3)
        .map(|i| {
            let block_id = format!("{i:032}");
            let base64_encoded = encode(block_id);
            base64_encoded
                .replace('=', "%3D")
                .replace('+', "%2B")
                .replace('/', "%2F")
        })
        .collect();

    let client = MockAzureClient::new(vec![
        TestRequest::new(
            "PUT",
            format!(
                "{TEST_CONTAINER}/{DIGEST_STR}-{TOTAL_SIZE}?blockid={}&comp=block",
                block_ids[0]
            ),
            TestResponse::ok(),
        ),
        TestRequest::new(
            "PUT",
            format!(
                "{TEST_CONTAINER}/{DIGEST_STR}-{TOTAL_SIZE}?blockid={}&comp=block",
                block_ids[1]
            ),
            TestResponse::ok(),
        ),
        TestRequest::new(
            "PUT",
            format!(
                "{TEST_CONTAINER}/{DIGEST_STR}-{TOTAL_SIZE}?blockid={}&comp=block",
                block_ids[2]
            ),
            TestResponse::ok(),
        ),
        TestRequest::new(
            "PUT",
            format!("{TEST_CONTAINER}/{DIGEST_STR}-{TOTAL_SIZE}?comp=blocklist"),
            TestResponse::ok(),
        ),
    ]);

    let store = create_test_store(client.clone(), None, None)?;
    let digest = DigestInfo::try_new(DIGEST_STR, TOTAL_SIZE)?;

    let _origin_guard = OriginContext::new();

    let store_key: StoreKey = StoreKey::from(&digest);
    store.update_oneshot(store_key, send_data.into()).await?;
    assert_eq!(client.request_count(), 4);
    Ok(())
}

#[nativelink_test]
async fn test_get_part_zero_digest() -> Result<(), Error> {
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);
    let client = MockAzureClient::new(vec![]);
    let store = Arc::new(create_test_store(client.clone(), None, None)?);
    let (mut writer, mut reader) = make_buf_channel_pair();

    let _origin_guard = OriginContext::new();
    tokio::task::yield_now().await;

    // Running both operations concurrently using join!
    let (get_result, file_data) = tokio::join!(
        store.get_part(digest, &mut writer, 0, None),
        reader.consume(Some(1024))
    );

    get_result?;
    let file_data = file_data.err_tip(|| "Error reading bytes")?;

    assert_eq!(file_data.len(), 0, "Expected empty file content");
    assert_eq!(
        client.request_count(),
        0,
        "Expected no requests for zero digest"
    );
    Ok(())
}
#[nativelink_test]
async fn test_has_with_results_zero_digests() -> Result<(), Error> {
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);
    let store_key: StoreKey = StoreKey::from(&digest);
    let keys = vec![store_key];
    let mut results = vec![None];

    let client = MockAzureClient::new(vec![]); // Should make no requests
    let store = create_test_store(client.clone(), None, None)?;

    let origin_guard = OriginContext::new();
    tokio::task::yield_now().await;

    store.has_with_results(&keys, &mut results).await?;

    assert_eq!(results, vec![Some(0)]);
    assert_eq!(
        client.request_count(),
        0,
        "Expected no requests for zero digest"
    );
    drop(origin_guard);
    Ok(())
}

#[nativelink_test]
async fn test_has_with_expired_result() -> Result<(), Error> {
    const CONTENT_SIZE: usize = 10;
    use mock_instant::thread_local::MockClock;

    let client = MockAzureClient::new(vec![
        TestRequest::head(
            TEST_HASH,
            CONTENT_SIZE as u64,
            TestResponse::ok().with_content_length(512),
        ),
        TestRequest::head(
            TEST_HASH,
            CONTENT_SIZE as u64,
            TestResponse::ok().with_content_length(512),
        ),
    ]);

    let store = create_test_store(
        client.clone(),
        Some(nativelink_config::stores::Retry {
            max_retries: 1,
            delay: 0.0,
            jitter: 0.0,
            ..Default::default()
        }),
        Some(2 * 24 * 60 * 60),
    )?;

    // Time starts at 1970-01-01 00:00:00
    let digest = DigestInfo::try_new(TEST_HASH, CONTENT_SIZE)?;

    // Check at 1 day
    {
        MockClock::advance(Duration::from_secs(24 * 60 * 60));
        let mut results = vec![None];
        store
            .has_with_results(&[digest.into()], &mut results)
            .await?;
        assert_eq!(
            results,
            vec![Some(512)],
            "Should find non-expired content after 1 day"
        );
    }

    // Check at 3 days (expired)
    {
        MockClock::advance(Duration::from_secs(3 * 24 * 60 * 60));
        let mut results = vec![None];
        store
            .has_with_results(&[digest.into()], &mut results)
            .await?;
        assert_eq!(
            results,
            vec![None],
            "Should not find expired content after 3 days"
        );
    }

    assert_eq!(client.request_count(), 2);
    Ok(())
}
