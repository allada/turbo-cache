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
use std::time::Duration;

use aws_sdk_s3::config::{BehaviorVersion, Builder, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use bytes::{BufMut, Bytes, BytesMut};
use futures::join;
use futures::task::Poll;
use http::header;
use http::status::StatusCode;
use hyper::Body;
use mock_instant::thread_local::MockClock;
use nativelink_error::{make_input_err, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::s3_store::S3Store;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::spawn;
use nativelink_util::store_trait::{StoreLike, UploadSizeInfo};
use pretty_assertions::assert_eq;
use sha2::{Digest, Sha256};

// TODO(aaronmondal): Figure out how to test the connector retry mechanism.

const BUCKET_NAME: &str = "dummy-bucket-name";
const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const REGION: &str = "testregion";

#[nativelink_test]
async fn simple_has_object_found() -> Result<(), Error> {
    let mock_client = StaticReplayClient::new(vec![ReplayEvent::new(
        http::Request::builder().body(SdkBody::empty()).unwrap(),
        http::Response::builder()
            .header(header::CONTENT_LENGTH, "512")
            .body(SdkBody::empty())
            .unwrap(),
    )]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);
    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
    let result = store.has(digest).await;
    assert_eq!(
        result,
        Ok(Some(512)),
        "Expected to find item, got: {result:?}"
    );
    Ok(())
}

#[nativelink_test]
async fn simple_has_object_not_found() -> Result<(), Error> {
    let mock_client = StaticReplayClient::new(vec![ReplayEvent::new(
        http::Request::builder().body(SdkBody::empty()).unwrap(),
        http::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(SdkBody::empty())
            .unwrap(),
    )]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);
    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;
    let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
    let result = store.has(digest).await;
    assert_eq!(
        result,
        Ok(None),
        "Expected to not find item, got: {result:?}"
    );
    Ok(())
}

#[nativelink_test]
async fn simple_has_retries() -> Result<(), Error> {
    let mock_client = StaticReplayClient::new(vec![
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            http::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(SdkBody::empty())
                .unwrap(),
        ),
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            http::Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(SdkBody::empty())
                .unwrap(),
        ),
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            http::Response::builder()
                .status(StatusCode::CONFLICT)
                .body(SdkBody::empty())
                .unwrap(),
        ),
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_LENGTH, "111")
                .body(SdkBody::empty())
                .unwrap(),
        ),
    ]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);

    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            retry: nativelink_config::stores::Retry {
                max_retries: 1024,
                delay: 0.,
                jitter: 0.,
                ..Default::default()
            },
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
    let result = store.has(digest).await;
    assert_eq!(
        result,
        Ok(Some(111)),
        "Expected to find item, got: {:?}",
        result
    );
    Ok(())
}

#[nativelink_test]
async fn simple_update_ac() -> Result<(), Error> {
    const AC_ENTRY_SIZE: u64 = 199;
    const CONTENT_LENGTH: usize = 50;
    let mut send_data = BytesMut::new();
    for i in 0..CONTENT_LENGTH {
        send_data.put_u8(u8::try_from((i % 93) + 33).expect("Failed to convert to u8"));
    }
    let send_data = send_data.freeze();

    let (mock_client, request_receiver) =
        aws_smithy_runtime::client::http::test_util::capture_request(Some(
            aws_smithy_runtime_api::http::Response::new(
                StatusCode::OK.into(),
                SdkBody::empty(), // This is an upload, so server does not send a body.
            )
            .try_into_http02x()
            .unwrap(),
        ));
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);
    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;
    let (mut tx, rx) = make_buf_channel_pair();
    // Make future responsible for processing the datastream
    // and forwarding it to the s3 backend/server.
    let mut update_fut = Box::pin(async move {
        store
            .update(
                DigestInfo::try_new(VALID_HASH1, AC_ENTRY_SIZE)?,
                rx,
                UploadSizeInfo::ExactSize(CONTENT_LENGTH as u64),
            )
            .await
    });

    // Extract out the body stream sent by the s3 store.
    let body_stream = {
        // We need to poll here to get the request sent, but future
        // wont be done until we send all the data (which we do later).
        assert_eq!(Poll::Pending, futures::poll!(&mut update_fut));
        let sent_request = request_receiver.expect_request();
        assert_eq!(sent_request.method(), "PUT");
        assert_eq!(sent_request.uri(), format!("https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{VALID_HASH1}-{AC_ENTRY_SIZE}?x-id=PutObject"));
        ByteStream::from_body_0_4(sent_request.into_body())
    };

    let send_data_copy = send_data.clone();
    // Create spawn that is responsible for sending the stream of data
    // to the S3Store and processing/forwarding to the S3 backend.
    let spawn_fut = spawn!("simple_update_ac", async move {
        tokio::try_join!(update_fut, async move {
            for i in 0..CONTENT_LENGTH {
                tx.send(send_data_copy.slice(i..=i)).await?;
            }
            tx.send_eof()
        })
        .or_else(|e| {
            // Printing error to make it easier to debug, since ordering
            // of futures is not guaranteed.
            eprintln!("Error updating or sending in spawn: {e:?}");
            Err(e)
        })
    });

    // Wait for all the data to be received by the s3 backend server.
    let data_sent_to_s3 = body_stream
        .collect()
        .await
        .map_err(|e| make_input_err!("{e:?}"))?;
    assert_eq!(
        send_data,
        data_sent_to_s3.into_bytes(),
        "Expected data to match"
    );

    // Collect our spawn future to ensure it completes without error.
    spawn_fut
        .await
        .err_tip(|| "Failed to launch spawn")?
        .err_tip(|| "In spawn")?;
    Ok(())
}

#[nativelink_test]
async fn simple_get_ac() -> Result<(), Error> {
    const VALUE: &str = "23";
    const AC_ENTRY_SIZE: u64 = 1000; // Any size that is not VALUE.len().

    let mock_client = StaticReplayClient::new(vec![ReplayEvent::new(
        http::Request::builder().body(SdkBody::empty()).unwrap(),
        http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::from(VALUE))
            .unwrap(),
    )]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);
    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    let store_data = store
        .get_part_unchunked(DigestInfo::try_new(VALID_HASH1, AC_ENTRY_SIZE)?, 0, None)
        .await?;
    assert_eq!(
            store_data,
            VALUE.as_bytes(),
            "Hash for key: {VALID_HASH1} did not insert. Expected: {VALUE:#x?}, but got: {store_data:#x?}",
        );
    Ok(())
}

#[nativelink_test]
async fn smoke_test_get_part() -> Result<(), Error> {
    const AC_ENTRY_SIZE: u64 = 1000; // Any size that is not raw_send_data.len().
    const OFFSET: usize = 105;
    const LENGTH: usize = 50_000; // Just a size that is not the same as the real data size.
    let mock_client = StaticReplayClient::new(vec![ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{VALID_HASH1}-{AC_ENTRY_SIZE}?x-id=GetObject",
                ))
                .header("range", format!("bytes={}-{}", OFFSET, OFFSET + LENGTH))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .body(SdkBody::empty())
                .unwrap(),
        )]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client.clone())
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);
    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    store
        .get_part_unchunked(
            DigestInfo::try_new(VALID_HASH1, AC_ENTRY_SIZE)?,
            OFFSET as u64,
            Some(LENGTH as u64),
        )
        .await?;

    mock_client.assert_requests_match(&[]);
    Ok(())
}

#[nativelink_test]
async fn get_part_simple_retries() -> Result<(), Error> {
    let mock_client = StaticReplayClient::new(vec![
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            http::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(SdkBody::empty())
                .unwrap(),
        ),
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            http::Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(SdkBody::empty())
                .unwrap(),
        ),
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            http::Response::builder()
                .status(StatusCode::CONFLICT)
                .body(SdkBody::empty())
                .unwrap(),
        ),
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .body(SdkBody::empty())
                .unwrap(),
        ),
    ]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);

    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            retry: nativelink_config::stores::Retry {
                max_retries: 1024,
                delay: 0.,
                jitter: 0.,
                ..Default::default()
            },
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_ok(), "Expected to find item, got: {result:?}");
    Ok(())
}

#[nativelink_test]
async fn multipart_update_large_cas() -> Result<(), Error> {
    // Same as in s3_store.
    const MIN_MULTIPART_SIZE: usize = 5 * 1024 * 1024; // 5mb.
    const AC_ENTRY_SIZE: usize = MIN_MULTIPART_SIZE * 2 + 50;

    let mut send_data = Vec::with_capacity(AC_ENTRY_SIZE);
    for i in 0..send_data.capacity() {
        send_data.push(u8::try_from((i * 3) % 256).unwrap());
    }

    let digest = DigestInfo::try_new(VALID_HASH1, send_data.len())?;

    let mock_client = StaticReplayClient::new(vec![
            ReplayEvent::new(
                http::Request::builder()
                    .uri(format!(
                        "https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{VALID_HASH1}-{AC_ENTRY_SIZE}?uploads",
                    ))
                    .method("POST")
                    .body(SdkBody::empty())
                    .unwrap(),
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(SdkBody::from(
                        r#"
                        <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                          <UploadId>Dummy-uploadid</UploadId>
                        </InitiateMultipartUploadResult>"#
                            .as_bytes(),
                    ))
                    .unwrap(),
            ),
            ReplayEvent::new(
                http::Request::builder()
                    .uri(format!(
                        "https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{VALID_HASH1}-{AC_ENTRY_SIZE}?x-id=UploadPart&partNumber=1&uploadId=Dummy-uploadid",
                    ))
                    .method("PUT")
                    .header("content-type", "application/octet-stream")
                    .header("content-length", "5242880")
                    .body(SdkBody::from(&send_data[0..MIN_MULTIPART_SIZE]))
                    .unwrap(),
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            ReplayEvent::new(
                http::Request::builder()
                    .uri(format!(
                        "https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{VALID_HASH1}-{AC_ENTRY_SIZE}?x-id=UploadPart&partNumber=2&uploadId=Dummy-uploadid",
                    ))
                    .method("PUT")
                    .header("content-type", "application/octet-stream")
                    .header("content-length", "5242880")
                    .body(SdkBody::from(&send_data[MIN_MULTIPART_SIZE..MIN_MULTIPART_SIZE * 2]))
                    .unwrap(),
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            ReplayEvent::new(
                http::Request::builder()
                    .uri(format!(
                        "https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{VALID_HASH1}-{AC_ENTRY_SIZE}?x-id=UploadPart&partNumber=3&uploadId=Dummy-uploadid",
                    ))
                    .method("PUT")
                    .header("content-type", "application/octet-stream")
                    .header("content-length", "50")
                    .body(SdkBody::from(
                        &send_data[MIN_MULTIPART_SIZE * 2..MIN_MULTIPART_SIZE * 2 + 50],
                    ))
                    .unwrap(),
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            ReplayEvent::new(
                http::Request::builder()
                    .uri(format!(
                        "https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{VALID_HASH1}-{AC_ENTRY_SIZE}?uploadId=Dummy-uploadid",
                    ))
                    .method("POST")
                    .header("content-length", "216")
                    .body(SdkBody::from(concat!(
                        r#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
                        "<Part><PartNumber>1</PartNumber></Part>",
                        "<Part><PartNumber>2</PartNumber></Part>",
                        "<Part><PartNumber>3</PartNumber></Part>",
                        "</CompleteMultipartUpload>",
                    )))
                    .unwrap(),
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(SdkBody::from(concat!(
                        "<CompleteMultipartUploadResult>",
                        "</CompleteMultipartUploadResult>",
                    )))
                    .unwrap(),
            ),
        ]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client.clone())
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);
    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;
    store
        .update_oneshot(digest, send_data.into())
        .await
        .unwrap();
    mock_client.assert_requests_match(&[]);
    Ok(())
}

#[nativelink_test]
async fn ensure_empty_string_in_stream_works_test() -> Result<(), Error> {
    const CAS_ENTRY_SIZE: usize = 10; // Length of "helloworld".
    let (mut tx, channel_body) = Body::channel();
    let mock_client = StaticReplayClient::new(vec![ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{VALID_HASH1}-{CAS_ENTRY_SIZE}?x-id=GetObject",
                ))
                .header("range", format!("bytes={}-{}", 0, CAS_ENTRY_SIZE))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .body(SdkBody::from_body_0_4(channel_body))
                .unwrap(),
        )]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client.clone())
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);
    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    let (_, get_part_result) = join!(
        async move {
            tx.send_data(Bytes::from_static(b"hello")).await?;
            tx.send_data(Bytes::from_static(b"")).await?;
            tx.send_data(Bytes::from_static(b"world")).await?;
            Result::<(), hyper::Error>::Ok(())
        },
        store.get_part_unchunked(
            DigestInfo::try_new(VALID_HASH1, CAS_ENTRY_SIZE)?,
            0,
            Some(CAS_ENTRY_SIZE as u64),
        )
    );
    assert_eq!(
        get_part_result.err_tip(|| "Expected get_part_result to pass")?,
        "helloworld".as_bytes()
    );

    mock_client.assert_requests_match(&[]);
    Ok(())
}

#[nativelink_test]
async fn get_part_is_zero_digest() -> Result<(), Error> {
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);

    let mock_client = StaticReplayClient::new(vec![]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);
    let store = Arc::new(S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?);
    let store_clone = store.clone();
    let (mut writer, mut reader) = make_buf_channel_pair();

    let _drop_guard = spawn!("get_part_is_zero_digest", async move {
        store_clone
            .get_part(digest, &mut writer, 0, None)
            .await
            .unwrap();
    });

    let file_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading bytes")?;

    let empty_bytes = Bytes::new();
    assert_eq!(&file_data, &empty_bytes, "Expected file content to match");
    Ok(())
}

#[nativelink_test]
async fn has_with_results_on_zero_digests() -> Result<(), Error> {
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);
    let keys = vec![digest.into()];
    let mut results = vec![None];

    let mock_client = StaticReplayClient::new(vec![]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);
    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    store.has_with_results(&keys, &mut results).await.unwrap();
    assert_eq!(results, vec!(Some(0)));

    Ok(())
}

#[nativelink_test]
async fn has_with_expired_result() -> Result<(), Error> {
    const CAS_ENTRY_SIZE: usize = 10;
    let mock_client = StaticReplayClient::new(vec![
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            http::Response::builder()
                .header(header::CONTENT_LENGTH, "512")
                .header(header::LAST_MODIFIED, "Thu, 01 Jan 1970 00:00:00 GMT")
                .body(SdkBody::empty())
                .unwrap(),
        ),
        ReplayEvent::new(
            http::Request::builder().body(SdkBody::empty()).unwrap(),
            http::Response::builder()
                .header(header::CONTENT_LENGTH, "512")
                .header(header::LAST_MODIFIED, "Thu, 01 Jan 1970 00:00:00 GMT")
                .body(SdkBody::empty())
                .unwrap(),
        ),
    ]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::from_static(REGION))
        .http_client(mock_client)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);
    let store = S3Store::new_with_client_and_jitter(
        &nativelink_config::stores::S3Store {
            bucket: BUCKET_NAME.to_string(),
            consider_expired_after_s: 2 * 24 * 60 * 60, // 2 days.
            ..Default::default()
        },
        s3_client,
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    // Time starts at 1970-01-01 00:00:00.
    let digest = DigestInfo::try_new(VALID_HASH1, CAS_ENTRY_SIZE).unwrap();
    {
        MockClock::advance(Duration::from_secs(24 * 60 * 60)); // 1 day.
                                                               // Date is now 1970-01-02 00:00:00.
        let mut results = vec![None];
        store
            .has_with_results(&[digest.into()], &mut results)
            .await
            .unwrap();
        assert_eq!(results, vec![Some(512)]);
    }
    {
        MockClock::advance(Duration::from_secs(24 * 60 * 60)); // 1 day.
                                                               // Date is now 1970-01-03 00:00:00.
        let mut results = vec![None];
        store
            .has_with_results(&[digest.into()], &mut results)
            .await
            .unwrap();
        // The result should be expired even though s3 says it's there.
        assert_eq!(results, vec![None]);
    }

    Ok(())
}
