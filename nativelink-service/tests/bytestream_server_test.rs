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

use bytes::Bytes;
use futures::task::Poll;
use futures::{poll, Future};
use http_body_util::BodyExt;
use hyper::body::Frame;
use hyper::Uri;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use maplit::hashmap;
use nativelink_config::cas_server::ByteStreamConfig;
use nativelink_config::stores::{MemorySpec, StoreRef};
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, WriteRequest, WriteResponse,
};
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::{encode_stream_proto, DigestInfo};
use nativelink_util::store_trait::StoreLike;
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_util::{background_spawn, spawn};
use pretty_assertions::assert_eq;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::yield_now;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::codec::{Codec, CompressionEncoding, ProstCodec};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Streaming};
use tower::service_fn;

const INSTANCE_NAME: &str = "foo_instance_name";
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";

async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "main_cas",
        store_factory(
            &StoreRef::new("main", MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_bytestream_server(
    store_manager: &StoreManager,
    config: Option<ByteStreamConfig>,
) -> Result<ByteStreamServer, Error> {
    let config = config.unwrap_or(nativelink_config::cas_server::ByteStreamConfig {
        cas_stores: hashmap! {
            "foo_instance_name".to_string() => "main_cas".to_string(),
        },
        persist_stream_on_disconnect_timeout: 0,
        max_bytes_per_stream: 1024,
        max_decoding_message_size: 0,
    });
    ByteStreamServer::new(&config, store_manager)
}

fn make_stream(
    encoding: Option<CompressionEncoding>,
) -> (mpsc::Sender<Frame<Bytes>>, Streaming<WriteRequest>) {
    let (tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
    let stream = Streaming::new_request(codec.decoder(), body, encoding, None);
    (tx, stream)
}

type JoinHandle = JoinHandleDropGuard<Result<Response<WriteResponse>, tonic::Status>>;

fn make_stream_and_writer_spawn(
    bs_server: Arc<ByteStreamServer>,
    encoding: Option<CompressionEncoding>,
) -> (mpsc::Sender<Frame<Bytes>>, JoinHandle) {
    let (tx, stream) = make_stream(encoding);
    let join_handle = spawn!("bs_server_write", async move {
        bs_server.write(Request::new(stream)).await
    });
    (tx, join_handle)
}

fn make_resource_name(data_len: impl std::fmt::Display) -> String {
    format!(
        "{}/uploads/{}/blobs/{}/{}",
        INSTANCE_NAME,
        "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
        HASH1,
        data_len,
    )
}

async fn server_and_client_stub(
    bs_server: ByteStreamServer,
) -> (JoinHandleDropGuard<()>, ByteStreamClient<Channel>) {
    #[derive(Clone)]
    struct Executor;
    impl<F> hyper::rt::Executor<F> for Executor
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        fn execute(&self, fut: F) {
            background_spawn!("executor_spawn", fut);
        }
    }

    let (tx, rx) = unbounded_channel::<Result<DuplexStream, Error>>();
    let mut rx = UnboundedReceiverStream::new(rx);

    let server_spawn = spawn!("grpc_server", async move {
        let http = auto::Builder::new(Executor);
        let grpc_service = tonic::service::Routes::new(bs_server.into_service());

        let adapted_service = tower::ServiceBuilder::new()
            .map_request(|req: hyper::Request<hyper::body::Incoming>| {
                let (parts, body) = req.into_parts();
                let body = body
                    .map_err(|e| tonic::Status::internal(e.to_string()))
                    .boxed_unsync();
                hyper::Request::from_parts(parts, body)
            })
            .service(grpc_service);

        let hyper_service = TowerToHyperService::new(adapted_service);

        while let Some(stream) = rx.next().await {
            http.serve_connection_with_upgrades(
                TokioIo::new(stream.expect("Failed to get stream")),
                hyper_service.clone(),
            )
            .await
            .expect("Connection failed");
        }
    });

    // Note: This is a dummy address, it will not actually connect to it,
    // instead it will be connecting via mpsc.
    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .executor(Executor)
        .connect_with_connector(service_fn(move |_: Uri| {
            let tx = tx.clone();
            async move {
                const MAX_BUFFER_SIZE: usize = 4096;
                let (client, server) = tokio::io::duplex(MAX_BUFFER_SIZE);
                tx.send(Ok(server)).unwrap();
                Result::<_, Error>::Ok(TokioIo::new(client))
            }
        }))
        .await
        .unwrap();

    let client = ByteStreamClient::new(channel);

    (server_spawn, client)
}

#[nativelink_test]
pub async fn chunked_stream_receives_all_data() -> Result<(), Box<dyn std::error::Error>> {
    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );
    let store = store_manager.get_store("main_cas").unwrap();

    // Setup stream.
    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server, Some(CompressionEncoding::Gzip));

    // Send data.
    let raw_data = {
        // Chunk our data into two chunks to simulate something a client
        // might do.
        const BYTE_SPLIT_OFFSET: usize = 8;

        let raw_data = "12456789abcdefghijk".as_bytes();

        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
            HASH1,
            raw_data.len()
        );
        let mut write_request = WriteRequest {
            resource_name,
            write_offset: 0,
            finish_write: false,
            data: vec![].into(),
        };
        // Write first chunk of data.
        write_request.write_offset = 0;
        write_request.data = raw_data[..BYTE_SPLIT_OFFSET].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;

        // Write empty set of data (clients are allowed to do this.
        write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
        write_request.data = vec![].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;

        // Write final bit of data.
        write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
        write_request.data = raw_data[BYTE_SPLIT_OFFSET..].into();
        write_request.finish_write = true;
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;

        raw_data
    };
    // Check results of server.
    {
        // One for spawn() future and one for result.
        let server_result = join_handle
            .await
            .expect("Failed to join")
            .expect("Failed write");
        let committed_size = usize::try_from(server_result.into_inner().committed_size)
            .or(Err("Cant convert i64 to usize"))?;
        assert_eq!(committed_size, raw_data.len());

        // Now lets check our store to ensure it was written with proper data.
        assert!(
            store
                .has(DigestInfo::try_new(HASH1, raw_data.len())?)
                .await?
                .is_some(),
            "Not found in store",
        );
        let store_data = store
            .get_part_unchunked(DigestInfo::try_new(HASH1, raw_data.len())?, 0, None)
            .await?;
        assert_eq!(
            std::str::from_utf8(&store_data),
            std::str::from_utf8(raw_data),
            "Expected store to have been updated to new value"
        );
    }
    Ok(())
}

#[nativelink_test]
pub async fn resume_write_success() -> Result<(), Box<dyn std::error::Error>> {
    const WRITE_DATA: &str = "12456789abcdefghijk";

    // Chunk our data into two chunks to simulate something a client
    // might do.
    const BYTE_SPLIT_OFFSET: usize = 8;

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );
    let store = store_manager.get_store("main_cas").unwrap();

    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server.clone(), Some(CompressionEncoding::Gzip));

    let resource_name = format!(
        "{}/uploads/{}/blobs/{}/{}",
        INSTANCE_NAME,
        "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
        HASH1,
        WRITE_DATA.len()
    );
    let mut write_request = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: false,
        data: vec![].into(),
    };
    {
        // Write first chunk of data.
        write_request.write_offset = 0;
        write_request.data = WRITE_DATA[..BYTE_SPLIT_OFFSET].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    {
        // Now disconnect our stream.
        drop(tx);
        let result = join_handle.await.expect("Failed to join");
        assert_eq!(result.is_err(), true, "Expected error to be returned");
    }
    // Now reconnect.
    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server, Some(CompressionEncoding::Gzip));
    {
        // Write the remainder of our data.
        write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
        write_request.finish_write = true;
        write_request.data = WRITE_DATA[BYTE_SPLIT_OFFSET..].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    {
        // Now disconnect our stream.
        drop(tx);
        join_handle
            .await
            .expect("Failed to join")
            .expect("Failed write");
    }
    {
        // Check to make sure our store recorded the data properly.
        let digest = DigestInfo::try_new(HASH1, WRITE_DATA.len())?;
        assert_eq!(
            store.get_part_unchunked(digest, 0, None).await?,
            WRITE_DATA,
            "Data written to store did not match expected data",
        );
    }
    Ok(())
}

#[nativelink_test]
pub async fn restart_write_success() -> Result<(), Box<dyn std::error::Error>> {
    const WRITE_DATA: &str = "12456789abcdefghijk";

    // Chunk our data into two chunks to simulate something a client
    // might do.
    const BYTE_SPLIT_OFFSET: usize = 8;

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );
    let store = store_manager.get_store("main_cas").unwrap();

    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server.clone(), Some(CompressionEncoding::Gzip));

    let resource_name = format!(
        "{}/uploads/{}/blobs/{}/{}",
        INSTANCE_NAME,
        "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
        HASH1,
        WRITE_DATA.len()
    );
    let mut write_request = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: false,
        data: vec![].into(),
    };
    {
        // Write first chunk of data.
        write_request.write_offset = 0;
        write_request.data = WRITE_DATA[..BYTE_SPLIT_OFFSET].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    {
        // Now disconnect our stream.
        drop(tx);
        let result = join_handle.await.expect("Failed to join");
        assert_eq!(result.is_err(), true, "Expected error to be returned");
    }
    // Now reconnect.
    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server, Some(CompressionEncoding::Gzip));
    {
        // Write first chunk of data again.
        write_request.write_offset = 0;
        write_request.data = WRITE_DATA[..BYTE_SPLIT_OFFSET].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    {
        // Write the remainder of our data.
        write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
        write_request.finish_write = true;
        write_request.data = WRITE_DATA[BYTE_SPLIT_OFFSET..].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    {
        // Now disconnect our stream.
        drop(tx);
        let result = join_handle.await.expect("Failed to join");
        assert!(result.is_ok(), "Expected success to be returned");
    }
    {
        // Check to make sure our store recorded the data properly.
        let digest = DigestInfo::try_new(HASH1, WRITE_DATA.len())?;
        assert_eq!(
            store.get_part_unchunked(digest, 0, None).await?,
            WRITE_DATA,
            "Data written to store did not match expected data",
        );
    }
    Ok(())
}

#[nativelink_test]
pub async fn restart_mid_stream_write_success() -> Result<(), Box<dyn std::error::Error>> {
    const WRITE_DATA: &str = "12456789abcdefghijk";

    // Chunk our data into two chunks to simulate something a client
    // might do.
    const BYTE_SPLIT_OFFSET: usize = 8;

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );
    let store = store_manager.get_store("main_cas").unwrap();

    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server.clone(), Some(CompressionEncoding::Gzip));

    let resource_name = format!(
        "{}/uploads/{}/blobs/{}/{}",
        INSTANCE_NAME,
        "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
        HASH1,
        WRITE_DATA.len()
    );
    let mut write_request = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: false,
        data: vec![].into(),
    };
    {
        // Write first chunk of data.
        write_request.write_offset = 0;
        write_request.data = WRITE_DATA[..BYTE_SPLIT_OFFSET].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    {
        // Now disconnect our stream.
        drop(tx);
        let result = join_handle.await.expect("Failed to join");
        assert_eq!(result.is_err(), true, "Expected error to be returned");
    }
    // Now reconnect.
    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server, Some(CompressionEncoding::Gzip));
    {
        // Write some of the first chunk of data again.
        write_request.write_offset = 2;
        write_request.data = WRITE_DATA[2..BYTE_SPLIT_OFFSET].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    {
        // Write the remainder of our data.
        write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
        write_request.finish_write = true;
        write_request.data = WRITE_DATA[BYTE_SPLIT_OFFSET..].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    {
        // Now disconnect our stream.
        drop(tx);
        let result = join_handle.await.expect("Failed to join");
        assert!(result.is_ok(), "Expected success to be returned");
    }
    {
        // Check to make sure our store recorded the data properly.
        let digest = DigestInfo::try_new(HASH1, WRITE_DATA.len())?;
        assert_eq!(
            store.get_part_unchunked(digest, 0, None).await?,
            WRITE_DATA,
            "Data written to store did not match expected data",
        );
    }
    Ok(())
}

#[nativelink_test]
pub async fn ensure_write_is_not_done_until_write_request_is_set(
) -> Result<(), Box<dyn std::error::Error>> {
    const WRITE_DATA: &str = "12456789abcdefghijk";

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );
    let store = store_manager.get_store("main_cas").unwrap();

    // Setup stream.
    let (tx, stream) = make_stream(Some(CompressionEncoding::Gzip));
    let mut write_fut = bs_server.write(Request::new(stream));

    let resource_name = make_resource_name(WRITE_DATA.len());
    let mut write_request = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: false,
        data: vec![].into(),
    };
    {
        // Write our data.
        write_request.write_offset = 0;
        write_request.data = WRITE_DATA[..].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    // Note: We have to pull multiple times because there are multiple futures
    // joined onto this one future and we need to ensure we run the state machine as
    // far as possible.
    for _ in 0..100 {
        assert!(
            poll!(&mut write_fut).is_pending(),
            "Expected the future to not be completed yet"
        );
    }
    {
        // Write our EOF.
        write_request.write_offset = WRITE_DATA.len() as i64;
        write_request.finish_write = true;
        write_request.data.clear();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    let mut result = None;
    for _ in 0..100 {
        if let Poll::Ready(r) = poll!(&mut write_fut) {
            result = Some(r);
            break;
        }
    }
    {
        // Check our results.
        assert_eq!(
            result
                .err_tip(|| "bs_server.write never returned a value")?
                .err_tip(|| "bs_server.write returned an error")?
                .into_inner(),
            WriteResponse {
                committed_size: WRITE_DATA.len() as i64
            },
            "Expected Responses to match"
        );
    }
    {
        // Check to make sure our store recorded the data properly.
        let digest = DigestInfo::try_new(HASH1, WRITE_DATA.len())?;
        assert_eq!(
            store.get_part_unchunked(digest, 0, None).await?,
            WRITE_DATA,
            "Data written to store did not match expected data",
        );
    }
    Ok(())
}

#[nativelink_test]
pub async fn out_of_order_data_fails() -> Result<(), Box<dyn std::error::Error>> {
    const WRITE_DATA: &str = "12456789abcdefghijk";

    // Chunk our data into two chunks to simulate something a client
    // might do.
    const BYTE_SPLIT_OFFSET: usize = 8;

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );

    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server, Some(CompressionEncoding::Gzip));

    let resource_name = make_resource_name(WRITE_DATA.len());
    let mut write_request = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: false,
        data: vec![].into(),
    };
    {
        // Write first chunk of data.
        write_request.write_offset = 0;
        write_request.data = WRITE_DATA[..BYTE_SPLIT_OFFSET].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    {
        // Write data it already has.
        write_request.write_offset = (BYTE_SPLIT_OFFSET - 1) as i64;
        write_request.data = WRITE_DATA[(BYTE_SPLIT_OFFSET - 1)..].into();
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
    }
    assert!(
        join_handle.await.expect("Failed to join").is_err(),
        "Expected error to be returned"
    );
    {
        // Make sure stream was closed.
        write_request.write_offset = (BYTE_SPLIT_OFFSET - 1) as i64;
        write_request.data = WRITE_DATA[(BYTE_SPLIT_OFFSET - 1)..].into();
        assert!(
            tx.send(Frame::data(encode_stream_proto(&write_request)?))
                .await
                .is_err(),
            "Expected error to be returned"
        );
    }
    Ok(())
}

#[nativelink_test]
pub async fn upload_zero_byte_chunk() -> Result<(), Box<dyn std::error::Error>> {
    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );
    let store = store_manager.get_store("main_cas").unwrap();

    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server, Some(CompressionEncoding::Gzip));

    let resource_name = make_resource_name(0);
    let write_request = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: true,
        data: vec![].into(),
    };

    {
        // Write our zero byte data.
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
        // Wait for stream to finish.
        join_handle
            .await
            .expect("Failed to join")
            .expect("Failed write");
    }
    {
        // Check to make sure our store recorded the data properly.
        let data = store
            .get_part_unchunked(DigestInfo::try_new(HASH1, 0)?, 0, None)
            .await?;
        assert_eq!(data, "", "Expected data to exist and be empty");
    }
    Ok(())
}

#[nativelink_test]
pub async fn disallow_negative_write_offset() -> Result<(), Box<dyn std::error::Error>> {
    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );

    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server, Some(CompressionEncoding::Gzip));

    let resource_name = make_resource_name(0);
    let write_request = WriteRequest {
        resource_name,
        write_offset: -1,
        finish_write: true,
        data: vec![].into(),
    };

    {
        // Write our zero byte data.
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
        // Expect the write command to fail.
        assert!(join_handle.await.expect("Failed to join").is_err());
    }
    Ok(())
}

#[nativelink_test]
pub async fn out_of_sequence_write() -> Result<(), Box<dyn std::error::Error>> {
    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );

    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server, Some(CompressionEncoding::Gzip));

    let resource_name = make_resource_name(100);
    let write_request = WriteRequest {
        resource_name,
        write_offset: 10,
        finish_write: false,
        data: "TEST".into(),
    };

    {
        // Write our zero byte data.
        tx.send(Frame::data(encode_stream_proto(&write_request)?))
            .await?;
        // Expect the write command to fail.
        assert!(join_handle.await.expect("Failed to join").is_err());
    }
    Ok(())
}

#[nativelink_test]
pub async fn chunked_stream_reads_small_set_of_data() -> Result<(), Box<dyn std::error::Error>> {
    const VALUE1: &str = "12456789abcdefghijk";

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );
    let store = store_manager.get_store("main_cas").unwrap();

    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;
    store.update_oneshot(digest, VALUE1.into()).await?;

    let read_request = ReadRequest {
        resource_name: format!("{}/blobs/{}/{}", INSTANCE_NAME, HASH1, VALUE1.len()),
        read_offset: 0,
        read_limit: VALUE1.len() as i64,
    };
    let mut read_stream = bs_server
        .read(Request::new(read_request))
        .await?
        .into_inner();
    {
        let mut roundtrip_data = Vec::with_capacity(VALUE1.len());
        while let Some(result_read_response) = read_stream.next().await {
            roundtrip_data.append(&mut result_read_response?.data.to_vec());
        }
        assert_eq!(
            roundtrip_data,
            VALUE1.as_bytes(),
            "Expected response to match what is in store"
        );
    }
    Ok(())
}

#[nativelink_test]
pub async fn chunked_stream_reads_10mb_of_data() -> Result<(), Box<dyn std::error::Error>> {
    const DATA_SIZE: usize = 10_000_000;

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );
    let store = store_manager.get_store("main_cas").unwrap();

    let mut raw_data = vec![41u8; DATA_SIZE];
    // Change just a few bits to ensure we don't get same packet
    // over and over.
    raw_data[5] = 42u8;
    raw_data[DATA_SIZE - 2] = 43u8;

    let data_len = raw_data.len();
    let digest = DigestInfo::try_new(HASH1, data_len)?;
    store
        .update_oneshot(digest, raw_data.clone().into())
        .await?;

    let read_request = ReadRequest {
        resource_name: format!("{}/blobs/{}/{}", INSTANCE_NAME, HASH1, raw_data.len()),
        read_offset: 0,
        read_limit: raw_data.len() as i64,
    };
    let mut read_stream = bs_server
        .read(Request::new(read_request))
        .await?
        .into_inner();
    {
        let mut roundtrip_data = Vec::with_capacity(raw_data.len());
        assert!(
            !raw_data.is_empty(),
            "Expected at least one byte to be sent"
        );
        while let Some(result_read_response) = read_stream.next().await {
            roundtrip_data.append(&mut result_read_response?.data.to_vec());
        }
        assert_eq!(
            roundtrip_data, raw_data,
            "Expected response to match what is in store"
        );
    }
    Ok(())
}

/// A bug was found in early development where we could deadlock when reading a stream if the
/// store backend resulted in an error. This was because we were not shutting down the stream
/// when on the backend store error which caused the AsyncReader to block forever because the
/// stream was never shutdown.
#[nativelink_test]
pub async fn read_with_not_found_does_not_deadlock() -> Result<(), Error> {
    let store_manager = make_store_manager()
        .await
        .err_tip(|| "Couldn't get store manager")?;
    let mut read_stream = {
        let bs_server = make_bytestream_server(store_manager.as_ref(), None)
            .err_tip(|| "Couldn't make store")?;
        let read_request = ReadRequest {
            resource_name: format!(
                "{}/blobs/{}/{}",
                INSTANCE_NAME,
                HASH1,
                55, // Dummy value
            ),
            read_offset: 0,
            read_limit: 55,
        };
        // This should fail because there's no data in the store yet.
        bs_server
            .read(Request::new(read_request))
            .await
            .err_tip(|| "Couldn't send read")?
            .into_inner()
    };
    // We need to give a chance for the other spawns to do some work before we poll.
    yield_now().await;
    {
        let result_fut = read_stream.next();

        let result = result_fut.await.err_tip(|| "Expected result to be ready")?;
        let expected_err_str = concat!(
                "status: NotFound, message: \"Key Digest(DigestInfo(\\\"0123456789abcdef000000000000000000000000000000000123456789abcdef-55\\\")) not found\", details: [], metadata: MetadataMap { headers: {} }",
            );
        assert_eq!(
            Error::from(result.unwrap_err()),
            make_err!(Code::NotFound, "{expected_err_str}"),
            "Expected error data to match"
        );
    }
    Ok(())
}

#[nativelink_test]
pub async fn test_query_write_status_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    const BYTE_SPLIT_OFFSET: usize = 8;

    let store_manager = make_store_manager()
        .await
        .expect("Failed to make store manager");
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref(), None).expect("Failed to make server"),
    );

    let raw_data = "12456789abcdefghijk".as_bytes();
    let resource_name = make_resource_name(raw_data.len());

    {
        // If the write does not exist it should respond with a 0 size.
        // This is because the client may have tried to write, but the
        // connection closed before the payload had been received.
        let response = bs_server
            .query_write_status(Request::new(QueryWriteStatusRequest {
                resource_name: resource_name.clone(),
            }))
            .await;
        assert_eq!(
            response.unwrap().into_inner(),
            QueryWriteStatusResponse {
                committed_size: 0,
                complete: false,
            }
        );
    }

    // Setup stream.
    let (tx, join_handle) =
        make_stream_and_writer_spawn(bs_server.clone(), Some(CompressionEncoding::Gzip));

    let mut write_request = WriteRequest {
        resource_name: resource_name.clone(),
        write_offset: 0,
        finish_write: false,
        data: vec![].into(),
    };

    // Write first chunk of data.
    write_request.write_offset = 0;
    write_request.data = raw_data[..BYTE_SPLIT_OFFSET].into();
    tx.send(Frame::data(encode_stream_proto(&write_request)?))
        .await?;

    {
        // Check to see if our request is active.
        tokio::task::yield_now().await;
        let data = bs_server
            .query_write_status(Request::new(QueryWriteStatusRequest {
                resource_name: resource_name.clone(),
            }))
            .await?;
        assert_eq!(
            data.into_inner(),
            QueryWriteStatusResponse {
                committed_size: write_request.data.len() as i64,
                complete: false,
            }
        );
    }

    // Finish writing our data.
    write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
    write_request.data = raw_data[BYTE_SPLIT_OFFSET..].into();
    write_request.finish_write = true;
    tx.send(Frame::data(encode_stream_proto(&write_request)?))
        .await?;

    {
        // Now that it's done uploading, ensure it returns a success when requested again.
        tokio::task::yield_now().await;
        let data = bs_server
            .query_write_status(Request::new(QueryWriteStatusRequest { resource_name }))
            .await?;
        assert_eq!(
            data.into_inner(),
            QueryWriteStatusResponse {
                committed_size: raw_data.len() as i64,
                complete: true,
            }
        );
    }
    join_handle
        .await
        .expect("Failed to join")
        .expect("Failed write");
    Ok(())
}

#[nativelink_test]
pub async fn max_decoding_message_size_test() -> Result<(), Box<dyn std::error::Error>> {
    const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB.

    // This is the size of the wrapper proto around the data.
    const WRITE_REQUEST_MSG_WRAPPER_SIZE: usize = 150;

    let store_manager = make_store_manager().await?;
    let config = ByteStreamConfig {
        cas_stores: hashmap! {
            INSTANCE_NAME.to_string() => "main_cas".to_string(),
        },
        max_decoding_message_size: MAX_MESSAGE_SIZE,
        ..Default::default()
    };
    let bs_server = make_bytestream_server(store_manager.as_ref(), Some(config))
        .expect("Failed to make server");
    let (server_join_handle, mut bs_client) = server_and_client_stub(bs_server).await;

    {
        // Test to ensure if we send exactly our max message size, it will succeed.
        let data = Bytes::from(vec![0u8; MAX_MESSAGE_SIZE - WRITE_REQUEST_MSG_WRAPPER_SIZE]);
        let write_request = WriteRequest {
            resource_name: make_resource_name(MAX_MESSAGE_SIZE),
            write_offset: 0,
            finish_write: true,
            data,
        };

        let (tx, rx) = unbounded_channel();
        let rx = UnboundedReceiverStream::new(rx);

        tx.send(write_request).expect("Failed to send data");

        let result = bs_client.write(Request::new(rx)).await;
        assert!(result.is_ok(), "Expected success, got {result:?}");
    }
    {
        // Test to ensure if we send exactly our max message size plus one, it will fail.
        let data = Bytes::from(vec![
            0u8;
            MAX_MESSAGE_SIZE - WRITE_REQUEST_MSG_WRAPPER_SIZE + 1
        ]);
        let write_request = WriteRequest {
            resource_name: make_resource_name(MAX_MESSAGE_SIZE),
            write_offset: 0,
            finish_write: true,
            data,
        };

        let (tx, rx) = unbounded_channel();
        let rx = UnboundedReceiverStream::new(rx);

        tx.send(write_request).expect("Failed to send data");

        let result = bs_client.write(Request::new(rx)).await;
        assert!(result.is_err(), "Expected error, got {result:?}");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Error, decoded message length too large"),
            "Message should be too large message"
        );
    }

    drop(bs_client);
    // Wait for server to shutdown. This should happen when `bs_client` is dropped.
    server_join_handle.await.expect("Failed to join");

    Ok(())
}
