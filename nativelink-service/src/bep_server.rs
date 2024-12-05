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
use std::pin::Pin;
use std::sync::Arc;

use bytes::BytesMut;
use futures::stream::unfold;
use futures::Stream;
use nativelink_error::{Error, ResultExt};
use nativelink_proto::google::devtools::build::v1::publish_build_event_server::{
    PublishBuildEvent, PublishBuildEventServer,
};
use nativelink_proto::google::devtools::build::v1::{
    PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
};
use nativelink_store::store_manager::StoreManager;
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey, StoreLike};
use prost::Message;
use tonic::{Request, Response, Result, Status, Streaming};
use tracing::{instrument, Level, event};
use nativelink_util::background_spawn;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;
use nativelink_util::request_metadata_tracer::MetadataEvent;

pub struct BepServer {
    store: Store,
}

impl BepServer {
    pub fn new(
        config: &nativelink_config::cas_server::BepConfig,
        store_manager: &StoreManager,
        metadata_rx: Arc<Mutex<UnboundedReceiver<MetadataEvent>>>
    ) -> Result<Self, Error> {
        let store = store_manager
            .get_store(&config.store)
            .err_tip(|| format!("Expected store {} to exist in store manager", &config.store))?;
        // Cloned store for pushing metadata to redis.
        let metadata_store = store.clone();

        background_spawn!("bep_metadata_event_flusher", async move {
            let mut metadata_rx = metadata_rx.lock().await;
            loop {
                event!(Level::TRACE, "bep_metadata_event_flusher loop");
                tokio::select! {
                    metadata_event = metadata_rx.recv() => {
                        event!(Level::DEBUG, ?metadata_event, "metadata_event received");
                        if let Some(metadata_event) = metadata_event {
                            let metadata = metadata_event.request_metadata_tracer.decode();
                            match metadata {
                                Ok(metadata) => {
                                    let mut buf = BytesMut::new();
                                    let name = &metadata_event.name;
                                    let tool_invocation_id = &metadata.tool_invocation_id;
                                    let action_id = &metadata.action_id;
                                    let correlated_invocations_id = &metadata.correlated_invocations_id;
                                    let action_mnemonic = &metadata.action_mnemonic;
                                    let target_id = &metadata.target_id;
                                    let configuration_id = &metadata.configuration_id;
                                    // Ex: RequestMetadata:3d00ea40-ba20-4897-b5e5-8c0d072e17a9:816e5e02-0e01-4167-8748-a84925c0abb9:capabilities:get_capabilities:::
                                    // Note: This key encoding doesn't work because actions can also contain ":"
                                    // Ex: RequestMetadata:1e76602f-167f-4576-935a-89d9979754f1:ac0c5b4c-0fdf-4cff-bc38-9c315c053c5a:c731e519b64e1c2087870819d8d6e6cc31284ffa8ceb6709e505651b94b82d7d:get_action_result://main:hello-world:e397b2d20257449e54bdd8501d86a350a746c7c6384f524486f141651d7993c8:CppLink
                                    let store_key = StoreKey::Str(Cow::Owned(format!(
                                        "RequestMetadata:{}:{}:{}:{}:{}:{}:{}",
                                        tool_invocation_id,
                                        correlated_invocations_id,
                                        action_id,
                                        name,
                                        target_id,
                                        configuration_id,
                                        action_mnemonic
                                    )));
                                    let encoding_result = metadata.encode(&mut buf);

                                    match encoding_result {
                                        Ok(_) => {
                                            let store_result = metadata_store.update_oneshot(
                                                store_key.clone(),
                                                buf.freeze()
                                            ).await;

                                            match store_result {
                                                Ok(_) => event!(Level::DEBUG, ?store_key, "Successfully stored result"),
                                                Err(err) => event!(Level::ERROR, ?err, ?store_key, "Failed to store result")
                                            }
                                        },
                                        Err(err) => event!(Level::ERROR, ?err, ?metadata, "Failed to encode RequestMetadata buffer")
                                    }
                                },
                                Err(err) => event!(Level::ERROR, ?err, ?metadata_event, "Failed to deserialize metadata")
                            }
                        } else {
                            event!(Level::DEBUG, "metadata_event is empty");
                        }
                    }
                }
            }
        });

        Ok(Self { store })
    }

    pub fn into_service(self) -> PublishBuildEventServer<BepServer> {
        PublishBuildEventServer::new(self)
    }

    async fn inner_publish_lifecycle_event(
        &self,
        request: PublishLifecycleEventRequest,
    ) -> Result<Response<()>, Error> {
        let build_event = request
            .build_event
            .as_ref()
            .err_tip(|| "Expected build_event to be set")?;
        let stream_id = build_event
            .stream_id
            .as_ref()
            .err_tip(|| "Expected stream_id to be set")?;

        let sequence_number = build_event.sequence_number;

        let mut buf = BytesMut::new();
        request
            .encode(&mut buf)
            .err_tip(|| "Could not encode PublishLifecycleEventRequest proto")?;

        self.store
            .update_oneshot(
                StoreKey::Str(Cow::Owned(format!(
                    "LifecycleEvent:{}:{}:{}",
                    &stream_id.build_id, &stream_id.invocation_id, sequence_number,
                ))),
                buf.freeze(),
            )
            .await
            .err_tip(|| "Failed to store PublishLifecycleEventRequest")?;

        Ok(Response::new(()))
    }

    async fn inner_publish_build_tool_event_stream(
        &self,
        stream: Streaming<PublishBuildToolEventStreamRequest>,
    ) -> Result<Response<PublishBuildToolEventStreamStream>, Error> {
        async fn process_request(
            store: Pin<&dyn StoreDriver>,
            request: PublishBuildToolEventStreamRequest,
        ) -> Result<PublishBuildToolEventStreamResponse, Status> {
            let ordered_build_event = request
                .ordered_build_event
                .as_ref()
                .err_tip(|| "Expected ordered_build_event to be set")?;
            let stream_id = ordered_build_event
                .stream_id
                .as_ref()
                .err_tip(|| "Expected stream_id to be set")?;

            let sequence_number = ordered_build_event.sequence_number;

            let mut buf = BytesMut::new();

            request
                .encode(&mut buf)
                .err_tip(|| "Could not encode PublishBuildToolEventStreamRequest proto")?;

            store
                .update_oneshot(
                    StoreKey::Str(Cow::Owned(format!(
                        "BuildToolEventStream:{}:{}:{}",
                        &stream_id.build_id, &stream_id.invocation_id, sequence_number,
                    ))),
                    buf.freeze(),
                )
                .await
                .err_tip(|| "Failed to store PublishBuildToolEventStreamRequest")?;

            Ok(PublishBuildToolEventStreamResponse {
                stream_id: Some(stream_id.clone()),
                sequence_number,
            })
        }

        struct State {
            store: Store,
            stream: Streaming<PublishBuildToolEventStreamRequest>,
        }

        let response_stream =
            unfold(
                Some(State {
                    store: self.store.clone(),
                    stream,
                }),
                move |maybe_state| async move {
                    let mut state = maybe_state?;
                    let request =
                        match state.stream.message().await.err_tip(|| {
                            "While receiving message in publish_build_tool_event_stream"
                        }) {
                            Ok(Some(request)) => request,
                            Ok(None) => return None,
                            Err(e) => return Some((Err(e.into()), None)),
                        };
                    process_request(state.store.as_store_driver_pin(), request)
                        .await
                        .map_or_else(
                            |e| Some((Err(e), None)),
                            |response| Some((Ok(response), Some(state))),
                        )
                },
            );

        Ok(Response::new(Box::pin(response_stream)))
    }
}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

#[tonic::async_trait]
impl PublishBuildEvent for BepServer {
    type PublishBuildToolEventStreamStream = PublishBuildToolEventStreamStream;

    #[allow(clippy::blocks_in_conditions)]
    #[instrument(
        err,
        ret(level = Level::INFO),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn publish_lifecycle_event(
        &self,
        grpc_request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        self.inner_publish_lifecycle_event(grpc_request.into_inner())
            .await
            .map_err(Error::into)
    }

    #[allow(clippy::blocks_in_conditions)]
    #[instrument(
      err,
      level = Level::ERROR,
      skip_all,
      fields(request = ?grpc_request.get_ref())
    )]
    async fn publish_build_tool_event_stream(
        &self,
        grpc_request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<Self::PublishBuildToolEventStreamStream>, Status> {
        self.inner_publish_build_tool_event_stream(grpc_request.into_inner())
            .await
            .map_err(Error::into)
    }
}
