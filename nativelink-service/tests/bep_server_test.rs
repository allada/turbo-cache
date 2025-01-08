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
use std::sync::Arc;

use futures::StreamExt;
use hyper::body::Frame;
use nativelink_config::cas_server::BepConfig;
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::events::{bep_event, BepEvent};
use nativelink_proto::google::devtools::build::v1::build_event::console_output::Output;
use nativelink_proto::google::devtools::build::v1::build_event::{
    BuildEnqueued, BuildFinished, ConsoleOutput, Event, InvocationAttemptFinished,
    InvocationAttemptStarted,
};
use nativelink_proto::google::devtools::build::v1::publish_build_event_server::PublishBuildEvent;
use nativelink_proto::google::devtools::build::v1::publish_lifecycle_event_request::ServiceLevel;
use nativelink_proto::google::devtools::build::v1::stream_id::BuildComponent;
use nativelink_proto::google::devtools::build::v1::{
    build_status, BuildEvent, BuildStatus, ConsoleOutputStream, OrderedBuildEvent,
    PublishBuildToolEventStreamRequest, PublishLifecycleEventRequest, StreamId,
};
use nativelink_service::bep_server::BepServer;
use nativelink_store::default_store_factory::make_and_add_store_to_manager;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::encode_stream_proto;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use pretty_assertions::assert_eq;
use prost::Message;
use prost_types::Timestamp;
use tonic::codec::{Codec, ProstCodec};
use tonic::{Request, Streaming};

const BEP_STORE_NAME: &str = "main_bep";

/// Utility function to construct a [`StoreManager`]
async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    make_and_add_store_to_manager(
        BEP_STORE_NAME,
        &StoreSpec::memory(MemorySpec::default()),
        &store_manager,
        None,
    )
    .await?;

    Ok(store_manager)
}

/// Utility function to construct a [`BepServer`]
fn make_bep_server(store_manager: &StoreManager) -> Result<BepServer, Error> {
    BepServer::new(
        &BepConfig {
            store: BEP_STORE_NAME.to_string(),
        },
        store_manager,
    )
}

fn get_bep_store(store_manager: &StoreManager) -> Result<Store, Error> {
    store_manager
        .get_store(BEP_STORE_NAME)
        .err_tip(|| format!("While retrieving bep_store {BEP_STORE_NAME}"))
}

/// Asserts that a gRPC request for a [`PublishLifecycleEventRequest`] is correctly dumped into a [`Store`]
#[nativelink_test]
async fn publish_lifecycle_event_test() -> Result<(), Box<dyn std::error::Error>> {
    let store_manager = make_store_manager().await?;
    let bep_server = make_bep_server(&store_manager)?;
    let bep_store = get_bep_store(&store_manager)?;

    let stream_id = StreamId {
        build_id: "some-build-id".to_string(),
        invocation_id: "some-invocation-id".to_string(),
        component: BuildComponent::Controller as i32,
    };

    let request = PublishLifecycleEventRequest {
        service_level: ServiceLevel::Interactive as i32,
        build_event: Some(OrderedBuildEvent {
            stream_id: Some(stream_id.clone()),
            sequence_number: 1,
            event: Some(BuildEvent {
                event_time: Some(Timestamp::date(1999, 1, 6)?),
                event: Some(Event::ConsoleOutput(ConsoleOutput {
                    r#type: ConsoleOutputStream::Stdout as i32,
                    output: Some(Output::TextOutput(
                        "Here's some text that's been printed to stdout".to_string(),
                    )),
                })),
            }),
        }),
        stream_timeout: None,
        notification_keywords: vec!["testing".to_string(), "console".to_string()],
        project_id: "some-project-id".to_string(),
        check_preceding_lifecycle_events_present: false,
    };

    let sequence_number = request.clone().build_event.unwrap().sequence_number;

    let store_key = StoreKey::Str(Cow::Owned(format!(
        "BepEvent:le:{}:{}:{}",
        stream_id.clone().build_id,
        stream_id.clone().invocation_id,
        sequence_number
    )));

    bep_server
        .publish_lifecycle_event(Request::new(request.clone()))
        .await
        .err_tip(|| "While invoking publish_lifecycle_event")?;

    let (mut writer, mut reader) = make_buf_channel_pair();

    bep_store
        .get_part(store_key, &mut writer, 0, None)
        .await
        .err_tip(|| "While retrieving lifecycle_event_request from store")?;

    let bytes = reader
        .recv()
        .await
        .err_tip(|| "While receiving bytes from reader")?;

    let decoded_request =
        BepEvent::decode(bytes).err_tip(|| "While decoding request from bytes")?;

    assert_eq!(
        BepEvent {
            version: 0,
            identity: String::new(),
            event: Some(bep_event::Event::LifecycleEvent(request.clone())),
        },
        decoded_request,
    );

    Ok(())
}

#[nativelink_test]
async fn publish_build_tool_event_stream_test() -> Result<(), Box<dyn std::error::Error>> {
    let store_manager = make_store_manager().await?;
    let bep_server = make_bep_server(&store_manager)?;
    let bep_store = get_bep_store(&store_manager)?;

    let (request_tx, mut response_stream) = async {
        // Set up the request and response streams.
        let (tx, body) = ChannelBody::new();
        let mut codec = ProstCodec::<PublishBuildToolEventStreamRequest, _>::default();
        let stream = Streaming::new_request(codec.decoder(), body, None, None);
        let stream = bep_server
            .publish_build_tool_event_stream(Request::new(stream))
            .await
            .err_tip(|| "While invoking publish_build_tool_event_stream")?
            .into_inner();

        Ok::<_, Box<dyn std::error::Error>>((tx, stream))
    }
    .await?;

    let (requests, store_keys) = {
        // Construct some requests to send off and a store key to retrieve them with.
        let stream_id = StreamId {
            build_id: "some-build-id".to_string(),
            invocation_id: "some-invocation-id".to_string(),
            component: BuildComponent::Controller as i32,
        };
        let project_id = "some-project-id".to_string();

        let requests = [
            PublishBuildToolEventStreamRequest {
                ordered_build_event: Some(OrderedBuildEvent {
                    stream_id: Some(stream_id.clone()),
                    sequence_number: 1,
                    event: Some(BuildEvent {
                        event_time: Some(Timestamp::date(1999, 1, 4)?),
                        event: Some(Event::BuildEnqueued(BuildEnqueued { details: None })),
                    }),
                }),
                notification_keywords: vec!["testing".to_string(), "build-enqueued".to_string()],
                project_id: project_id.clone(),
                check_preceding_lifecycle_events_present: false,
            },
            PublishBuildToolEventStreamRequest {
                ordered_build_event: Some(OrderedBuildEvent {
                    stream_id: Some(stream_id.clone()),
                    sequence_number: 2,
                    event: Some(BuildEvent {
                        event_time: Some(Timestamp::date(1999, 1, 5)?),
                        event: Some(Event::InvocationAttemptStarted(InvocationAttemptStarted {
                            attempt_number: 1,
                            details: None,
                        })),
                    }),
                }),
                notification_keywords: vec!["testing".to_string()],
                project_id: project_id.clone(),
                check_preceding_lifecycle_events_present: false,
            },
            PublishBuildToolEventStreamRequest {
                ordered_build_event: Some(OrderedBuildEvent {
                    stream_id: Some(stream_id.clone()),
                    sequence_number: 3,
                    event: Some(BuildEvent {
                        event_time: Some(Timestamp::date(1999, 1, 6)?),
                        event: Some(Event::ConsoleOutput(ConsoleOutput {
                            r#type: ConsoleOutputStream::Stdout as i32,
                            output: Some(Output::TextOutput(
                                "This is taking a while...".to_string(),
                            )),
                        })),
                    }),
                }),
                notification_keywords: vec!["testing".to_string()],
                project_id: project_id.clone(),
                check_preceding_lifecycle_events_present: false,
            },
            PublishBuildToolEventStreamRequest {
                ordered_build_event: Some(OrderedBuildEvent {
                    stream_id: Some(stream_id.clone()),
                    sequence_number: 4,
                    event: Some(BuildEvent {
                        event_time: Some(Timestamp::date(1999, 1, 7)?),
                        event: Some(Event::InvocationAttemptFinished(
                            InvocationAttemptFinished {
                                invocation_status: Some(BuildStatus {
                                    result: build_status::Result::InvocationDeadlineExceeded as i32,
                                    final_invocation_id: String::default(),
                                    build_tool_exit_code: Some(1),
                                    error_message: "You missed my birthday!".to_string(),
                                    details: None,
                                }),
                                details: None,
                            },
                        )),
                    }),
                }),
                notification_keywords: vec!["testing".to_string()],
                project_id: "some-project-id".to_string(),
                check_preceding_lifecycle_events_present: false,
            },
            PublishBuildToolEventStreamRequest {
                ordered_build_event: Some(OrderedBuildEvent {
                    stream_id: Some(stream_id.clone()),
                    sequence_number: 5,
                    event: Some(BuildEvent {
                        event_time: Some(Timestamp::date(1999, 1, 8)?),
                        event: Some(Event::BuildFinished(BuildFinished {
                            status: Some(BuildStatus {
                                result: build_status::Result::InvocationDeadlineExceeded as i32,
                                final_invocation_id: String::default(),
                                build_tool_exit_code: Some(1),
                                error_message: "Missed her birthday...".to_string(),
                                details: None,
                            }),
                            details: None,
                        })),
                    }),
                }),
                notification_keywords: vec!["testing".to_string()],
                project_id: project_id.clone(),
                check_preceding_lifecycle_events_present: false,
            },
        ];

        (
            requests.clone(),
            requests
                .iter()
                .map(|request| {
                    StoreKey::Str(Cow::Owned(format!(
                        "BepEvent:be:{}:{}:{}",
                        stream_id.build_id,
                        stream_id.invocation_id,
                        request
                            .ordered_build_event
                            .as_ref()
                            .unwrap()
                            .sequence_number
                    )))
                })
                .collect::<Vec<_>>(),
        )
    };

    {
        // Send off the requests and validate the responses.
        for (sequence_number, request) in requests.iter().enumerate().map(|(i, req)| {
            // Sequence numbers are 1-indexed, while `.enumerate()` indexes from 0.
            (i as i64 + 1, req)
        }) {
            let encoded_request = encode_stream_proto(request)?;
            request_tx.send(Frame::data(encoded_request)).await?;

            let response = response_stream
                .next()
                .await
                .err_tip(|| "Response stream closed unexpectedly")?
                .err_tip(|| "While awaiting next PublishBuildToolEventStreamResponse")?;

            // First, check if the response matches what we expect.
            assert_eq!(response.sequence_number, sequence_number);

            assert_eq!(
                response.stream_id,
                request
                    .ordered_build_event
                    .as_ref()
                    .and_then(|evt| evt.stream_id.clone())
            );

            // Second, check if the message was forwarded correctly.
            let (mut writer, mut reader) = make_buf_channel_pair();
            bep_store
                .get_part(
                    store_keys[sequence_number as usize - 1].clone(),
                    &mut writer,
                    0,
                    None,
                )
                .await?;
            let encoded_request = reader.recv().await?;

            let decoded_request = BepEvent::decode(encoded_request)?;

            assert_eq!(
                BepEvent {
                    version: 0,
                    identity: String::new(),
                    event: Some(bep_event::Event::BuildToolEvent(request.clone())),
                },
                decoded_request
            );
        }

        Ok(())
    }
}
