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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::unfold;
use futures::{StreamExt, TryFutureExt};
use nativelink_error::{error_if, make_err, Code, Error, ResultExt};
use nativelink_metric::{MetricsComponent, RootMetricsComponent};
use nativelink_proto::build::bazel::remote::execution::v2::capabilities_client::CapabilitiesClient;
use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ExecuteRequest, ExecutionPolicy, GetCapabilitiesRequest, WaitExecutionRequest,
};
use nativelink_proto::google::longrunning::Operation;
use nativelink_util::action_messages::{
    ActionInfo, ActionState, ActionUniqueQualifier, OperationId, DEFAULT_EXECUTION_PRIORITY,
};
use nativelink_util::connection_manager::ConnectionManager;
use nativelink_util::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, OperationFilter,
};
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::{background_spawn, tls_utils};
use parking_lot::Mutex;
use rand::rngs::OsRng;
use rand::Rng;
use tokio::select;
use tokio::sync::watch;
use tokio::time::sleep;
use tonic::{Request, Streaming};
use tracing::{event, Level};

struct GrpcActionStateResult {
    client_operation_id: OperationId,
    rx: watch::Receiver<Arc<ActionState>>,
}

#[async_trait]
impl ActionStateResult for GrpcActionStateResult {
    async fn as_state(&self) -> Result<Arc<ActionState>, Error> {
        let mut action_state = self.rx.borrow().clone();
        Arc::make_mut(&mut action_state).client_operation_id = self.client_operation_id.clone();
        Ok(action_state)
    }

    async fn changed(&mut self) -> Result<Arc<ActionState>, Error> {
        self.rx.changed().await.map_err(|_| {
            make_err!(
                Code::Internal,
                "Channel closed in GrpcActionStateResult::changed"
            )
        })?;
        let mut action_state = self.rx.borrow().clone();
        Arc::make_mut(&mut action_state).client_operation_id = self.client_operation_id.clone();
        Ok(action_state)
    }

    async fn as_action_info(&self) -> Result<Arc<ActionInfo>, Error> {
        // TODO(allada) We should probably remove as_action_info()
        // or implement it properly.
        return Err(make_err!(
            Code::Unimplemented,
            "as_action_info not implemented for GrpcActionStateResult::as_action_info"
        ));
    }
}

#[derive(MetricsComponent)]
pub struct GrpcScheduler {
    #[metric(group = "property_managers")]
    supported_props: Mutex<HashMap<String, Vec<String>>>,
    retrier: Retrier,
    connection_manager: ConnectionManager,
}

impl GrpcScheduler {
    pub fn new(config: &nativelink_config::schedulers::GrpcSpec) -> Result<Self, Error> {
        let jitter_amt = config.retry.jitter;
        Self::new_with_jitter(
            config,
            Box::new(move |delay: Duration| {
                if jitter_amt == 0. {
                    return delay;
                }
                let min = 1. - (jitter_amt / 2.);
                let max = 1. + (jitter_amt / 2.);
                delay.mul_f32(OsRng.gen_range(min..max))
            }),
        )
    }

    pub fn new_with_jitter(
        config: &nativelink_config::schedulers::GrpcSpec,
        jitter_fn: Box<dyn Fn(Duration) -> Duration + Send + Sync>,
    ) -> Result<Self, Error> {
        let endpoint = tls_utils::endpoint(&config.endpoint)?;
        let jitter_fn = Arc::new(jitter_fn);
        Ok(Self {
            supported_props: Mutex::new(HashMap::new()),
            retrier: Retrier::new(
                Arc::new(|duration| Box::pin(sleep(duration))),
                jitter_fn.clone(),
                config.retry.clone(),
            ),
            connection_manager: ConnectionManager::new(
                std::iter::once(endpoint),
                config.connections_per_endpoint,
                config.max_concurrent_requests,
                config.retry.clone(),
                jitter_fn,
            ),
        })
    }

    async fn perform_request<F, Fut, R, I>(&self, input: I, mut request: F) -> Result<R, Error>
    where
        F: FnMut(I) -> Fut + Send + Copy,
        Fut: Future<Output = Result<R, Error>> + Send,
        R: Send,
        I: Send + Clone,
    {
        self.retrier
            .retry(unfold(input, move |input| async move {
                let input_clone = input.clone();
                Some((
                    request(input_clone)
                        .await
                        .map_or_else(RetryResult::Retry, RetryResult::Ok),
                    input,
                ))
            }))
            .await
    }

    async fn stream_state(
        mut result_stream: Streaming<Operation>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        if let Some(initial_response) = result_stream
            .message()
            .await
            .err_tip(|| "Recieving response from upstream scheduler")?
        {
            let client_operation_id = OperationId::from(initial_response.name.as_str());
            // Our operation_id is not needed here is just a place holder to recycle existing object.
            // The only thing that actually matters is the operation_id.
            let operation_id = OperationId::default();
            let action_state =
                ActionState::try_from_operation(initial_response, operation_id.clone())
                    .err_tip(|| "In GrpcScheduler::stream_state")?;
            let (tx, mut rx) = watch::channel(Arc::new(action_state));
            rx.mark_changed();
            background_spawn!("grpc_scheduler_stream_state", async move {
                loop {
                    select!(
                        _ = tx.closed() => {
                            event!(
                                Level::INFO,
                                "Client disconnected in GrpcScheduler"
                            );
                            return;
                        }
                        response = result_stream.message() => {
                            // When the upstream closes the channel, close the
                            // downstream too.
                            let Ok(Some(response)) = response else {
                                return;
                            };
                            let maybe_action_state = ActionState::try_from_operation(response, operation_id.clone());
                            match maybe_action_state {
                                Ok(response) => {
                                    if let Err(err) = tx.send(Arc::new(response)) {
                                        event!(
                                            Level::INFO,
                                            ?err,
                                            "Client error in GrpcScheduler"
                                        );
                                        return;
                                    }
                                }
                                Err(err) => {
                                    event!(
                                        Level::ERROR,
                                        ?err,
                                        "Error converting response to ActionState in GrpcScheduler"
                                    );
                                },
                            }
                        }
                    );
                }
            });

            return Ok(Box::new(GrpcActionStateResult {
                client_operation_id,
                rx,
            }));
        }
        Err(make_err!(
            Code::Internal,
            "Upstream scheduler didn't accept action."
        ))
    }

    async fn inner_get_known_properties(&self, instance_name: &str) -> Result<Vec<String>, Error> {
        if let Some(supported_props) = self.supported_props.lock().get(instance_name) {
            return Ok(supported_props.clone());
        }

        self.perform_request(instance_name, |instance_name| async move {
            // Not in the cache, lookup the capabilities with the upstream.
            let channel = self
                .connection_manager
                .connection()
                .await
                .err_tip(|| "in get_platform_property_manager()")?;
            let capabilities_result = CapabilitiesClient::new(channel)
                .get_capabilities(GetCapabilitiesRequest {
                    instance_name: instance_name.to_string(),
                })
                .await
                .err_tip(|| "Retrieving upstream GrpcScheduler capabilities");
            let capabilities = capabilities_result?.into_inner();
            let supported_props = capabilities
                .execution_capabilities
                .err_tip(|| "Unable to get execution properties in GrpcScheduler")?
                .supported_node_properties
                .into_iter()
                .collect::<Vec<String>>();

            self.supported_props
                .lock()
                .insert(instance_name.to_string(), supported_props.clone());
            Ok(supported_props)
        })
        .await
    }

    async fn inner_add_action(
        &self,
        _client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        let execution_policy = if action_info.priority == DEFAULT_EXECUTION_PRIORITY {
            None
        } else {
            Some(ExecutionPolicy {
                priority: action_info.priority,
            })
        };
        let skip_cache_lookup = match action_info.unique_qualifier {
            ActionUniqueQualifier::Cachable(_) => false,
            ActionUniqueQualifier::Uncachable(_) => true,
        };
        let request = ExecuteRequest {
            instance_name: action_info.instance_name().clone(),
            skip_cache_lookup,
            action_digest: Some(action_info.digest().into()),
            execution_policy,
            // TODO: Get me from the original request, not very important as we ignore it.
            results_cache_policy: None,
            digest_function: action_info
                .unique_qualifier
                .digest_function()
                .proto_digest_func()
                .into(),
        };
        let result_stream = self
            .perform_request(request, |request| async move {
                let channel = self
                    .connection_manager
                    .connection()
                    .await
                    .err_tip(|| "in add_action()")?;
                ExecutionClient::new(channel)
                    .execute(Request::new(request))
                    .await
                    .err_tip(|| "Sending action to upstream scheduler")
            })
            .await?
            .into_inner();
        Self::stream_state(result_stream).await
    }

    async fn inner_filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream, Error> {
        error_if!(filter != OperationFilter {
            client_operation_id: filter.client_operation_id.clone(),
            ..Default::default()
        }, "Unsupported filter in GrpcScheduler::filter_operations. Only client_operation_id is supported - {filter:?}");
        let client_operation_id = filter.client_operation_id.ok_or_else(|| {
            make_err!(Code::InvalidArgument, "`client_operation_id` is the only supported filter in GrpcScheduler::filter_operations")
        })?;
        let request = WaitExecutionRequest {
            name: client_operation_id.to_string(),
        };
        let result_stream = self
            .perform_request(request, |request| async move {
                let channel = self
                    .connection_manager
                    .connection()
                    .await
                    .err_tip(|| "in find_by_client_operation_id()")?;
                ExecutionClient::new(channel)
                    .wait_execution(Request::new(request))
                    .await
                    .err_tip(|| "While getting wait_execution stream")
            })
            .and_then(|result_stream| Self::stream_state(result_stream.into_inner()))
            .await;
        match result_stream {
            Ok(result_stream) => Ok(unfold(
                Some(result_stream),
                |maybe_result_stream| async move { maybe_result_stream.map(|v| (v, None)) },
            )
            .boxed()),
            Err(err) => {
                event!(
                    Level::WARN,
                    ?err,
                    "Error looking up action with upstream scheduler"
                );
                Ok(futures::stream::empty().boxed())
            }
        }
    }
}

#[async_trait]
impl ClientStateManager for GrpcScheduler {
    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        self.inner_add_action(client_operation_id, action_info)
            .await
    }

    async fn filter_operations<'a>(
        &'a self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream<'a>, Error> {
        self.inner_filter_operations(filter).await
    }

    fn as_known_platform_property_provider(&self) -> Option<&dyn KnownPlatformPropertyProvider> {
        Some(self)
    }
}

#[async_trait]
impl KnownPlatformPropertyProvider for GrpcScheduler {
    async fn get_known_properties(&self, instance_name: &str) -> Result<Vec<String>, Error> {
        self.inner_get_known_properties(instance_name).await
    }
}

impl RootMetricsComponent for GrpcScheduler {}
