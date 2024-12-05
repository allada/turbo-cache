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
use std::convert::Into;
use std::fmt::Debug;

use bytes::BytesMut;
use nativelink_config::cas_server::{AcStoreConfig, InstanceName};
use nativelink_error::{make_err, make_input_err, Code, Error, ResultExt};
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_server::{
    ActionCache, ActionCacheServer as Server,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, GetActionResultRequest, UpdateActionResultRequest,
};
use nativelink_store::ac_utils::{get_and_decode_digest, ESTIMATED_DIGEST_SIZE};
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::make_ctx_for_hash_func;
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{error_span, event, instrument, Level};
use tokio::sync::mpsc::UnboundedSender;
use nativelink_util::request_metadata_tracer::MetadataEvent;

#[derive(Clone)]
pub struct AcStoreInfo {
    store: Store,
    read_only: bool,
}

pub struct AcServer {
    stores: HashMap<String, AcStoreInfo>,
    metadata_tx: Option<UnboundedSender<MetadataEvent>>
}

impl Debug for AcServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcServer").finish()
    }
}

impl AcServer {
    pub fn new(
        config: &HashMap<InstanceName, AcStoreConfig>,
        store_manager: &StoreManager,
        metadata_tx: UnboundedSender<MetadataEvent>
    ) -> Result<Self, Error> {
        let mut stores = HashMap::with_capacity(config.len());
        for (instance_name, ac_cfg) in config {
            let store = store_manager.get_store(&ac_cfg.ac_store).ok_or_else(|| {
                make_input_err!("'ac_store': '{}' does not exist", ac_cfg.ac_store)
            })?;
            stores.insert(
                instance_name.to_string(),
                AcStoreInfo {
                    store,
                    read_only: ac_cfg.read_only,
                },
            );
        }
        Ok(AcServer {
            stores: stores.clone(),
            metadata_tx: Some(metadata_tx.clone())
        })
    }

    pub fn into_service(self) -> Server<AcServer> {
        Server::new(self)
    }

    async fn inner_get_action_result(
        &self,
        request: GetActionResultRequest,
    ) -> Result<Response<ActionResult>, Error> {
        let instance_name = &request.instance_name;
        let store_info = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        // TODO(blaise.bruer) We should write a test for these errors.
        let digest: DigestInfo = request
            .action_digest
            .clone()
            .err_tip(|| "Action digest was not set in message")?
            .try_into()?;

        // If we are a GrpcStore we shortcut here, as this is a special store.
        if let Some(grpc_store) = store_info
            .store
            .downcast_ref::<GrpcStore>(Some(digest.into()))
        {
            return grpc_store.get_action_result(Request::new(request)).await;
        }

        Ok(Response::new(
            get_and_decode_digest::<ActionResult>(&store_info.store, digest.into()).await?,
        ))
    }

    async fn inner_update_action_result(
        &self,
        request: UpdateActionResultRequest,
    ) -> Result<Response<ActionResult>, Error> {
        let instance_name = &request.instance_name;
        let store_info = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        if store_info.read_only {
            return Err(make_err!(
                Code::PermissionDenied,
                "The store '{instance_name}' is read only on this endpoint",
            ));
        }

        let digest: DigestInfo = request
            .action_digest
            .clone()
            .err_tip(|| "Action digest was not set in message")?
            .try_into()?;

        // If we are a GrpcStore we shortcut here, as this is a special store.
        if let Some(grpc_store) = store_info
            .store
            .downcast_ref::<GrpcStore>(Some(digest.into()))
        {
            return grpc_store.update_action_result(Request::new(request)).await;
        }

        let action_result = request
            .action_result
            .err_tip(|| "Action result was not set in message")?;

        let mut store_data = BytesMut::with_capacity(ESTIMATED_DIGEST_SIZE);
        action_result
            .encode(&mut store_data)
            .err_tip(|| "Provided ActionResult could not be serialized")?;

        store_info
            .store
            .update_oneshot(digest, store_data.freeze())
            .await
            .err_tip(|| "Failed to update in action cache")?;
        Ok(Response::new(action_result))
    }
}

#[tonic::async_trait]
impl ActionCache for AcServer {
    #[allow(clippy::blocks_in_conditions)]
    #[instrument(
        ret(level = Level::INFO),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn get_action_result(
        &self,
        grpc_request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let inner_get_action_result = |grpc_request: Request<GetActionResultRequest>| async {
            let request = grpc_request.into_inner();
            let resp = make_ctx_for_hash_func(request.digest_function)
                .err_tip(|| "In AcServer::get_action_result")?
                .wrap_async(
                    error_span!("ac_server_get_action_result"),
                    self.inner_get_action_result(request),
                )
                .await;

            // let resp = self.inner_get_action_result(grpc_request).await;
            if resp.is_err() && resp.as_ref().err().unwrap().code != Code::NotFound {
                event!(Level::ERROR, return = ?resp);
            }
            return resp.map_err(Into::into);
        };

        // DANGER DANGER WILL ROBINSON
        // An option was used to avoid the Default derive macro
        let metadata_tx = &self.metadata_tx.as_ref().unwrap().clone();
        wrap_with_metadata_tracing!(
            "get_action_result",
            inner_get_action_result,
            grpc_request,
            metadata_tx
        )
    }

    #[allow(clippy::blocks_in_conditions)]
    #[instrument(
        err,
        ret(level = Level::INFO),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn update_action_result(
        &self,
        grpc_request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let inner_update_action_result = |grpc_request: Request<UpdateActionResultRequest>| async {
            let request = grpc_request.into_inner();
            make_ctx_for_hash_func(request.digest_function)
                .err_tip(|| "In AcServer::update_action_result")?
                .wrap_async(
                    error_span!("ac_server_update_action_result"),
                    self.inner_update_action_result(request),
                )
                .await
                .map_err(Into::into)
        };

        // DANGER DANGER WILL ROBINSON
        // An option was used to avoid the Default derive macro
        let metadata_tx = &self.metadata_tx.as_ref().unwrap().clone();
        wrap_with_metadata_tracing!(
            "update_action_result",
            inner_update_action_result,
            grpc_request,
            metadata_tx
        )
    }
}
