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
use std::sync::Arc;
use std::time::UNIX_EPOCH;

mod utils {
    pub(crate) mod mock_scheduler;
    pub(crate) mod scheduler_utils;
}

use futures::{join, StreamExt};
use nativelink_config::schedulers::{
    PlatformPropertyAddition, PropertyModification, SchedulerRef, SimpleSpec,
};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::property_modifier_scheduler::PropertyModifierScheduler;
use nativelink_util::action_messages::{ActionStage, ActionState, OperationId};
use nativelink_util::common::DigestInfo;
use nativelink_util::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::operation_state_manager::{ClientStateManager, OperationFilter};
use pretty_assertions::assert_eq;
use tokio::sync::watch;
use utils::mock_scheduler::MockActionScheduler;
use utils::scheduler_utils::{make_base_action_info, TokioWatchActionStateResult, INSTANCE_NAME};

struct TestContext {
    mock_scheduler: Arc<MockActionScheduler>,
    modifier_scheduler: PropertyModifierScheduler,
}

fn make_modifier_scheduler(modifications: Vec<PropertyModification>) -> TestContext {
    let mock_scheduler = Arc::new(MockActionScheduler::new());
    let config = nativelink_config::schedulers::PropertyModifierSpec {
        modifications,
        scheduler: SchedulerRef::new("simple", SimpleSpec::default()),
    };
    let modifier_scheduler = PropertyModifierScheduler::new(&config, mock_scheduler.clone());
    TestContext {
        mock_scheduler,
        modifier_scheduler,
    }
}

#[nativelink_test]
async fn add_action_adds_property() -> Result<(), Error> {
    let name = "name".to_string();
    let value = "value".to_string();
    let context =
        make_modifier_scheduler(vec![PropertyModification::add(PlatformPropertyAddition {
            name: name.clone(),
            value: value.clone(),
        })]);
    let action_info = make_base_action_info(UNIX_EPOCH, DigestInfo::zero_digest());
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            client_operation_id: OperationId::default(),
            stage: ActionStage::Queued,
            action_digest: action_info.unique_qualifier.digest(),
        }));
    let client_operation_id = OperationId::default();
    let (_, (passed_client_operation_id, action_info)) = join!(
        context
            .modifier_scheduler
            .add_action(client_operation_id.clone(), action_info.clone()),
        context
            .mock_scheduler
            .expect_add_action(Ok(Box::new(TokioWatchActionStateResult::new(
                client_operation_id.clone(),
                action_info,
                forward_watch_channel_rx
            )))),
    );
    assert_eq!(client_operation_id, passed_client_operation_id);
    assert_eq!(
        HashMap::from([(name, value)]),
        action_info.platform_properties
    );
    Ok(())
}

#[nativelink_test]
async fn add_action_overwrites_property() -> Result<(), Error> {
    let name = "name".to_string();
    let original_value = "value".to_string();
    let replaced_value = "replaced".to_string();
    let context =
        make_modifier_scheduler(vec![PropertyModification::add(PlatformPropertyAddition {
            name: name.clone(),
            value: replaced_value.clone(),
        })]);
    let mut action_info = make_base_action_info(UNIX_EPOCH, DigestInfo::zero_digest())
        .as_ref()
        .clone();
    action_info
        .platform_properties
        .insert(name.clone(), original_value);
    let action_info = Arc::new(action_info);
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            client_operation_id: OperationId::default(),
            stage: ActionStage::Queued,
            action_digest: action_info.unique_qualifier.digest(),
        }));
    let client_operation_id = OperationId::default();
    let (_, (passed_client_operation_id, action_info)) = join!(
        context
            .modifier_scheduler
            .add_action(client_operation_id.clone(), action_info.clone()),
        context
            .mock_scheduler
            .expect_add_action(Ok(Box::new(TokioWatchActionStateResult::new(
                client_operation_id.clone(),
                action_info,
                forward_watch_channel_rx
            )))),
    );
    assert_eq!(client_operation_id, passed_client_operation_id);
    assert_eq!(
        HashMap::from([(name, replaced_value)]),
        action_info.platform_properties
    );
    Ok(())
}

#[nativelink_test]
async fn add_action_property_added_after_remove() -> Result<(), Error> {
    let name = "name".to_string();
    let value = "value".to_string();
    let context = make_modifier_scheduler(vec![
        PropertyModification::remove(name.clone()),
        PropertyModification::add(PlatformPropertyAddition {
            name: name.clone(),
            value: value.clone(),
        }),
    ]);
    let action_info = make_base_action_info(UNIX_EPOCH, DigestInfo::zero_digest());
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            client_operation_id: OperationId::default(),
            stage: ActionStage::Queued,
            action_digest: action_info.unique_qualifier.digest(),
        }));
    let client_operation_id = OperationId::default();
    let (_, (passed_client_operation_id, action_info)) = join!(
        context
            .modifier_scheduler
            .add_action(client_operation_id.clone(), action_info.clone()),
        context
            .mock_scheduler
            .expect_add_action(Ok(Box::new(TokioWatchActionStateResult::new(
                client_operation_id.clone(),
                action_info,
                forward_watch_channel_rx
            )))),
    );
    assert_eq!(client_operation_id, passed_client_operation_id);
    assert_eq!(
        HashMap::from([(name, value)]),
        action_info.platform_properties
    );
    Ok(())
}

#[nativelink_test]
async fn add_action_property_remove_after_add() -> Result<(), Error> {
    let name = "name".to_string();
    let value = "value".to_string();
    let context = make_modifier_scheduler(vec![
        PropertyModification::add(PlatformPropertyAddition {
            name: name.clone(),
            value: value.clone(),
        }),
        PropertyModification::remove(name.clone()),
    ]);
    let action_info = make_base_action_info(UNIX_EPOCH, DigestInfo::zero_digest());
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            client_operation_id: OperationId::default(),
            stage: ActionStage::Queued,
            action_digest: action_info.unique_qualifier.digest(),
        }));
    // let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::from([(
    //     name,
    //     PropertyType::exact,
    // )])));
    let client_operation_id = OperationId::default();
    let (_, (passed_client_operation_id, action_info)) = join!(
        context
            .modifier_scheduler
            .add_action(client_operation_id.clone(), action_info.clone()),
        context
            .mock_scheduler
            .expect_add_action(Ok(Box::new(TokioWatchActionStateResult::new(
                client_operation_id.clone(),
                action_info,
                forward_watch_channel_rx
            )))),
    );
    assert_eq!(client_operation_id, passed_client_operation_id);
    assert_eq!(HashMap::from([]), action_info.platform_properties);
    Ok(())
}

#[nativelink_test]
async fn add_action_property_remove() -> Result<(), Error> {
    let name = "name".to_string();
    let value = "value".to_string();
    let context = make_modifier_scheduler(vec![PropertyModification::remove(name.clone())]);
    let mut action_info = make_base_action_info(UNIX_EPOCH, DigestInfo::zero_digest())
        .as_ref()
        .clone();
    action_info.platform_properties.insert(name, value);
    let action_info = Arc::new(action_info);
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            client_operation_id: OperationId::default(),
            stage: ActionStage::Queued,
            action_digest: action_info.unique_qualifier.digest(),
        }));
    // let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let client_operation_id = OperationId::default();
    let (_, (passed_client_operation_id, action_info)) = join!(
        context
            .modifier_scheduler
            .add_action(client_operation_id.clone(), action_info.clone()),
        context
            .mock_scheduler
            .expect_add_action(Ok(Box::new(TokioWatchActionStateResult::new(
                client_operation_id.clone(),
                action_info,
                forward_watch_channel_rx
            )))),
    );
    assert_eq!(client_operation_id, passed_client_operation_id);
    assert_eq!(HashMap::from([]), action_info.platform_properties);
    Ok(())
}

#[nativelink_test]
async fn find_by_client_operation_id_call_passed() -> Result<(), Error> {
    let context = make_modifier_scheduler(vec![]);
    let client_operation_id = OperationId::default();
    let (actual_result, actual_filter) = join!(
        context
            .modifier_scheduler
            .filter_operations(OperationFilter {
                client_operation_id: Some(client_operation_id.clone()),
                ..Default::default()
            }),
        context
            .mock_scheduler
            .expect_filter_operations(Ok(Box::pin(futures::stream::empty()))),
    );
    assert_eq!(true, actual_result.unwrap().next().await.is_none());
    assert_eq!(
        OperationFilter {
            client_operation_id: Some(client_operation_id),
            ..Default::default()
        },
        actual_filter
    );
    Ok(())
}

#[nativelink_test]
async fn remove_adds_to_underlying_manager() -> Result<(), Error> {
    let name = "name".to_string();
    let context = make_modifier_scheduler(vec![PropertyModification::remove(name.clone())]);
    let known_properties = Vec::new();
    let instance_name_fut = context
        .mock_scheduler
        .expect_get_known_properties(Ok(known_properties));
    let known_props_fut = context
        .modifier_scheduler
        .get_known_properties(INSTANCE_NAME);
    let (actual_instance_name, known_props) = join!(instance_name_fut, known_props_fut);
    assert_eq!(Ok(vec![name]), known_props);
    assert_eq!(actual_instance_name, INSTANCE_NAME);
    Ok(())
}

#[nativelink_test]
async fn remove_retains_type_in_underlying_manager() -> Result<(), Error> {
    let name = "name".to_string();
    let context = make_modifier_scheduler(vec![PropertyModification::remove(name.clone())]);
    let known_properties = vec![name.clone()];
    let instance_name_fut = context
        .mock_scheduler
        .expect_get_known_properties(Ok(known_properties));
    let known_props_fut = context
        .modifier_scheduler
        .get_known_properties(INSTANCE_NAME);
    let (_, known_props) = join!(instance_name_fut, known_props_fut);
    assert_eq!(Ok(vec![name]), known_props);
    Ok(())
}
