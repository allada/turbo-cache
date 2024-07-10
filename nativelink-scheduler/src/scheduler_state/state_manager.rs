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

use std::cmp;
use std::collections::{BTreeSet, VecDeque};
use std::ops::Bound;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime};

use async_lock::Mutex;
use async_trait::async_trait;
use futures::stream::{self, unfold};
use hashbrown::HashMap;
use nativelink_config::stores::EvictionPolicy;
use nativelink_error::{make_err, make_input_err, Code, Error, ResultExt};
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionState, ActionUniqueKey, ActionUniqueQualifier,
    ClientOperationId, ExecutionMetadata, OperationId, WorkerId,
};
use nativelink_util::evicting_map::{EvictingMap, LenEntry};
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use tokio::sync::{mpsc, watch, Notify};
use tracing::{event, Level};

use super::awaited_action::AwaitedActionSortKey;
use crate::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, MatchingEngineStateManager,
    OperationFilter, OperationStageFlags, OrderDirection, WorkerStateManager,
};
use crate::scheduler_state::awaited_action::AwaitedAction;
use crate::scheduler_state::client_action_state_result::ClientActionStateResult;
use crate::scheduler_state::matching_engine_action_state_result::MatchingEngineActionStateResult;

/// How often the owning database will have the AwaitedAction touched
/// to keep it from being evicted.
const KEEPALIVE_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
struct SortedAwaitedAction {
    sort_key: AwaitedActionSortKey,
    awaited_action: Arc<AwaitedAction>,
}

impl PartialEq for SortedAwaitedAction {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.awaited_action, &other.awaited_action) && self.sort_key == other.sort_key
    }
}

impl Eq for SortedAwaitedAction {}

impl PartialOrd for SortedAwaitedAction {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortedAwaitedAction {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.sort_key.cmp(&other.sort_key).then_with(|| {
            Arc::as_ptr(&self.awaited_action).cmp(&Arc::as_ptr(&other.awaited_action))
        })
    }
}

#[derive(Default)]
struct SortedAwaitedActions {
    unknown: BTreeSet<SortedAwaitedAction>,
    cache_check: BTreeSet<SortedAwaitedAction>,
    queued: BTreeSet<SortedAwaitedAction>,
    executing: BTreeSet<SortedAwaitedAction>,
    completed: BTreeSet<SortedAwaitedAction>,
    completed_from_cache: BTreeSet<SortedAwaitedAction>,
}

/// Represents a client that is currently listening to an action.
/// When the client is dropped, it will send the [`AwaitedAction`] to the
/// `client_operation_drop_tx` if there are other cleanups needed.
#[derive(Debug)]
struct ClientAwaitedAction {
    /// The awaited action that the client is listening to.
    // Note: This is an Option because it is taken when the
    // ClientAwaitedAction is dropped, but will never actually be
    // None except during the drop.
    awaited_action: Option<Arc<AwaitedAction>>,

    /// The sender to notify of this struct being dropped.
    client_operation_drop_tx: mpsc::UnboundedSender<Arc<AwaitedAction>>,
}

impl ClientAwaitedAction {
    fn new(
        awaited_action: Arc<AwaitedAction>,
        client_operation_drop_tx: mpsc::UnboundedSender<Arc<AwaitedAction>>,
    ) -> Self {
        awaited_action.inc_listening_clients();
        Self {
            awaited_action: Some(awaited_action),
            client_operation_drop_tx,
        }
    }

    /// Returns the awaited action that the client is listening to.
    fn awaited_action(&self) -> &Arc<AwaitedAction> {
        self.awaited_action
            .as_ref()
            .expect("AwaitedAction should be present")
    }
}

impl Drop for ClientAwaitedAction {
    fn drop(&mut self) {
        let awaited_action = self
            .awaited_action
            .take()
            .expect("AwaitedAction should be present");
        awaited_action.dec_listening_clients();
        // If we failed to send it means noone is listening.
        let _ = self.client_operation_drop_tx.send(awaited_action);
    }
}

/// Trait to be able to use the [`EvictingMap`] with [`ClientAwaitedAction`].
/// Note: We only use [`EvictingMap`] for a time based evictiong, which is
/// why the implementation has fixed default values in it.
impl LenEntry for ClientAwaitedAction {
    #[inline]
    fn len(&self) -> usize {
        0
    }

    #[inline]
    fn is_empty(&self) -> bool {
        true
    }
}

/// The database for storing the state of all actions.
/// IMPORTANT: Any time an item is removed from
/// [`AwaitedActionDb::client_operation_to_awaited_action`], it must
/// also remove the entries from all the other maps.
pub struct AwaitedActionDb {
    /// A lookup table to lookup the state of an action by its client operation id.
    client_operation_to_awaited_action:
        EvictingMap<ClientOperationId, Arc<ClientAwaitedAction>, SystemTime>,

    /// A lookup table to lookup the state of an action by its worker operation id.
    operation_id_to_awaited_action: HashMap<OperationId, Arc<AwaitedAction>>,

    /// A lookup table to lookup the state of an action by its unique qualifier.
    action_info_hash_key_to_awaited_action: HashMap<ActionUniqueKey, Arc<AwaitedAction>>,

    /// A sorted set of [`AwaitedAction`]s. A wrapper is used to perform sorting
    /// based on the [`AwaitedActionSortKey`] of the [`AwaitedAction`].
    ///
    /// See [`AwaitedActionSortKey`] for more information on the ordering.
    sorted_action_info_hash_keys: SortedAwaitedActions,
}

#[allow(clippy::mutable_key_type)]
impl AwaitedActionDb {
    /// Refreshes/Updates the time to live of the [`ClientOperationId`] in
    /// the [`EvictingMap`] by touching the key.
    async fn refresh_client_operation_id(&self, client_operation_id: &ClientOperationId) -> bool {
        self.client_operation_to_awaited_action
            .size_for_key(client_operation_id)
            .await
            .is_some()
    }

    async fn get_by_client_operation_id(
        &self,
        client_operation_id: &ClientOperationId,
    ) -> Option<Arc<ClientAwaitedAction>> {
        self.client_operation_to_awaited_action
            .get(client_operation_id)
            .await
    }

    /// When a client operation is dropped, we need to remove it from the
    /// other maps and update the listening clients count on the [`AwaitedAction`].
    fn on_client_operations_drop(
        &mut self,
        awaited_actions: impl IntoIterator<Item = Arc<AwaitedAction>>,
    ) {
        for awaited_action in awaited_actions.into_iter() {
            if awaited_action.get_listening_clients() != 0 {
                // We still have other clients listening to this action.
                continue;
            }

            let operation_id = awaited_action.get_operation_id();

            // Cleanup operation_id_to_awaited_action.
            if self
                .operation_id_to_awaited_action
                .remove(&operation_id)
                .is_none()
            {
                event!(
                    Level::ERROR,
                    ?operation_id,
                    ?awaited_action,
                    "operation_id_to_awaited_action and client_operation_to_awaited_action are out of sync",
                );
            }

            // Cleanup action_info_hash_key_to_awaited_action if it was marked cached.
            let action_info = awaited_action.get_action_info();
            match &action_info.unique_qualifier {
                ActionUniqueQualifier::Cachable(action_key) => {
                    let maybe_awaited_action = self
                        .action_info_hash_key_to_awaited_action
                        .remove(action_key);
                    if maybe_awaited_action.is_none() {
                        event!(
                            Level::ERROR,
                            ?operation_id,
                            ?awaited_action,
                            ?action_key,
                            "action_info_hash_key_to_awaited_action and operation_id_to_awaited_action are out of sync",
                        );
                    }
                }
                ActionUniqueQualifier::Uncachable(_action_key) => {
                    // This Operation should not be in the hash_key map.
                }
            }

            // Cleanup sorted_awaited_action.
            let sort_info = awaited_action.get_sort_info();
            let sort_key = sort_info.get_previous_sort_key();
            let sort_map_for_state =
                self.get_sort_map_for_state(&awaited_action.get_current_state().stage);
            drop(sort_info);
            let maybe_sorted_awaited_action = sort_map_for_state.take(&SortedAwaitedAction {
                sort_key,
                awaited_action,
            });
            if maybe_sorted_awaited_action.is_none() {
                event!(
                    Level::ERROR,
                    ?operation_id,
                    ?sort_key,
                    "sorted_action_info_hash_keys and action_info_hash_key_to_awaited_action are out of sync",
                );
            }
        }
    }

    fn get_all_awaited_actions(&self) -> impl Iterator<Item = &Arc<AwaitedAction>> {
        self.operation_id_to_awaited_action.values()
    }

    fn get_by_operation_id(&self, operation_id: &OperationId) -> Option<&Arc<AwaitedAction>> {
        self.operation_id_to_awaited_action.get(operation_id)
    }

    fn get_cache_check_actions(&self) -> &BTreeSet<SortedAwaitedAction> {
        &self.sorted_action_info_hash_keys.cache_check
    }

    fn get_queued_actions(&self) -> &BTreeSet<SortedAwaitedAction> {
        &self.sorted_action_info_hash_keys.queued
    }

    fn get_executing_actions(&self) -> &BTreeSet<SortedAwaitedAction> {
        &self.sorted_action_info_hash_keys.executing
    }

    fn get_completed_actions(&self) -> &BTreeSet<SortedAwaitedAction> {
        &self.sorted_action_info_hash_keys.completed
    }

    fn get_sort_map_for_state(
        &mut self,
        state: &ActionStage,
    ) -> &mut BTreeSet<SortedAwaitedAction> {
        match state {
            ActionStage::Unknown => &mut self.sorted_action_info_hash_keys.unknown,
            ActionStage::CacheCheck => &mut self.sorted_action_info_hash_keys.cache_check,
            ActionStage::Queued => &mut self.sorted_action_info_hash_keys.queued,
            ActionStage::Executing => &mut self.sorted_action_info_hash_keys.executing,
            ActionStage::Completed(_) => &mut self.sorted_action_info_hash_keys.completed,
            ActionStage::CompletedFromCache(_) => {
                &mut self.sorted_action_info_hash_keys.completed_from_cache
            }
        }
    }

    fn insert_sort_map_for_stage(
        &mut self,
        stage: &ActionStage,
        sorted_awaited_action: SortedAwaitedAction,
    ) {
        let newly_inserted = match stage {
            ActionStage::Unknown => self
                .sorted_action_info_hash_keys
                .unknown
                .insert(sorted_awaited_action),
            ActionStage::CacheCheck => self
                .sorted_action_info_hash_keys
                .cache_check
                .insert(sorted_awaited_action),
            ActionStage::Queued => self
                .sorted_action_info_hash_keys
                .queued
                .insert(sorted_awaited_action),
            ActionStage::Executing => self
                .sorted_action_info_hash_keys
                .executing
                .insert(sorted_awaited_action),
            ActionStage::Completed(_) => self
                .sorted_action_info_hash_keys
                .completed
                .insert(sorted_awaited_action),
            ActionStage::CompletedFromCache(_) => self
                .sorted_action_info_hash_keys
                .completed_from_cache
                .insert(sorted_awaited_action),
        };
        if !newly_inserted {
            event!(
                Level::ERROR,
                "Tried to insert an action that was already in the sorted map. This should never happen.",
            );
        }
    }

    /// Sets the state of the action to the provided `action_state` and notifies all listeners.
    /// If the action has no more listeners, returns `false`.
    fn set_action_state(
        &mut self,
        awaited_action: Arc<AwaitedAction>,
        new_action_state: Arc<ActionState>,
    ) -> bool {
        // We need to first get a lock on the awaited action to ensure
        // another operation doesn't update it while we are looking up
        // the sorted key.
        let sort_info = awaited_action.get_sort_info();
        let old_state = awaited_action.get_current_state();

        let has_listeners = awaited_action.set_current_state(new_action_state.clone());

        if !old_state.stage.is_same_stage(&new_action_state.stage) {
            let sort_key = sort_info.get_previous_sort_key();
            let btree = self.get_sort_map_for_state(&old_state.stage);
            drop(sort_info);
            let maybe_sorted_awaited_action = btree.take(&SortedAwaitedAction {
                sort_key,
                awaited_action,
            });

            let Some(sorted_awaited_action) = maybe_sorted_awaited_action else {
                event!(
                    Level::ERROR,
                    "sorted_action_info_hash_keys and action_info_hash_key_to_awaited_action are out of sync",
                );
                return false;
            };

            self.insert_sort_map_for_stage(&new_action_state.stage, sorted_awaited_action);
        }
        has_listeners
    }

    async fn subscribe_or_add_action(
        &mut self,
        client_operation_id: ClientOperationId,
        action_info: Arc<ActionInfo>,
        client_operation_drop_tx: &mpsc::UnboundedSender<Arc<AwaitedAction>>,
    ) -> watch::Receiver<Arc<ActionState>> {
        // Check to see if the action is already known and subscribe if it is.
        let subscription_result = self
            .try_subscribe(
                &client_operation_id,
                &action_info.unique_qualifier,
                action_info.priority,
                client_operation_drop_tx,
            )
            .await;
        let action_info = match subscription_result {
            Ok(subscription) => return subscription,
            // TODO!(we should not ignore the error here.)
            Err(_) => action_info,
        };

        let maybe_unique_key = match &action_info.unique_qualifier {
            ActionUniqueQualifier::Cachable(unique_key) => Some(unique_key.clone()),
            ActionUniqueQualifier::Uncachable(_unique_key) => None,
        };
        let (awaited_action, sort_key, subscription) =
            AwaitedAction::new_with_subscription(action_info);
        let awaited_action = Arc::new(awaited_action);
        self.client_operation_to_awaited_action
            .insert(
                client_operation_id,
                Arc::new(ClientAwaitedAction::new(
                    awaited_action.clone(),
                    client_operation_drop_tx.clone(),
                )),
            )
            .await;
        // Note: We only put items in the map that are cachable.
        if let Some(unique_key) = maybe_unique_key {
            self.action_info_hash_key_to_awaited_action
                .insert(unique_key, awaited_action.clone());
        }
        self.operation_id_to_awaited_action
            .insert(awaited_action.get_operation_id(), awaited_action.clone());

        self.insert_sort_map_for_stage(
            &awaited_action.get_current_state().stage,
            SortedAwaitedAction {
                sort_key,
                awaited_action,
            },
        );
        subscription
    }

    async fn try_subscribe(
        &mut self,
        client_operation_id: &ClientOperationId,
        unique_qualifier: &ActionUniqueQualifier,
        priority: i32,
        client_operation_drop_tx: &mpsc::UnboundedSender<Arc<AwaitedAction>>,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let unique_key = match unique_qualifier {
            ActionUniqueQualifier::Cachable(unique_key) => unique_key,
            ActionUniqueQualifier::Uncachable(_unique_key) => {
                return Err(make_err!(
                    Code::InvalidArgument,
                    "Cannot subscribe to an existing item when skip_cache_lookup is true."
                ));
            }
        };

        let awaited_action = self
            .action_info_hash_key_to_awaited_action
            .get(unique_key)
            .ok_or(make_input_err!(
                "Could not find existing action with name: {unique_qualifier}"
            ))
            .err_tip(|| "In state_manager::try_subscribe")?;

        // Do not subscribe if the action is already completed,
        // this is the responsibility of the CacheLookupScheduler.
        if awaited_action.get_current_state().stage.is_finished() {
            return Err(make_input_err!(
                "Subscribing an item that is already completed should be handled by CacheLookupScheduler."
            ));
        }
        let awaited_action = awaited_action.clone();
        if let Some(sort_info_lock) = awaited_action.upgrade_priority(priority) {
            let state = awaited_action.get_current_state();
            let maybe_sorted_awaited_action =
                self.get_sort_map_for_state(&state.stage)
                    .take(&SortedAwaitedAction {
                        sort_key: sort_info_lock.get_previous_sort_key(),
                        awaited_action: awaited_action.clone(),
                    });
            let Some(mut sorted_awaited_action) = maybe_sorted_awaited_action else {
                // TODO!(Either use event on all of the above error here, but both is overkill).
                let err = make_err!(
                    Code::Internal,
                    "sorted_action_info_hash_keys and action_info_hash_key_to_awaited_action are out of sync");
                event!(Level::ERROR, ?unique_qualifier, ?awaited_action, "{err:?}",);
                return Err(err);
            };
            sorted_awaited_action.sort_key = sort_info_lock.get_new_sort_key();
            self.insert_sort_map_for_stage(&state.stage, sorted_awaited_action);
        }

        let subscription = awaited_action.subscribe();

        self.client_operation_to_awaited_action
            .insert(
                client_operation_id.clone(),
                Arc::new(ClientAwaitedAction::new(
                    awaited_action,
                    client_operation_drop_tx.clone(),
                )),
            )
            .await;

        Ok(subscription)
    }
}

#[repr(transparent)]
pub struct StateManager {
    inner: Arc<Mutex<StateManagerImpl>>,
}

impl StateManager {
    pub fn new(
        config: &EvictionPolicy,
        tasks_change_notify: Arc<Notify>,
        max_job_retries: usize,
    ) -> Self {
        Self {
            inner: Arc::new_cyclic(move |weak_self| -> Mutex<StateManagerImpl> {
                let weak_inner = weak_self.clone();
                let (client_operation_drop_tx, mut client_operation_drop_rx) =
                    mpsc::unbounded_channel();
                let client_operation_cleanup_spawn =
                    spawn!("state_manager_client_drop_rx", async move {
                        /// Number of events to pull from the stream at a time.
                        const MAX_DROP_HANDLES_PER_CYCLE: usize = 1024;
                        let mut dropped_client_ids = Vec::with_capacity(MAX_DROP_HANDLES_PER_CYCLE);
                        loop {
                            dropped_client_ids.clear();
                            client_operation_drop_rx
                                .recv_many(&mut dropped_client_ids, MAX_DROP_HANDLES_PER_CYCLE)
                                .await;
                            let Some(inner) = weak_inner.upgrade() else {
                                return; // Nothing to cleanup, our struct is dropped.
                            };
                            let mut inner_mux = inner.lock().await;
                            inner_mux
                                .action_db
                                .on_client_operations_drop(dropped_client_ids.drain(..));
                        }
                    });
                Mutex::new(StateManagerImpl {
                    action_db: AwaitedActionDb {
                        client_operation_to_awaited_action: EvictingMap::new(
                            config,
                            SystemTime::now(),
                        ),
                        operation_id_to_awaited_action: HashMap::new(),
                        action_info_hash_key_to_awaited_action: HashMap::new(),
                        sorted_action_info_hash_keys: SortedAwaitedActions::default(),
                    },
                    tasks_change_notify,
                    max_job_retries,
                    client_operation_drop_tx,
                    _client_operation_cleanup_spawn: client_operation_cleanup_spawn,
                })
            }),
        }
    }

    async fn inner_filter_operations<F>(
        &self,
        filter: &OperationFilter,
        to_action_state_result: F,
    ) -> Result<ActionStateResultStream, Error>
    where
        F: Fn(Arc<AwaitedAction>) -> Arc<dyn ActionStateResult> + Send + Sync + 'static,
    {
        fn get_tree_for_stage(
            action_db: &AwaitedActionDb,
            stage: OperationStageFlags,
        ) -> Option<&BTreeSet<SortedAwaitedAction>> {
            match stage {
                OperationStageFlags::CacheCheck => Some(action_db.get_cache_check_actions()),
                OperationStageFlags::Queued => Some(action_db.get_queued_actions()),
                OperationStageFlags::Executing => Some(action_db.get_executing_actions()),
                OperationStageFlags::Completed => Some(action_db.get_completed_actions()),
                _ => None,
            }
        }

        let inner = self.inner.lock().await;

        if let Some(operation_id) = &filter.operation_id {
            return Ok(inner
                .action_db
                .get_by_operation_id(operation_id)
                .filter(|awaited_action| filter_check(awaited_action.as_ref(), filter))
                .cloned()
                .map(|awaited_action| -> ActionStateResultStream {
                    Box::pin(stream::once(async move {
                        to_action_state_result(awaited_action)
                    }))
                })
                .unwrap_or_else(|| Box::pin(stream::empty())));
        }
        if let Some(client_operation_id) = &filter.client_operation_id {
            return Ok(inner
                .action_db
                .get_by_client_operation_id(client_operation_id)
                .await
                .filter(|client_awaited_action| {
                    filter_check(client_awaited_action.awaited_action().as_ref(), filter)
                })
                .map(|client_awaited_action| -> ActionStateResultStream {
                    Box::pin(stream::once(async move {
                        to_action_state_result(client_awaited_action.awaited_action().clone())
                    }))
                })
                .unwrap_or_else(|| Box::pin(stream::empty())));
        }

        if get_tree_for_stage(&inner.action_db, filter.stages).is_none() {
            let mut all_items: Vec<Arc<AwaitedAction>> = inner
                .action_db
                .get_all_awaited_actions()
                .filter(|awaited_action| filter_check(awaited_action.as_ref(), filter))
                .cloned()
                .collect();
            match filter.order_by_priority_direction {
                Some(OrderDirection::Asc) => all_items.sort_unstable_by(|a, b| {
                    a.get_sort_info()
                        .get_new_sort_key()
                        .cmp(&b.get_sort_info().get_new_sort_key())
                }),
                Some(OrderDirection::Desc) => all_items.sort_unstable_by(|a, b| {
                    b.get_sort_info()
                        .get_new_sort_key()
                        .cmp(&a.get_sort_info().get_new_sort_key())
                }),
                None => {}
            }
            return Ok(Box::pin(stream::iter(
                all_items.into_iter().map(to_action_state_result),
            )));
        }

        drop(inner);

        struct State<
            F: Fn(Arc<AwaitedAction>) -> Arc<dyn ActionStateResult> + Send + Sync + 'static,
        > {
            inner: Arc<Mutex<StateManagerImpl>>,
            filter: OperationFilter,
            buffer: VecDeque<SortedAwaitedAction>,
            start_key: Bound<SortedAwaitedAction>,
            to_action_state_result: F,
        }
        let state = State {
            inner: self.inner.clone(),
            filter: filter.clone(),
            buffer: VecDeque::new(),
            start_key: Bound::Unbounded,
            to_action_state_result,
        };

        const STREAM_BUFF_SIZE: usize = 64;

        Ok(Box::pin(unfold(state, move |mut state| async move {
            if let Some(sorted_awaited_action) = state.buffer.pop_front() {
                if state.buffer.is_empty() {
                    state.start_key = Bound::Excluded(sorted_awaited_action.clone());
                }
                return Some((
                    (state.to_action_state_result)(sorted_awaited_action.awaited_action),
                    state,
                ));
            }

            let inner = state.inner.lock().await;

            #[allow(clippy::mutable_key_type)]
            let btree = get_tree_for_stage(&inner.action_db, state.filter.stages)
                .expect("get_tree_for_stage() should have already returned Some but in iteration it returned None");

            let range = (state.start_key.as_ref(), Bound::Unbounded);
            if state.filter.order_by_priority_direction == Some(OrderDirection::Asc) {
                btree
                    .range(range)
                    .filter(|item| filter_check(item.awaited_action.as_ref(), &state.filter))
                    .take(STREAM_BUFF_SIZE)
                    .for_each(|item| state.buffer.push_back(item.clone()));
            } else {
                btree
                    .range(range)
                    .rev()
                    .filter(|item| filter_check(item.awaited_action.as_ref(), &state.filter))
                    .take(STREAM_BUFF_SIZE)
                    .for_each(|item| state.buffer.push_back(item.clone()));
            }
            drop(inner);
            let sorted_awaited_action = state.buffer.pop_front()?;
            if state.buffer.is_empty() {
                state.start_key = Bound::Excluded(sorted_awaited_action.clone());
            }
            Some((
                (state.to_action_state_result)(sorted_awaited_action.awaited_action),
                state,
            ))
        })))
    }
}

/// StateManager is responsible for maintaining the state of the scheduler. Scheduler state
/// includes the actions that are queued, active, and recently completed. It also includes the
/// workers that are available to execute actions based on allocation strategy.
pub(crate) struct StateManagerImpl {
    /// Database for storing the state of all actions.
    action_db: AwaitedActionDb,

    /// Notify task<->worker matching engine that work needs to be done.
    tasks_change_notify: Arc<Notify>,

    /// Maximum number of times a job can be retried.
    max_job_retries: usize,

    /// Channel to notify when a client operation id is dropped.
    client_operation_drop_tx: mpsc::UnboundedSender<Arc<AwaitedAction>>,

    /// Task to cleanup client operation ids that are no longer being listened to.
    // Note: This has a custom Drop function on it. It should stay alive only while
    // the StateManager is alive.
    _client_operation_cleanup_spawn: JoinHandleDropGuard<()>,
}

fn filter_check(awaited_action: &AwaitedAction, filter: &OperationFilter) -> bool {
    // Note: The caller must filter `client_operation_id`.

    if let Some(operation_id) = &filter.operation_id {
        if operation_id != &awaited_action.get_operation_id() {
            return false;
        }
    }

    if filter.worker_id.is_some() && filter.worker_id != awaited_action.get_worker_id() {
        return false;
    }

    {
        let action_info = awaited_action.get_action_info();
        if let Some(filter_unique_key) = &filter.unique_key {
            match &action_info.unique_qualifier {
                ActionUniqueQualifier::Cachable(unique_key) => {
                    if filter_unique_key != unique_key {
                        return false;
                    }
                }
                ActionUniqueQualifier::Uncachable(_) => {
                    return false;
                }
            }
        }
        if let Some(action_digest) = filter.action_digest {
            if action_digest != action_info.digest() {
                return false;
            }
        }
    }

    {
        let last_worker_update_timestamp = awaited_action.get_last_worker_updated_timestamp();
        if let Some(worker_update_before) = filter.worker_update_before {
            if worker_update_before < last_worker_update_timestamp {
                return false;
            }
        }
        let state = awaited_action.get_current_state();
        if let Some(completed_before) = filter.completed_before {
            if state.stage.is_finished() && completed_before < last_worker_update_timestamp {
                return false;
            }
        }
        if filter.stages != OperationStageFlags::Any {
            let stage_flag = match state.stage {
                ActionStage::Unknown => OperationStageFlags::Any,
                ActionStage::CacheCheck => OperationStageFlags::CacheCheck,
                ActionStage::Queued => OperationStageFlags::Queued,
                ActionStage::Executing => OperationStageFlags::Executing,
                ActionStage::Completed(_) => OperationStageFlags::Completed,
                ActionStage::CompletedFromCache(_) => OperationStageFlags::Completed,
            };
            if !filter.stages.intersects(stage_flag) {
                return false;
            }
        }
    }

    true
}

impl StateManagerImpl {
    fn inner_update_operation(
        &mut self,
        operation_id: &OperationId,
        maybe_worker_id: Option<&WorkerId>,
        action_stage_result: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        let awaited_action = self
            .action_db
            .get_by_operation_id(operation_id)
            .ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "Could not find action info StateManager::update_operation"
                )
            })?
            .clone();

        // Make sure we don't update an action that is already completed.
        if awaited_action.get_current_state().stage.is_finished() {
            return Err(make_err!(
                Code::Internal,
                "Action {operation_id:?} is already completed with state {:?}",
                awaited_action.get_current_state().stage,
            ));
        }

        // Make sure the worker id matches the awaited action worker id.
        // This might happen if the worker sending the update is not the
        // worker that was assigned.
        let awaited_action_worker_id = awaited_action.get_worker_id();
        if awaited_action_worker_id.is_some()
            && maybe_worker_id.is_some()
            && maybe_worker_id != awaited_action_worker_id.as_ref()
        {
            let err = make_err!(
                Code::Internal,
                "Worker ids do not match - {:?} != {:?} for {:?}",
                maybe_worker_id,
                awaited_action_worker_id,
                awaited_action,
            );
            event!(
                Level::ERROR,
                ?operation_id,
                ?maybe_worker_id,
                ?awaited_action_worker_id,
                "{}",
                err.to_string(),
            );
            return Err(err);
        }

        let stage = match action_stage_result {
            Ok(stage) => stage,
            Err(err) => {
                // Don't count a backpressure failure as an attempt for an action.
                let due_to_backpressure = err.code == Code::ResourceExhausted;
                if !due_to_backpressure {
                    awaited_action.inc_attempts();
                }

                if awaited_action.get_attempts() > self.max_job_retries {
                    ActionStage::Completed(ActionResult {
                        execution_metadata: ExecutionMetadata {
                            worker: maybe_worker_id.map_or_else(String::default, |v| v.to_string()),
                            ..ExecutionMetadata::default()
                        },
                        error: Some(err.clone().merge(make_err!(
                            Code::Internal,
                            "Job cancelled because it attempted to execute too many times and failed"
                        ))),
                        ..ActionResult::default()
                    })
                } else {
                    ActionStage::Queued
                }
            }
        };
        if matches!(stage, ActionStage::Queued) {
            // If the action is queued, we need to unset the worker id regardless of
            // which worker sent the update.
            awaited_action.set_worker_id(None);
        } else {
            awaited_action.set_worker_id(maybe_worker_id.copied());
        }
        let has_listeners = self.action_db.set_action_state(
            awaited_action.clone(),
            Arc::new(ActionState {
                stage,
                id: operation_id.clone(),
            }),
        );
        if !has_listeners {
            let action_state = awaited_action.get_current_state();
            event!(
                Level::WARN,
                ?awaited_action,
                ?action_state,
                "Action has no more listeners during AwaitedActionDb::set_action_state"
            );
        }

        self.tasks_change_notify.notify_one();
        Ok(())
    }

    async fn inner_add_operation(
        &mut self,
        new_client_operation_id: ClientOperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let rx = self
            .action_db
            .subscribe_or_add_action(
                new_client_operation_id,
                action_info,
                &self.client_operation_drop_tx,
            )
            .await;
        self.tasks_change_notify.notify_one();
        Ok(rx)
    }
}

/// Utility struct to create a background task that keeps the client operation id alive.
fn make_client_keepalive_spawn(
    client_operation_id: ClientOperationId,
    inner_weak: Weak<Mutex<StateManagerImpl>>,
) -> JoinHandleDropGuard<()> {
    spawn!("client_action_state_result_keepalive", async move {
        loop {
            tokio::time::sleep(KEEPALIVE_DURATION).await;
            let Some(inner) = inner_weak.upgrade() else {
                return; // Nothing to do.
            };
            let inner = inner.lock().await;
            let refresh_success = inner
                .action_db
                .refresh_client_operation_id(&client_operation_id)
                .await;
            if !refresh_success {
                event! {
                    Level::ERROR,
                    ?client_operation_id,
                    "Client operation id not found in StateManager::add_action keepalive"
                };
            }
        }
    })
}

#[async_trait]
impl ClientStateManager for StateManager {
    async fn add_action(
        &self,
        client_operation_id: ClientOperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Arc<dyn ActionStateResult>, Error> {
        let mut inner = self.inner.lock().await;
        let rx = inner
            .inner_add_operation(client_operation_id.clone(), action_info.clone())
            .await?;

        let inner_weak = Arc::downgrade(&self.inner);
        Ok(Arc::new(ClientActionStateResult::new(
            action_info,
            rx,
            Some(make_client_keepalive_spawn(client_operation_id, inner_weak)),
        )))
    }

    async fn filter_operations(
        &self,
        filter: &OperationFilter,
    ) -> Result<ActionStateResultStream, Error> {
        let maybe_client_operation_id = filter.client_operation_id.clone();
        let inner_weak = Arc::downgrade(&self.inner);
        self.inner_filter_operations(filter, move |awaited_action| {
            Arc::new(ClientActionStateResult::new(
                awaited_action.get_action_info().clone(),
                awaited_action.subscribe(),
                maybe_client_operation_id
                    .as_ref()
                    .map(|client_operation_id| {
                        make_client_keepalive_spawn(client_operation_id.clone(), inner_weak.clone())
                    }),
            ))
        })
        .await
    }
}

#[async_trait]
impl WorkerStateManager for StateManager {
    async fn update_operation(
        &self,
        operation_id: &OperationId,
        worker_id: &WorkerId,
        action_stage_result: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        inner.inner_update_operation(operation_id, Some(worker_id), action_stage_result)
    }
}

#[async_trait]
impl MatchingEngineStateManager for StateManager {
    async fn filter_operations(
        &self,
        filter: &OperationFilter,
    ) -> Result<ActionStateResultStream, Error> {
        self.inner_filter_operations(filter, |awaited_action| {
            Arc::new(MatchingEngineActionStateResult::new(awaited_action))
        })
        .await
    }

    async fn assign_operation(
        &self,
        operation_id: &OperationId,
        worker_id_or_reason_for_unsassign: Result<&WorkerId, Error>,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;

        let (maybe_worker_id, stage_result) = match worker_id_or_reason_for_unsassign {
            Ok(worker_id) => (Some(worker_id), Ok(ActionStage::Executing)),
            Err(err) => (None, Err(err)),
        };
        inner.inner_update_operation(operation_id, maybe_worker_id, stage_result)
    }
}
