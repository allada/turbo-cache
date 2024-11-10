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
use std::time::SystemTime;

use nativelink_config::schedulers::{
    ExperimentalSimpleSchedulerBackend, MemoryBackend, SchedulerConfig, SchedulerRef,
    SchedulerSpec, SimpleSpec,
};
use nativelink_config::stores::EvictionPolicy;
use nativelink_error::{make_input_err, Error, ResultExt};
use nativelink_store::redis_store::RedisStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::operation_state_manager::ClientStateManager;
use tokio::sync::Notify;

use crate::cache_lookup_scheduler::CacheLookupScheduler;
use crate::grpc_scheduler::GrpcScheduler;
use crate::memory_awaited_action_db::MemoryAwaitedActionDb;
use crate::property_modifier_scheduler::PropertyModifierScheduler;
use crate::simple_scheduler::SimpleScheduler;
use crate::store_awaited_action_db::StoreAwaitedActionDb;
use crate::worker_scheduler::WorkerScheduler;

/// Default timeout for recently completed actions in seconds.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_RETAIN_COMPLETED_FOR_S: u32 = 60;

pub type SchedulerFactoryResults = (
    Option<Arc<dyn ClientStateManager>>,
    Option<Arc<dyn WorkerScheduler>>,
);

pub fn scheduler_factory(
    scheduler_type_cfg: &SchedulerRef,
    store_manager: &StoreManager,
) -> Result<SchedulerFactoryResults, Error> {
    inner_scheduler_factory(scheduler_type_cfg, store_manager)
}

fn inner_scheduler_factory(
    scheduler_config: &SchedulerRef,
    store_manager: &StoreManager,
) -> Result<SchedulerFactoryResults, Error> {
    let scheduler_cfg: SchedulerConfig = scheduler_config.clone().into();

    let scheduler: SchedulerFactoryResults = match &scheduler_cfg.spec {
        SchedulerSpec::Simple(spec) => {
            simple_scheduler_factory(spec, store_manager, SystemTime::now)?
        }
        SchedulerSpec::Grpc(spec) => (Some(Arc::new(GrpcScheduler::new(spec)?)), None),
        SchedulerSpec::CacheLookup(spec) => {
            let ac_store = store_manager
                .get_store(&spec.ac_store)
                .err_tip(|| format!("'ac_store': '{}' does not exist", spec.ac_store))?;
            let (action_scheduler, worker_scheduler) =
                inner_scheduler_factory(&spec.scheduler, store_manager)
                    .err_tip(|| "In nested CacheLookupScheduler construction")?;
            let cache_lookup_scheduler = Arc::new(CacheLookupScheduler::new(
                ac_store,
                action_scheduler.err_tip(|| "Nested scheduler is not an action scheduler")?,
            )?);
            (Some(cache_lookup_scheduler), worker_scheduler)
        }
        SchedulerSpec::PropertyModifier(spec) => {
            let (action_scheduler, worker_scheduler) =
                inner_scheduler_factory(&spec.scheduler, store_manager)
                    .err_tip(|| "In nested PropertyModifierScheduler construction")?;
            let property_modifier_scheduler = Arc::new(PropertyModifierScheduler::new(
                spec,
                action_scheduler.err_tip(|| "Nested scheduler is not an action scheduler")?,
            ));
            (Some(property_modifier_scheduler), worker_scheduler)
        }
    };

    Ok(scheduler)
}

fn simple_scheduler_factory(
    spec: &SimpleSpec,
    store_manager: &StoreManager,
    now_fn: fn() -> SystemTime,
) -> Result<SchedulerFactoryResults, Error> {
    match spec
        .experimental_backend
        .as_ref()
        .unwrap_or(&ExperimentalSimpleSchedulerBackend::Memory(
            MemoryBackend {},
        )) {
        ExperimentalSimpleSchedulerBackend::Memory(_) => {
            let task_change_notify = Arc::new(Notify::new());
            let awaited_action_db = memory_awaited_action_db_factory(
                spec.retain_completed_for_s,
                &task_change_notify.clone(),
                SystemTime::now,
            );
            let (action_scheduler, worker_scheduler) =
                SimpleScheduler::new(spec, awaited_action_db, task_change_notify);
            Ok((Some(action_scheduler), Some(worker_scheduler)))
        }
        ExperimentalSimpleSchedulerBackend::redis(redis_config) => {
            let store = store_manager
                .get_store(redis_config.redis_store.as_ref())
                .err_tip(|| {
                    format!(
                        "'redis_store': '{}' does not exist",
                        redis_config.redis_store
                    )
                })?;
            let task_change_notify = Arc::new(Notify::new());
            let store = store
                .into_inner()
                .as_any_arc()
                .downcast::<RedisStore>()
                .map_err(|_| {
                    make_input_err!(
                        "Could not downcast to redis store in RedisAwaitedActionDb::new"
                    )
                })?;
            let awaited_action_db = StoreAwaitedActionDb::new(
                store,
                task_change_notify.clone(),
                now_fn,
                Default::default,
            )
            .err_tip(|| "In state_manager_factory::redis_state_manager")?;
            let (action_scheduler, worker_scheduler) =
                SimpleScheduler::new(spec, awaited_action_db, task_change_notify);
            Ok((Some(action_scheduler), Some(worker_scheduler)))
        }
    }
}

pub fn memory_awaited_action_db_factory<I, NowFn>(
    mut retain_completed_for_s: u32,
    task_change_notify: &Arc<Notify>,
    now_fn: NowFn,
) -> MemoryAwaitedActionDb<I, NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Clone + Send + Sync + 'static,
{
    if retain_completed_for_s == 0 {
        retain_completed_for_s = DEFAULT_RETAIN_COMPLETED_FOR_S;
    }
    MemoryAwaitedActionDb::new(
        &EvictionPolicy {
            max_seconds: retain_completed_for_s,
            ..Default::default()
        },
        task_change_notify.clone(),
        now_fn,
    )
}
