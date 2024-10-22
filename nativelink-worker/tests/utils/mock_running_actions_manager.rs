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

use async_lock::Mutex;
use nativelink_error::{make_input_err, Error};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
use nativelink_util::action_messages::{ActionResult, OperationId};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_worker::running_actions_manager::{Metrics, RunningAction, RunningActionsManager};
use tokio::sync::mpsc;

#[derive(Debug)]
enum RunningActionManagerCalls {
    CreateAndAddAction((String, StartExecute)),
    CacheActionResult(Box<(DigestInfo, ActionResult, DigestHasherFunc)>),
}

enum RunningActionManagerReturns {
    CreateAndAddAction(Result<Arc<MockRunningAction>, Error>),
}

pub struct MockRunningActionsManager {
    rx_call: Mutex<mpsc::UnboundedReceiver<RunningActionManagerCalls>>,
    tx_call: mpsc::UnboundedSender<RunningActionManagerCalls>,

    rx_resp: Mutex<mpsc::UnboundedReceiver<RunningActionManagerReturns>>,
    tx_resp: mpsc::UnboundedSender<RunningActionManagerReturns>,

    rx_kill_all: Mutex<mpsc::UnboundedReceiver<()>>,
    tx_kill_all: mpsc::UnboundedSender<()>,

    rx_kill_operation: Mutex<mpsc::UnboundedReceiver<OperationId>>,
    tx_kill_operation: mpsc::UnboundedSender<OperationId>,
    metrics: Arc<Metrics>,
}

impl Default for MockRunningActionsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MockRunningActionsManager {
    pub fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        let (tx_kill_all, rx_kill_all) = mpsc::unbounded_channel();
        let (tx_kill_operation, rx_kill_operation) = mpsc::unbounded_channel();
        Self {
            rx_call: Mutex::new(rx_call),
            tx_call,
            rx_resp: Mutex::new(rx_resp),
            tx_resp,
            rx_kill_all: Mutex::new(rx_kill_all),
            tx_kill_all,
            rx_kill_operation: Mutex::new(rx_kill_operation),
            tx_kill_operation,
            metrics: Arc::new(Metrics::default()),
        }
    }
}

impl MockRunningActionsManager {
    pub async fn expect_create_and_add_action(
        &self,
        result: Result<Arc<MockRunningAction>, Error>,
    ) -> (String, StartExecute) {
        let mut rx_call_lock = self.rx_call.lock().await;
        let RunningActionManagerCalls::CreateAndAddAction(req) = rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        else {
            panic!("Got incorrect call waiting for create_and_add_action")
        };
        self.tx_resp
            .send(RunningActionManagerReturns::CreateAndAddAction(result))
            .map_err(|_| make_input_err!("Could not send request to mpsc"))
            .unwrap();
        req
    }

    pub async fn expect_cache_action_result(&self) -> (DigestInfo, ActionResult, DigestHasherFunc) {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not recieve msg in mpsc")
        {
            RunningActionManagerCalls::CacheActionResult(req) => *req,
            RunningActionManagerCalls::CreateAndAddAction(_) => {
                panic!("Got incorrect call waiting for cache_action_result")
            }
        }
    }

    pub async fn expect_kill_all(&self) {
        let mut rx_kill_all_lock = self.rx_kill_all.lock().await;
        rx_kill_all_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc");
    }

    pub async fn expect_kill_operation(&self) -> OperationId {
        let mut rx_kill_operation_lock = self.rx_kill_operation.lock().await;
        rx_kill_operation_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
    }
}

impl RunningActionsManager for MockRunningActionsManager {
    type RunningAction = MockRunningAction;

    async fn create_and_add_action(
        self: &Arc<Self>,
        worker_id: String,
        start_execute: StartExecute,
    ) -> Result<Arc<Self::RunningAction>, Error> {
        self.tx_call
            .send(RunningActionManagerCalls::CreateAndAddAction((
                worker_id,
                start_execute,
            )))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionManagerReturns::CreateAndAddAction(result) => result,
        }
    }

    async fn cache_action_result(
        &self,
        action_digest: DigestInfo,
        action_result: &mut ActionResult,
        digest_function: DigestHasherFunc,
    ) -> Result<(), Error> {
        self.tx_call
            .send(RunningActionManagerCalls::CacheActionResult(Box::new((
                action_digest,
                action_result.clone(),
                digest_function,
            ))))
            .expect("Could not send request to mpsc");
        Ok(())
    }

    async fn kill_operation(&self, operation_id: &OperationId) -> Result<(), Error> {
        self.tx_kill_operation
            .send(operation_id.clone())
            .expect("Could not send request to mpsc");
        Ok(())
    }

    async fn kill_all(&self) {
        self.tx_kill_all
            .send(())
            .expect("Could not send request to mpsc");
    }

    fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

#[derive(Debug)]
enum RunningActionCalls {
    PrepareAction,
    Execute,
    UploadResults,
    Cleanup,
    GetFinishedResult,
}

#[derive(Debug)]
enum RunningActionReturns {
    PrepareAction(Result<Arc<MockRunningAction>, Error>),
    Execute(Result<Arc<MockRunningAction>, Error>),
    UploadResults(Result<Arc<MockRunningAction>, Error>),
    Cleanup(Result<Arc<MockRunningAction>, Error>),
    GetFinishedResult(Box<Result<ActionResult, Error>>),
}

#[derive(Debug)]
pub struct MockRunningAction {
    rx_call: Mutex<mpsc::UnboundedReceiver<RunningActionCalls>>,
    tx_call: mpsc::UnboundedSender<RunningActionCalls>,

    rx_resp: Mutex<mpsc::UnboundedReceiver<RunningActionReturns>>,
    tx_resp: mpsc::UnboundedSender<RunningActionReturns>,
}

impl Default for MockRunningAction {
    fn default() -> Self {
        Self::new()
    }
}

impl MockRunningAction {
    pub fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        Self {
            rx_call: Mutex::new(rx_call),
            tx_call,
            rx_resp: Mutex::new(rx_resp),
            tx_resp,
        }
    }

    pub async fn simple_expect_get_finished_result(
        self: &Arc<Self>,
        result: Result<ActionResult, Error>,
    ) -> Result<(), Error> {
        self.expect_prepare_action(Ok(())).await?;
        self.expect_execute(Ok(())).await?;
        self.upload_results(Ok(())).await?;
        let result = self.get_finished_result(result).await;
        self.cleanup(Ok(())).await?;
        result
    }

    pub async fn expect_prepare_action(
        self: &Arc<Self>,
        result: Result<(), Error>,
    ) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionCalls::PrepareAction => (),
            req => panic!("expect_prepare_action expected PrepareAction, got : {req:?}"),
        };
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::PrepareAction(result))
            .expect("Could not send request to mpsc");
        Ok(())
    }

    pub async fn expect_execute(self: &Arc<Self>, result: Result<(), Error>) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionCalls::Execute => (),
            req => panic!("expect_execute expected Execute, got : {req:?}"),
        };
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::Execute(result))
            .expect("Could not send request to mpsc");
        Ok(())
    }

    pub async fn upload_results(self: &Arc<Self>, result: Result<(), Error>) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionCalls::UploadResults => (),
            req => panic!("expect_upload_results expected UploadResults, got : {req:?}"),
        };
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::UploadResults(result))
            .expect("Could not send request to mpsc");
        Ok(())
    }

    pub async fn cleanup(self: &Arc<Self>, result: Result<(), Error>) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionCalls::Cleanup => (),
            req => panic!("expect_cleanup expected Cleanup, got : {req:?}"),
        };
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::Cleanup(result))
            .expect("Could not send request to mpsc");
        Ok(())
    }

    pub async fn get_finished_result(
        self: &Arc<Self>,
        result: Result<ActionResult, Error>,
    ) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionCalls::GetFinishedResult => (),
            req => panic!("expect_get_finished_result expected GetFinishedResult, got : {req:?}"),
        };
        self.tx_resp
            .send(RunningActionReturns::GetFinishedResult(Box::new(result)))
            .expect("Could not send request to mpsc");
        Ok(())
    }
}

impl RunningAction for MockRunningAction {
    fn get_operation_id(&self) -> &OperationId {
        unreachable!("not implemented for tests");
    }

    async fn prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::PrepareAction)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionReturns::PrepareAction(result) => result,
            resp => panic!("execution_response expected PrepareAction response, received {resp:?}"),
        }
    }

    async fn execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::Execute)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionReturns::Execute(result) => result,
            resp => panic!("execution_response expected Execute response, received {resp:?}"),
        }
    }

    async fn upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::UploadResults)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionReturns::UploadResults(result) => result,
            resp => panic!("execution_response expected UploadResults response, received {resp:?}"),
        }
    }

    async fn cleanup(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::Cleanup)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionReturns::Cleanup(result) => result,
            resp => panic!("execution_response expected Cleanup response, received {resp:?}"),
        }
    }

    async fn get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error> {
        self.tx_call
            .send(RunningActionCalls::GetFinishedResult)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionReturns::GetFinishedResult(result) => *result,
            resp => {
                panic!("execution_response expected GetFinishedResult response, received {resp:?}")
            }
        }
    }

    fn get_work_directory(&self) -> &String {
        unreachable!();
    }
}
