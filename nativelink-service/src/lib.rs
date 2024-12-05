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

macro_rules! wrap_with_metadata_tracing {
    ($span_name:expr, $inner_fn:expr, $grpc_request:expr, $metadata_tx:expr) => {{
        use futures::future::FutureExt;
        use nativelink_util::request_metadata_tracer;
        match request_metadata_tracer::extract_request_metadata_bin(&$grpc_request) {
            Some(request_metadata_tracer) => {
                let context = request_metadata_tracer::make_ctx_request_metadata_tracer(
                    &request_metadata_tracer.metadata,
                    $metadata_tx,
                )
                .err_tip(|| "Unable to parse request metadata")?;

                context
                    .wrap_async(
                        error_span!($span_name),
                        ($inner_fn)($grpc_request).inspect(|_| {
                            request_metadata_tracer::emit_metadata_event(String::from($span_name));
                        }),
                    )
                    .await
                    .map_err(Into::into)
            }
            _ => ($inner_fn)($grpc_request).await,
        }
    }};
}

pub mod ac_server;
pub mod bep_server;
pub mod bytestream_server;
pub mod capabilities_server;
pub mod cas_server;
pub mod execution_server;
pub mod health_server;
pub mod worker_api_server;
