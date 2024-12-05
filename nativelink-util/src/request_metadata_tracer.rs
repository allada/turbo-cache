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

use nativelink_error::ResultExt;
use crate::origin_context::{ActiveOriginContext, OriginContext};
use crate::make_symbol;
use nativelink_error::{make_err, Code, Error};
use std::sync::Arc;
use tonic::Request;
use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use nativelink_proto::build::bazel::remote::execution::v2::RequestMetadata;
use prost::Message;
use bytes;
use tracing::{event, Level};
use tokio::sync::mpsc::UnboundedSender;

type RequestMetadataTraceFnType = Box<dyn Fn(String) + Send + Sync + 'static>;
make_symbol!(REQUEST_METADATA_TRACE, RequestMetadataTraceFnType);

const BAZEL_METADATA_KEY: &'static str = "build.bazel.remote.execution.v2.requestmetadata-bin";

#[derive(Clone, Debug)]
pub struct RequestMetadataTracer {
    pub metadata: String,
}

#[derive(Clone, Debug)]
pub struct MetadataEvent {
    pub request_metadata_tracer: RequestMetadataTracer,
    pub name: String // Name of who emitted the event
}

pub fn make_ctx_request_metadata_tracer(metadata_bin: &str, metadata_tx: &UnboundedSender<MetadataEvent>) -> Result<Arc<OriginContext>, Error> {

    let mut new_ctx = ActiveOriginContext::fork().err_tip(|| "Must be in a ActiveOriginContext")?;
    let metadata = RequestMetadataTracer {
        metadata: metadata_bin.to_string()
    };

    let metadata_tx = metadata_tx.to_owned();

    let sender: Box<dyn Fn(String) + Send + Sync + 'static> = Box::new(move |name: String| {
        let metadata_event = MetadataEvent {
        request_metadata_tracer: metadata.clone(),
        name: name
        };
        let _ = metadata_tx.send(metadata_event);
    });

    new_ctx.set_value(&REQUEST_METADATA_TRACE, Arc::new(sender));
    Ok(Arc::new(new_ctx))
}

pub fn emit_metadata_event(name: String) {
    let sender_result = ActiveOriginContext::get_value(&REQUEST_METADATA_TRACE);
    match sender_result {
        Ok(Some(sender)) => {
            event!(Level::DEBUG, ?name, "Sending event");
            sender(name)
        },
        _ => event!(Level::WARN, ?name, "REQUEST_METADATA_TRACE not in ActiveOriginContext")
    }
}

pub fn extract_request_metadata_bin<T>(request: &Request<T>) -> Option<RequestMetadataTracer> {
    let headers = request.metadata().clone().into_headers();
    let metadata_opt: Option<&hyper::header::HeaderValue> = headers.get(BAZEL_METADATA_KEY);

    if let Some(header_value) = metadata_opt {
        match header_value.to_str() {
            Ok(metadata_str) => {
                event!(Level::DEBUG, ?metadata_str, "RequestMetadataTracer in header");
                return Some(RequestMetadataTracer {
                metadata: metadata_str.to_string()
            })
        },
            Err(err) => {
                event!(
                    Level::ERROR,
                    ?err,
                    "Unable to extract metadata from headers",
                );
                return None
            }
        }
    }

    event!(Level::INFO, "Header does not contain bazel metadata key");
    return None
}

impl RequestMetadataTracer {
    pub fn decode(&self) -> Result<RequestMetadata, Error> {
        let decoded = match STANDARD_NO_PAD.decode(self.metadata.clone()) {
            Ok(decoded) => decoded,
            Err(err) => {
                event!(Level::ERROR, "Could not convert request data from base64: {err}");
                return Err(make_err!(Code::Internal, "Could not convert request data from base64: {err}"));
            }
        };

        let buf = bytes::BytesMut::from(decoded.as_slice());

        let request_metadata = match RequestMetadata::decode(buf) {
            Ok(request_metadata) => request_metadata,
            Err(err) => {
                event!(Level::ERROR, "Could not convert grpc request from binary data: {err}");
                return Err(make_err!(Code::Internal, "Could not convert grpc request from binary data: {err}"));
            }
        };

        return Ok(request_metadata)
    }
}
