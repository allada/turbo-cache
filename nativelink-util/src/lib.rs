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

pub mod action_messages;
pub mod buf_channel;
pub mod channel_body_for_tests;
pub mod chunked_stream;
pub mod common;
pub mod connection_manager;
pub mod digest_hasher;
pub mod evicting_map;
pub mod fastcdc;
pub mod fs;
pub mod health_utils;
pub mod instant_wrapper;
pub mod known_platform_property_provider;
pub mod metrics_utils;
pub mod operation_state_manager;
pub mod origin_context;
pub mod platform_properties;
pub mod proto_stream_utils;
pub mod request_metadata_tracer;
pub mod resource_info;
pub mod retry;
pub mod shutdown_guard;
pub mod store_trait;
pub mod task;
pub mod tls_utils;
pub mod write_counter;

// Re-export tracing mostly for use in macros.
pub use tracing as __tracing;

/// Initialize tracing.
pub fn init_tracing() -> Result<(), nativelink_error::Error> {
    use tracing_subscriber::prelude::*;

    static LOGGING_INITIALIZED: parking_lot::Mutex<bool> = parking_lot::Mutex::new(false);
    let mut logging_initized_guard = LOGGING_INITIALIZED.lock();
    if *logging_initized_guard {
        return Err(nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "Logging already initialized"
        ));
    }
    *logging_initized_guard = true;
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::metadata::LevelFilter::WARN.into())
        .from_env_lossy();

    // Setup tracing logger for multiple format types, compact, json, and pretty as a single layer.
    // Configuration for log format comes from environment variable NL_LOG_FMT due to subscribers
    // being configured before config parsing.
    let nl_log_fmt = std::env::var("NL_LOG").unwrap_or_else(|_| "pretty".to_string());
    // Layers vector is used for due to how tracing_subscriber::fmt::layer builds type signature
    // not being able to unify a single trait type before being boxed. For example see
    // https://docs.rs/tracing-subscriber/0.3.18/tracing_subscriber/layer/index.html
    let mut layers = Vec::new();
    match nl_log_fmt.as_str() {
        "compact" => layers.push(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_timer(tracing_subscriber::fmt::time::time())
                .with_filter(env_filter)
                .boxed(),
        ),
        "json" => layers.push(
            tracing_subscriber::fmt::layer()
                .json()
                .with_timer(tracing_subscriber::fmt::time::time())
                .with_filter(env_filter)
                .boxed(),
        ),
        _ => layers.push(
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_timer(tracing_subscriber::fmt::time::time())
                .with_filter(env_filter)
                .boxed(),
        ),
    };

    // Add a console subscriber if the feature is enabled, see tokio-console for a client console.
    // https://crates.io/crates/tokio-console
    if cfg!(feature = "enable_tokio_console") {
        layers.push(console_subscriber::spawn().boxed());
    }

    tracing_subscriber::registry().with(layers).init();
    Ok(())
}
