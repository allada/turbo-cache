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

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::stream::{unfold, FuturesUnordered, StreamExt};
use futures::Future;
use nativelink_config::stores::Retry;
use nativelink_error::{make_err, Code, Error};
use tokio::sync::{mpsc, oneshot};
use tonic::transport::{channel, Channel, Endpoint};
use tracing::{event, Level};

use crate::background_spawn;
use crate::retry::{self, Retrier, RetryResult};

/// A helper utility that enables management of a suite of connections to an
/// upstream gRPC endpoint using Tonic.
pub struct ConnectionManager {
    // The channel to request connections from the worker.
    worker_tx: mpsc::Sender<oneshot::Sender<Connection>>,
}

/// The index into ConnectionManagerWorker::endpoints.
type EndpointIndex = usize;
/// The identifier for a given connection to a given Endpoint, used to identify
/// when a particular connection has failed or becomes available.
type ConnectionIndex = usize;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct ChannelIdentifier {
    /// The index into ConnectionManagerWorker::endpoints that established this
    /// Channel.
    endpoint_index: EndpointIndex,
    /// A unique identifier for this particular connection to the Endpoint.
    connection_index: ConnectionIndex,
}

/// The requests that can be made from a Connection to the
/// ConnectionManagerWorker such as informing it that it's been dropped or that
/// an error occurred.
enum ConnectionRequest {
    /// Notify that a Connection was dropped, if it was dropped while the
    /// connection was still pending, then return the pending Channel to be
    /// added back to the available channels.
    Dropped(Option<EstablishedChannel>),
    /// Notify that a Connection was established, return the Channel to the
    /// available channels.
    Connected(EstablishedChannel),
    /// Notify that there was a transport error on the given Channel, the bool
    /// specifies whether the connection was in the process of being established
    /// or not (i.e. whether it's been returned to available channels yet).
    Error((ChannelIdentifier, bool)),
}

/// The result of a Future that connects to a given Endpoint.  This is a tuple
/// of the index into the ConnectionManagerWorker::endpoints that this
/// connection is for, the iteration of the connection and the result of the
/// connection itself.
type IndexedChannel = Result<EstablishedChannel, (ChannelIdentifier, Error)>;

/// A channel that has been established to an endpoint with some metadata around
/// it to allow identification of the Channel if it errors in order to correctly
/// remove it.
#[derive(Clone)]
struct EstablishedChannel {
    /// The Channel itself that the meta data relates to.
    channel: Channel,
    /// The identifier of the channel in the worker.
    identifier: ChannelIdentifier,
}

/// The context of the worker used to manage all of the connections.  This
/// handles reconnecting to endpoints on errors and multiple connections to a
/// given endpoint.
struct ConnectionManagerWorker {
    /// The endpoints to establish Channels and the identifier of the last
    /// connection attempt to that endpoint.
    endpoints: Vec<(ConnectionIndex, Endpoint)>,
    /// The channel used to communicate between a Connection and the worker.
    connection_tx: mpsc::UnboundedSender<ConnectionRequest>,
    /// The number of connections that are currently allowed to be made.
    available_connections: usize,
    /// Channels that are currently being connected.
    connecting_channels: FuturesUnordered<Pin<Box<dyn Future<Output = IndexedChannel> + Send>>>,
    /// Connected channels that are available for use.
    available_channels: VecDeque<EstablishedChannel>,
    /// Requests for a Channel when available.
    waiting_connections: VecDeque<oneshot::Sender<Connection>>,
    /// The retry configuration for connecting to an Endpoint, on failure will
    /// restart the retrier after a 1 second delay.
    retrier: Retrier,
}

/// The maximum number of queued requests to obtain a connection from the
/// worker before applying back pressure to the requestor.  It makes sense to
/// keep this small since it has to wait for a response anyway.
const WORKER_BACKLOG: usize = 8;

impl ConnectionManager {
    /// Create a connection manager that creates a balance list between a given
    /// set of Endpoints.  This will restrict the number of concurrent requests
    /// and automatically re-connect upon transport error.
    pub fn new(
        endpoints: impl IntoIterator<Item = Endpoint>,
        mut connections_per_endpoint: usize,
        mut max_concurrent_requests: usize,
        retry: Retry,
        jitter_fn: retry::JitterFn,
    ) -> Self {
        let (worker_tx, worker_rx) = mpsc::channel(WORKER_BACKLOG);
        // The connection messages always come from sync contexts (e.g. drop)
        // and therefore, we'd end up spawning for them if this was bounded
        // which defeats the object since there would be no backpressure
        // applied. Therefore it makes sense for this to be unbounded.
        let (connection_tx, connection_rx) = mpsc::unbounded_channel();
        let endpoints = Vec::from_iter(
            endpoints
                .into_iter()
                .map(|endpoint| (0, endpoint))
                .collect::<Vec<(_, _)>>(),
        );
        if max_concurrent_requests == 0 {
            max_concurrent_requests = usize::MAX;
        }
        if connections_per_endpoint == 0 {
            connections_per_endpoint = 1;
        }
        let worker = ConnectionManagerWorker {
            endpoints,
            available_connections: max_concurrent_requests,
            connection_tx,
            connecting_channels: FuturesUnordered::new(),
            available_channels: VecDeque::new(),
            waiting_connections: VecDeque::new(),
            retrier: Retrier::new(
                Arc::new(|duration| Box::pin(tokio::time::sleep(duration))),
                jitter_fn,
                retry,
            ),
        };
        background_spawn!("connection_manager_worker_spawn", async move {
            worker
                .service_requests(connections_per_endpoint, worker_rx, connection_rx)
                .await;
        });
        Self { worker_tx }
    }

    /// Get a Connection that can be used as a tonic::Channel, except it
    /// performs some additional counting to reconnect on error and restrict
    /// the number of concurrent connections.
    pub async fn connection(&self) -> Result<Connection, Error> {
        let (tx, rx) = oneshot::channel();
        self.worker_tx
            .send(tx)
            .await
            .map_err(|err| make_err!(Code::Unavailable, "Requesting a new connection: {err:?}"))?;
        rx.await
            .map_err(|err| make_err!(Code::Unavailable, "Waiting for a new connection: {err:?}"))
    }
}

impl ConnectionManagerWorker {
    async fn service_requests(
        mut self,
        connections_per_endpoint: usize,
        mut worker_rx: mpsc::Receiver<oneshot::Sender<Connection>>,
        mut connection_rx: mpsc::UnboundedReceiver<ConnectionRequest>,
    ) {
        // Make the initial set of connections, connection failures will be
        // handled in the same way as future transport failures, so no need to
        // do anything special.
        for endpoint_index in 0..self.endpoints.len() {
            for _ in 0..connections_per_endpoint {
                self.connect_endpoint(endpoint_index, None);
            }
        }

        // The main worker loop, when select resolves one of its arms the other
        // ones are cancelled, therefore it's important that they maintain no
        // state while `await`-ing.  This is enforced through the use of
        // non-async functions to do all of the work.
        loop {
            tokio::select! {
                request = worker_rx.recv() => {
                    let Some(request) = request else {
                        // The ConnectionManager was dropped, shut down the
                        // worker.
                        break;
                    };
                    self.handle_worker(request);
                }
                maybe_request = connection_rx.recv() => {
                    if let Some(request) = maybe_request {
                        self.handle_connection(request);
                    }
                }
                maybe_connection_result = self.connect_next() => {
                    if let Some(connection_result) = maybe_connection_result {
                        self.handle_connected(connection_result);
                    }
                }
            }
        }
    }

    async fn connect_next(&mut self) -> Option<IndexedChannel> {
        if self.connecting_channels.is_empty() {
            // Make this Future never resolve, we will get cancelled by the
            // select if there's some change in state to `self` and can re-enter
            // and evaluate `connecting_channels` again.
            futures::future::pending::<()>().await;
        }
        self.connecting_channels.next().await
    }

    // This must never be made async otherwise the select may cancel it.
    fn handle_connected(&mut self, connection_result: IndexedChannel) {
        match connection_result {
            Ok(established_channel) => {
                self.available_channels.push_back(established_channel);
                self.maybe_available_connection();
            }
            // When the retrier runs out of attempts start again from the
            // beginning of the retry period.  Never want to be in a
            // situation where we give up on an Endpoint forever.
            Err((identifier, _)) => {
                self.connect_endpoint(identifier.endpoint_index, Some(identifier.connection_index));
            }
        }
    }

    fn connect_endpoint(&mut self, endpoint_index: usize, connection_index: Option<usize>) {
        let Some((current_connection_index, endpoint)) = self.endpoints.get_mut(endpoint_index)
        else {
            // Unknown endpoint, this should never happen.
            event!(
                Level::ERROR,
                ?endpoint_index,
                "Connection to unknown endpoint requested"
            );
            return;
        };
        let is_backoff = connection_index.is_some();
        let connection_index = connection_index.unwrap_or_else(|| {
            *current_connection_index += 1;
            *current_connection_index
        });
        if is_backoff {
            event!(
                Level::WARN,
                ?connection_index,
                endpoint = ?endpoint.uri(),
                "Connection failed, reconnecting"
            );
        } else {
            event!(
                Level::INFO,
                ?connection_index,
                endpoint = ?endpoint.uri(),
                "Creating new connection"
            );
        }
        let identifier = ChannelIdentifier {
            endpoint_index,
            connection_index,
        };
        let connection_stream = unfold(endpoint.clone(), move |endpoint| async move {
            let result = endpoint.connect().await.map_err(|err| {
                make_err!(
                    Code::Unavailable,
                    "Failed to connect to {:?}: {err:?}",
                    endpoint.uri()
                )
            });
            Some((
                result.map_or_else(RetryResult::Retry, RetryResult::Ok),
                endpoint,
            ))
        });
        let retrier = self.retrier.clone();
        self.connecting_channels.push(Box::pin(async move {
            if is_backoff {
                // Just in case the retry config is 0, then we need to
                // introduce some delay so we aren't in a hard loop.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            retrier.retry(connection_stream).await.map_or_else(
                |err| Err((identifier, err)),
                |channel| {
                    Ok(EstablishedChannel {
                        channel,
                        identifier,
                    })
                },
            )
        }));
    }

    // This must never be made async otherwise the select may cancel it.
    fn handle_worker(&mut self, tx: oneshot::Sender<Connection>) {
        if let Some(channel) = (self.available_connections > 0)
            .then_some(())
            .and_then(|_| self.available_channels.pop_front())
        {
            self.provide_channel(channel, tx);
        } else {
            self.waiting_connections.push_back(tx);
        }
    }

    fn provide_channel(&mut self, channel: EstablishedChannel, tx: oneshot::Sender<Connection>) {
        // We decrement here because we create Connection, this will signal when
        // it is Dropped and therefore increment this again.
        self.available_connections -= 1;
        let _ = tx.send(Connection {
            connection_tx: self.connection_tx.clone(),
            pending_channel: Some(channel.channel.clone()),
            channel,
        });
    }

    fn maybe_available_connection(&mut self) {
        while self.available_connections > 0
            && !self.waiting_connections.is_empty()
            && !self.available_channels.is_empty()
        {
            if let Some(channel) = self.available_channels.pop_front() {
                if let Some(tx) = self.waiting_connections.pop_front() {
                    self.provide_channel(channel, tx);
                } else {
                    // This should never happen, but better than an unwrap.
                    self.available_channels.push_front(channel);
                }
            }
        }
    }

    // This must never be made async otherwise the select may cancel it.
    fn handle_connection(&mut self, request: ConnectionRequest) {
        match request {
            ConnectionRequest::Dropped(maybe_channel) => {
                if let Some(channel) = maybe_channel {
                    self.available_channels.push_back(channel);
                }
                self.available_connections += 1;
                self.maybe_available_connection();
            }
            ConnectionRequest::Connected(channel) => {
                self.available_channels.push_back(channel);
                self.maybe_available_connection();
            }
            // Handle a transport error on a connection by making it unavailable
            // for use and establishing a new connection to the endpoint.
            ConnectionRequest::Error((identifier, was_pending)) => {
                let should_reconnect = if was_pending {
                    true
                } else {
                    let original_length = self.available_channels.len();
                    self.available_channels
                        .retain(|channel| channel.identifier != identifier);
                    // Only reconnect if it wasn't already disconnected.
                    original_length != self.available_channels.len()
                };
                if should_reconnect {
                    self.connect_endpoint(identifier.endpoint_index, None);
                }
            }
        }
    }
}

/// An instance of this is obtained for every communication with the gGRPC
/// service.  This handles the permit for limiting concurrency, and also
/// re-connecting the underlying channel on error.  It depends on users
/// reporting all errors.
/// NOTE: This should never be cloneable because its lifetime is linked to the
///       ConnectionManagerWorker::available_connections.
pub struct Connection {
    /// Communication with ConnectionManagerWorker to inform about transport
    /// errors and when the Connection is dropped.
    connection_tx: mpsc::UnboundedSender<ConnectionRequest>,
    /// If set, the Channel that will be returned to the worker when connection
    /// completes (success or failure) or when the Connection is dropped if that
    /// happens before connection completes.
    pending_channel: Option<Channel>,
    /// The identifier to send to connection_tx.
    channel: EstablishedChannel,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let pending_channel = self
            .pending_channel
            .take()
            .map(|channel| EstablishedChannel {
                channel,
                identifier: self.channel.identifier,
            });
        let _ = self
            .connection_tx
            .send(ConnectionRequest::Dropped(pending_channel));
    }
}

/// A wrapper around the channel::ResponseFuture that forwards errors to the
/// connection_tx.
pub struct ResponseFuture {
    /// The wrapped future that actually does the work.
    inner: channel::ResponseFuture,
    /// Communication with ConnectionManagerWorker to inform about transport
    /// errors.
    connection_tx: mpsc::UnboundedSender<ConnectionRequest>,
    /// The identifier to send to connection_tx on a transport error.
    identifier: ChannelIdentifier,
}

/// This is mostly copied from tonic::transport::channel except it wraps it
/// to allow messaging about connection success and failure.
impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::BoxBody>> for Connection {
    type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
    type Error = tonic::transport::Error;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self.channel.channel.poll_ready(cx);
        if let Poll::Ready(result) = &result {
            match result {
                Ok(_) => {
                    if let Some(pending_channel) = self.pending_channel.take() {
                        let _ = self.connection_tx.send(ConnectionRequest::Connected(
                            EstablishedChannel {
                                channel: pending_channel,
                                identifier: self.channel.identifier,
                            },
                        ));
                    }
                }
                Err(err) => {
                    event!(
                        Level::DEBUG,
                        ?err,
                        "Error while creating connection on channel"
                    );
                    let _ = self.connection_tx.send(ConnectionRequest::Error((
                        self.channel.identifier,
                        self.pending_channel.take().is_some(),
                    )));
                }
            }
        }
        result
    }

    fn call(
        &mut self,
        request: tonic::codegen::http::Request<tonic::body::BoxBody>,
    ) -> Self::Future {
        ResponseFuture {
            inner: self.channel.channel.call(request),
            connection_tx: self.connection_tx.clone(),
            identifier: self.channel.identifier,
        }
    }
}

/// This is mostly copied from tonic::transport::channel except it wraps it
/// to allow messaging about connection failure.
impl Future for ResponseFuture {
    type Output =
        Result<tonic::codegen::http::Response<tonic::body::BoxBody>, tonic::transport::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = Pin::new(&mut self.inner).poll(cx);
        if let Poll::Ready(Err(_)) = &result {
            let _ = self
                .connection_tx
                .send(ConnectionRequest::Error((self.identifier, false)));
        }
        result
    }
}
