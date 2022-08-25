#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]

pub mod connector;

use std::{
    cell::UnsafeCell, future::Future, marker::PhantomData, net::SocketAddr, sync::Arc,
    time::Duration,
};

use connector::circuit_breaker::{DynCondition, Handler, InspectedFuture};
pub use connector::*;
use futures::TryStreamExt;
use http::{uri::Authority, Request, Response, Uri};
use hyper::Body;
use metainfo::TypeMap;
use motore::{
    builder::ServiceBuilder,
    layer::{layer_fn, Identity, LayerFn, Stack},
    BoxCloneService, BoxError, Service,
};
use newtype::NewType;
use volo::{
    net::{conn::Conn, Address},
    util::Ref,
    Layer,
};
use volo_grpc::{
    body::Body as GrpcBody,
    codec::decode::Kind,
    context::{Config, ServerContext},
    request::Request as GrpcRequest,
    response::Response as GrpcResponse,
    status::Status as GrpcStatus,
    RecvEntryMessage, SendEntryMessage,
};

pub type BoxFuture<T, E> = futures::future::BoxFuture<'static, Result<T, E>>;
pub type ConnExtra = TypeMap;
pub type HyperRequest = hyper::Request<hyper::Body>;
pub type HyperResponse = hyper::Response<hyper::Body>;

#[derive(Clone)]
pub struct GrpcSendRequest {
    auth: Authority,
    // http2 only should be set when initializing the connection.
    pub client: hyper::Client<hyper::client::HttpConnector, hyper::Body>,
}

impl GrpcSendRequest {
    pub fn new(addr: SocketAddr) -> Self {
        let five_mins = Duration::from_secs(60 * 5);
        let mut http = hyper::client::connect::HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        http.set_keepalive(Some(five_mins));
        let client = hyper::Client::builder()
            .http2_keep_alive_interval(five_mins)
            .http2_only(true)
            .http2_keep_alive_while_idle(true)
            .pool_idle_timeout(Duration::from_secs(30))
            .build(http);
        GrpcSendRequest {
            auth: addr.to_string().parse().unwrap(),
            client,
        }
    }

    pub async fn send_request(
        &self,
        mut req: Request<hyper::Body>,
    ) -> hyper::Result<Response<hyper::Body>> {
        // we need to do a hack here to tell hyper to use the authority of our addr
        let mut parts = req.uri_mut().clone().into_parts();
        parts.authority = Some(self.auth.clone());
        let uri = Uri::from_parts(parts).unwrap();
        *req.uri_mut() = uri;
        self.client.request(req).await
    }
}

#[derive(NewType, Clone, Debug)]
pub struct PeerAddr(pub SocketAddr);

#[derive(NewType, Clone, Debug)]
pub struct ServiceName<'a>(pub Ref<'a, str>);

#[derive(NewType, Clone, Debug)]
pub struct RemoteAddr(pub SocketAddr);

pub struct RedirectServer<C, L = Identity, P = Identity> {
    connector: C,
    proxy: P,
    layer: L,
}

impl<C> RedirectServer<C> {
    pub fn new(connector: C) -> Self {
        Self {
            connector,
            proxy: Identity::new(),
            layer: Identity::new(),
        }
    }
}

impl<C, L, P> RedirectServer<C, L, P> {
    pub fn layer<NL>(self, layer: NL) -> RedirectServer<C, Stack<L, NL>, P> {
        RedirectServer {
            connector: self.connector,
            proxy: self.proxy,
            layer: Stack::new(self.layer, layer),
        }
    }

    pub fn proxy<NP>(self, proxy: NP) -> RedirectServer<C, L, Stack<P, NP>> {
        RedirectServer {
            connector: self.connector,
            proxy: Stack::new(self.proxy, proxy),
            layer: self.layer,
        }
    }

    // pub fn layer_fn<M, F, Fut>(self, f: F) -> RedirectServer<C, Stack<L, LayerFn<F>>, P>
    // where
    //     F: Fn(
    //             (GrpcRequest<M>, Arc<ConnExtra>),
    //             BoxCloneService<(GrpcRequest, Arc<ConnExtra>), GrpcResponse<GrpcBody>,
    // GrpcStatus>,         ) -> Fut
    //         + Clone,
    //     Fut: Future<Output = Result<GrpcResponse<GrpcBody>, GrpcStatus>>,
    // {
    //     self.layer(layer_fn(f))
    // }

    pub async fn run_with_circuit_breaker<T, U, A: volo::net::MakeIncoming, Cond>(
        self,
        incoming: A,
        cond: Cond,
    ) -> Result<(), BoxError>
    where
        // middleware layer constraints
        L: Layer<CircuitBreakableProxyService<C>> + Sync + Send + 'static,
        L::Service: Service<
                ServerContext,
                (GrpcRequest<Config>, Arc<ConnExtra>),
                Response = GrpcResponse<GrpcBody>,
                Error = GrpcStatus,
            > + Send
            + 'static,

        // connector constraint
        C: CircuitBreaker + Clone,

        // circuit breaker condition constraint
        Cond: Fn(Handler<C::Breakee>, InspectedFuture) -> InspectedFuture + Sync + Send + 'static,
        T: Send + 'static + RecvEntryMessage,
        U: Send + 'static + SendEntryMessage,
    {
        let mut incoming = incoming.make_incoming().await?;
        // let custom_layer = self.layer.clone();
        let cond = Arc::new(cond);
        while let Some(conn) = incoming.try_next().await? {
            // let custom_layer = custom_layer.clone();
            let connector = self.connector.clone();
            let cond = cond.clone();
            tokio::spawn(async move {
                let conn: Conn = conn;
                let peer_addr = conn.info.peer_addr.clone();
                let peer_addr = match peer_addr {
                    Some(Address::Ip(addr)) => Some(PeerAddr(addr)),
                    _ => None,
                };

                let mut extra = ConnExtra::default();

                if let Some(peer_addr) = &peer_addr {
                    extra.insert(peer_addr.clone());
                }
                let conn_extra = Arc::new(extra);

                let proxy_service = ServiceBuilder::new()
                    .layer(RedirectServerHyperAdaptorLayer(conn_extra))
                    // .layer(custom_layer)
                    .service(CircuitBreakableProxyService::new(connector, cond));

                let mut server = hyper::server::conn::Http::new();
                if let Err(err) = server
                    .http2_max_send_buf_size(4 * 1024 * 1024)
                    .http2_max_concurrent_streams(1000)
                    .http2_adaptive_window(true)
                    .serve_connection(conn, proxy_service)
                    .await
                {
                    tracing::debug!(error=?err, peer=?peer_addr, "[LUST] fail to serve connection");
                }
            });
        }
        Ok(())
    }

    pub async fn run<A: volo::net::MakeIncoming>(self, incoming: A) -> Result<(), BoxError>
    where
        // middleware layer constraints
        L: Layer<ProxyService> + Sync + Send + 'static,
        L::Service: Service<
                ServerContext,
                (GrpcRequest<Config>, Arc<ConnExtra>),
                Response = GrpcResponse<GrpcBody>,
                Error = GrpcStatus,
            > + Send
            + 'static,

        // connector constraints
        C: GrpcConnector<Conn = GrpcSendRequest> + Clone,
    {
        let mut incoming = incoming.make_incoming().await?;
        let mut connector = self.connector.clone();
        let custom_layer = Arc::new(self.layer);
        while let Some(conn) = incoming.try_next().await? {
            let (send_request, mut extra) = connector.connection_with_extra().await?;
            let custom_layer = custom_layer.clone();
            tokio::spawn(async move {
                let conn: Conn = conn;
                let peer_addr = conn.info.peer_addr.clone();
                let peer_addr = match peer_addr {
                    Some(Address::Ip(addr)) => Some(PeerAddr(addr)),
                    _ => None,
                };

                if let Some(peer_addr) = &peer_addr {
                    extra.insert(peer_addr.clone());
                }
                let extra = Arc::new(extra);
                let proxy_service = ServiceBuilder::new()
                    .layer(&*custom_layer)
                    .service(ProxyService::new(send_request));

                let mut server = hyper::server::conn::Http::new();
                if let Err(err) = server
                    .http2_max_send_buf_size(4 * 1024 * 1024)
                    .http2_max_concurrent_streams(1000)
                    .http2_adaptive_window(true)
                    .serve_connection(conn, proxy_service)
                    .await
                {
                    tracing::debug!(error=?err, peer=?peer_addr, "[LUST] fail to serve connection");
                }
            });
        }
        Ok(())
    }
}

struct Connection<C> {
    remote_addr: Option<SocketAddr>,
    target: Option<Ref<'static, str>>,
    extra: TypeMap,
    conn: C,
}

async fn do_connect<C: CircuitBreaker>(mut c: C) -> Result<Connection<C::Conn>, BoxError> {
    let (conn, extra) = c.connection_with_extra().await?;
    let rip = extra.get::<SocketAddr>().cloned();
    let target = extra.get::<ServiceName>().cloned().map(|t| t.0);
    Ok(Connection {
        remote_addr: rip,
        target,
        extra,
        conn,
    })
}

struct RedirectServerHyperAdaptorLayer<T, U>(Arc<ConnExtra>, PhantomData<(T, U)>);

impl<T, S, U> Layer<S> for RedirectServerHyperAdaptorLayer<T, U> {
    type Service = RedirectServerHyperAdaptorService<T, S, U>;

    fn layer(self, inner: S) -> Self::Service {
        RedirectServerHyperAdaptorService {
            inner,
            extra: self.0.clone(),
            _marker: self.1,
        }
    }
}

#[derive(Clone)]
struct RedirectServerHyperAdaptorService<T, S, U> {
    inner: S,
    extra: Arc<ConnExtra>,
    _marker: PhantomData<(T, U)>,
}

macro_rules! trans {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(status) => return Ok(status.to_http()),
        }
    };
}

impl<T, S, U> tower::Service<HyperRequest> for RedirectServerHyperAdaptorService<T, S, U>
where
    S: Service<
            ServerContext,
            (GrpcRequest<T>, Arc<ConnExtra>),
            Response = volo_grpc::response::Response<U>,
            Error = GrpcStatus,
        > + Clone
        + Send
        + 'static,
    T: RecvEntryMessage + Send + Sync,
    U: SendEntryMessage + Send + Sync,
{
    type Response = hyper::Response<GrpcBody>;
    type Error = anyhow::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        core::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: HyperRequest) -> Self::Future {
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let mut cx = ServerContext::default();

            cx.rpc_info.method = Some(req.uri().path().into());

            let (parts, body) = req.into_parts();
            let body = trans!(T::from_body(
                cx.rpc_info.method.as_deref(),
                body,
                Kind::Request
            ));
            let volo_req = volo_grpc::request::Request::from_http_parts(parts, body);

            let volo_resp = trans!(inner.call(&mut cx, (volo_req, self.extra.clone())).await);

            let (mut parts, body) = volo_resp.into_http().into_parts();
            parts.headers.insert(
                http::header::CONTENT_TYPE,
                http::header::HeaderValue::from_static("application/grpc"),
            );
            let bytes_stream = body.into_body();
            Ok(hyper::Response::from_parts(
                parts,
                volo_grpc::body::Body::new(bytes_stream),
            ))
        })
    }
}

#[derive(Clone)]
pub struct ProxyService {
    sender: GrpcSendRequest,
}

impl ProxyService {
    pub fn new(sender: GrpcSendRequest) -> Self {
        Self { sender }
    }
}

impl Service<ServerContext, (GrpcRequest<Config>, Arc<ConnExtra>)> for ProxyService {
    type Response = GrpcResponse<GrpcBody>;

    type Error = GrpcStatus;

    type Future<'cx> = BoxFuture<Self::Response, Self::Error>;

    fn call<'cx, 's>(
        &'s mut self,
        cx: &'cx mut ServerContext,
        req: (GrpcRequest<Config>, Arc<ConnExtra>),
    ) -> Self::Future<'cx>
    where
        's: 'cx,
    {
        let tx = self.sender.clone();
        Box::pin(async move {
            let req: HyperRequest = req.into();
            let resp = tx.send_request(req).await;
            let resp = resp.map_err(|err| GrpcStatus::from_error(Box::new(err)))?;
            let resp: GrpcResponse<GrpcBody> = resp.try_into()?;
            Ok(resp)
        })
    }
}

pub struct CircuitBreakableProxyService<C: CircuitBreaker> {
    connector: C,
    condition: DynCondition<C::Breakee>,
}

impl<C: CircuitBreaker> CircuitBreakableProxyService<C> {
    pub fn new(connector: C, condition: DynCondition<C::Breakee>) -> Self {
        Self {
            connector,
            condition,
        }
    }
}

impl<C> tower::Service<(GrpcRequest<Config>, Arc<ConnExtra>)> for CircuitBreakableProxyService<C>
where
    C: CircuitBreaker + GrpcConnector<Conn = GrpcSendRequest> + Clone,
{
    type Response = GrpcResponse<GrpcBody>;

    type Error = GrpcStatus;

    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, (req, _): (GrpcRequest<Config>, Arc<ConnExtra>)) -> Self::Future {
        let connector = self.connector.clone();
        let condition = self.condition.clone();
        Box::pin(async move {
            // TODO: rename do_connect to something like `get_conn` since it doesn't really connect
            // every time, instead it may reuse existing connections.
            let cli_conn = match do_connect(connector).await {
                Err(err) => {
                    tracing::warn!(error=?err, "[LUST] fail to connect to upstream");
                    return Err(GrpcStatus::from_error(err));
                }
                Ok(ret) => ret,
            };
            let extra = cli_conn.extra;
            let tx = cli_conn.conn;

            let req: HyperRequest = req.into();
            let f = async move {
                let resp = tx
                    .send_request(req)
                    .await
                    .map_err(|err| GrpcStatus::from_error(Box::new(err)))
                    .and_then(GrpcResponse::<GrpcBody>::try_from);
                let resp = resp?;
                Ok(resp)
            };
            if let Some(handler) = extra.get::<Handler<C::Breakee>>() {
                condition(handler.clone(), Box::pin(f)).await
            } else {
                f.await
            }
        })
    }
}
