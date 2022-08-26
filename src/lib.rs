#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]

pub mod connector;

use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use connector::circuit_breaker::{DynCondition, Handler, InspectedFuture};
pub use connector::*;
use futures::{Future, TryStreamExt};
use http::{uri::Authority, Request, Response, Uri};
use hyper::Body;
use metainfo::TypeMap;
use motore::{
    builder::ServiceBuilder,
    layer::{layer_fn, Identity, LayerFn, Stack},
    BoxCloneService, BoxError, Service,
};
use newtype::NewType;
use volo::{net::Address, util::Ref, Layer};
use volo_grpc::context::ServerContext;

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

#[derive(Clone)]
pub struct ProxyService {
    sender: GrpcSendRequest,
}

impl ProxyService {
    pub fn new(sender: GrpcSendRequest) -> Self {
        Self { sender }
    }
}

impl Service<ServerContext, (Request<Body>, Arc<ConnExtra>)> for ProxyService {
    type Response = Response<Body>;

    type Error = BoxError;

    type Future<'cx> = impl Future<Output = Result<Self::Response, Self::Error>> + Send + 'cx
    where
        Self: 'cx;

    fn call<'cx, 's>(
        &'s mut self,
        _cx: &'cx mut ServerContext,
        (req, _): (Request<Body>, Arc<ConnExtra>),
    ) -> Self::Future<'cx>
    where
        's: 'cx,
    {
        let tx = self.sender.clone();
        async move {
            tx.send_request(req)
                .await
                .map_err(|err| Box::new(err).into())
        }
    }
}

struct Connection<C> {
    extra: TypeMap,
    conn: C,
}

async fn do_connect<C: CircuitBreaker>(mut c: C) -> Result<Connection<C::Conn>, BoxError> {
    let (conn, extra) = c.connection_with_extra().await?;
    Ok(Connection { extra, conn })
}

#[derive(Clone)]
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

impl<C> Service<ServerContext, (Request<Body>, Arc<ConnExtra>)> for CircuitBreakableProxyService<C>
where
    C: CircuitBreaker + GrpcConnector<Conn = GrpcSendRequest> + Clone,
{
    type Response = Response<Body>;

    type Error = volo_grpc::Status;

    type Future<'cx> = impl Future<Output = Result<Self::Response, Self::Error>> + Send + 'cx
    where
        Self: 'cx;

    fn call<'cx, 's>(
        &'s mut self,
        _cx: &'cx mut ServerContext,
        (req, _): (Request<Body>, Arc<ConnExtra>),
    ) -> Self::Future<'cx>
    where
        's: 'cx,
    {
        let connector = self.connector.clone();
        let condition = self.condition.clone();
        async move {
            // TODO: rename do_connect to something like `get_conn` since it doesn't really connect
            // every time, instead it may reuse existing connections.
            let cli_conn = match do_connect(connector).await {
                Err(err) => {
                    tracing::warn!(error=?err, "[VOLO] fail to connect to upstream");
                    return Err(volo_grpc::Status::from_error(err));
                }
                Ok(ret) => ret,
            };
            let extra = cli_conn.extra;
            let tx = cli_conn.conn;

            let req: HyperRequest = req.into();
            let f = async move {
                let resp: Result<http::Response<hyper::Body>, volo_grpc::Status> = tx
                    .send_request(req)
                    .await
                    .map_err(|err| volo_grpc::Status::from_error(Box::new(err)));
                let resp = resp?;
                Ok(resp)
            };
            if let Some(handler) = extra.get::<Handler<C::Breakee>>() {
                condition(handler.clone(), Box::pin(f))
                    .await
                    .map_err(|err| volo_grpc::Status::from_error(err))
            } else {
                f.await.map_err(|err| volo_grpc::Status::from_error(err))
            }
        }
    }
}

pub struct RedirectServer<C, L = Identity> {
    connector: C,
    layer: L,
}

impl<C> RedirectServer<C> {
    pub fn new(connector: C) -> Self {
        Self {
            connector,
            layer: Identity::new(),
        }
    }
}

impl<C, L> RedirectServer<C, L> {
    pub fn layer<NL>(self, layer: NL) -> RedirectServer<C, Stack<L, NL>> {
        RedirectServer {
            connector: self.connector,
            layer: Stack::new(self.layer, layer),
        }
    }

    pub fn layer_fn<F, Fut>(self, f: F) -> RedirectServer<C, Stack<L, LayerFn<F>>>
    where
        F: Fn(
                (Request<Body>, Arc<ConnExtra>),
                BoxCloneService<
                    ServerContext,
                    (Request<Body>, Arc<ConnExtra>),
                    Response<Body>,
                    BoxError,
                >,
            ) -> Fut
            + Clone,
        Fut: Future<Output = Result<Response<Body>, BoxError>>,
    {
        self.layer(layer_fn(f))
    }

    pub async fn run_with_circuit_breaker<Cond>(
        self,
        addr: impl volo::net::MakeIncoming,
        cond: Cond,
    ) -> Result<(), BoxError>
    where
        L: Layer<CircuitBreakableProxyService<C>> + Sync + Send + Clone + 'static,
        L::Service: Service<
                ServerContext,
                (Request<Body>, Arc<ConnExtra>),
                Response = Response<Body>,
                Error = volo_grpc::Status,
            > + Send
            + Clone
            + 'static,

        // connector constraint
        C: CircuitBreaker + Clone,
        C: GrpcConnector<Conn = GrpcSendRequest>,
        C::Breakee: Clone,

        // circuit breaker condition constraint
        Cond: Fn(Handler<C::Breakee>, InspectedFuture) -> InspectedFuture + Sync + Send + 'static,
    {
        let mut incoming = addr.make_incoming().await?;
        let cond = Arc::new(cond);
        while let Some(conn) = incoming.try_next().await? {
            let layer = self.layer.clone();
            let connector = self.connector.clone();
            let cond = cond.clone();
            tokio::spawn(async move {
                // let conn: Conn = conn;
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
                    .layer(HyperAdaptorLayer::new(conn_extra))
                    .layer(layer)
                    .service(CircuitBreakableProxyService::new(connector, cond));

                let mut server = hyper::server::conn::Http::new();
                if let Err(err) = server
                    .http2_max_send_buf_size(4 * 1024 * 1024)
                    .http2_max_concurrent_streams(1000)
                    .http2_adaptive_window(true)
                    .serve_connection(conn, proxy_service)
                    .await
                {
                    tracing::debug!(error=?err, peer=?peer_addr, "[VOLO] fail to serve connection");
                }
            });
        }
        Ok(())
    }

    // TODO: fix sd update bug
    pub async fn run<Cond>(self, addr: impl volo::net::MakeIncoming) -> Result<(), BoxError>
    where
        L: Layer<ProxyService> + Sync + Send + Clone + 'static,
        L::Service: Service<
                ServerContext,
                (Request<Body>, Arc<ConnExtra>),
                Response = Response<Body>,
                Error = volo_grpc::Status,
            > + Send
            + Clone
            + 'static,

        // connector constraint
        C: CircuitBreaker + Clone,
        C: GrpcConnector<Conn = GrpcSendRequest>,
        C::Breakee: Clone,

        // circuit breaker condition constraint
        Cond: Fn(Handler<C::Breakee>, InspectedFuture) -> InspectedFuture + Sync + Send + 'static,
    {
        let mut incoming = addr.make_incoming().await?;
        while let Some(conn) = incoming.try_next().await? {
            let layer = self.layer.clone();
            let mut connector = self.connector.clone();
            let (send_request, mut extra) = connector.connection_with_extra().await?;
            tokio::spawn(async move {
                let peer_addr = conn.info.peer_addr.clone();
                let peer_addr = match peer_addr {
                    Some(Address::Ip(addr)) => Some(PeerAddr(addr)),
                    _ => None,
                };

                if let Some(peer_addr) = &peer_addr {
                    extra.insert(peer_addr.clone());
                }
                let conn_extra = Arc::new(extra);

                let proxy_service = ServiceBuilder::new()
                    .layer(HyperAdaptorLayer::new(conn_extra))
                    .layer(layer)
                    .service(ProxyService::new(send_request));

                let mut server = hyper::server::conn::Http::new();
                if let Err(err) = server
                    .http2_max_send_buf_size(4 * 1024 * 1024)
                    .http2_max_concurrent_streams(1000)
                    .http2_adaptive_window(true)
                    .serve_connection(conn, proxy_service)
                    .await
                {
                    tracing::debug!(error=?err, peer=?peer_addr, "[volo] fail to serve connection");
                }
            });
        }
        Ok(())
    }
}

pub struct HyperAdaptorLayer {
    extra: Arc<ConnExtra>,
}

impl HyperAdaptorLayer {
    pub fn new(extra: Arc<ConnExtra>) -> Self {
        Self { extra }
    }
}

impl<S> Layer<S> for HyperAdaptorLayer {
    type Service = HyperAdaptorService<S>;

    fn layer(self, inner: S) -> Self::Service {
        HyperAdaptorService {
            inner,
            extra: self.extra.clone(),
        }
    }
}

#[derive(Clone)]
pub struct HyperAdaptorService<S> {
    inner: S,
    extra: Arc<ConnExtra>,
}

impl<S> tower::Service<hyper::Request<hyper::Body>> for HyperAdaptorService<S>
where
    S: Service<
            ServerContext,
            (Request<Body>, Arc<ConnExtra>),
            Response = Response<Body>,
            Error = volo_grpc::Status,
        > + Clone
        + Send
        + 'static,
{
    type Response = hyper::Response<hyper::Body>;
    type Error = Infallible;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut ::core::task::Context<'_>,
    ) -> ::core::task::Poll<Result<(), Self::Error>> {
        ::core::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: HyperRequest) -> Self::Future {
        let extra = self.extra.clone();
        let mut inner = self.inner.clone();

        async move {
            let mut cx = ServerContext::default();
            match inner.call(&mut cx, (req, extra)).await {
                Ok(resp) => Ok(resp),
                Err(status) => {
                    let (parts, _) = status.to_http().into_parts();
                    Ok(hyper::Response::from_parts(parts, hyper::Body::empty()))
                }
            }
        }
    }
}
